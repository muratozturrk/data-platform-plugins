# Pseudo-Anonymizer

Category: Data Protection · Layer: data sharing / export

## Problem
The anonymized dataset shared with the external analytics company lacks the removed columns,
the simplest anonymization choice. The team can't answer most business queries and asks for a
dataset that still hides real PII but replaces it with a **more usable form**.

## Solution / Mechanics
Replace initial values with substitutes more or less related to the original. Four
implementations:

1. **Data masking**: meaningless characters or realistic substitutes. SSN `999-55-1040` ->
   `XXX-XX-1040` or `9XX-5X-1XXX`. Several users can share one masked value.
2. **Data tokenization**: fictive values plus a mapping stored in a **token vault**. Securing the
   vault is the key: compromise it and every token is reversible.
3. **Hashing**: irreversible replacement, e.g. SHA-256 + Base64 of an email.
4. **Encryption**: column- or row-level keys; holders of the key restore the original.

Plus **generalization**: country -> geographic area, salary -> range.
Simple methods are column functions; tokenization needs a mapping table and more work.

```
 user_id | country | ssn            | salary | email
 1       | Poland  | 0940-0000-1000 | 50000  | a@x.com
                  |                 |              |
       generalize |     mask        |   range      |  keyed hash / token
                  v                 v              v
 1       | eu      | 0***-0***-1*** | 50000-60000 | 3f9a...(HMAC)  or  tok_8c1e (vault)
 Joins on user/email still work; values aren't raw, but the row is still personal data.
```

Anonymization vs pseudo-anonymization: pseudo-anonymized data can become
identifiable when combined with other datasets; anonymized data can't.

## Consequences & trade-offs
- **False sense of security**: joining tables re-identifies people. For
  example: a masked country `S*n M****o` + masked role `C******h P*******r i******r` + food
  preferences point to one famous person. Full anonymization (`Europe`, `Software engineer`)
  would not.
- **Information loss**: masks collide (`999-55-1040` and `999-13-1040` both become
  `XXX-XX-1040`). Generalization also changes the data **type** (integer salary -> string range).
- Tokenization moves the risk to the vault; hashing of low-entropy values is brute-forceable
  without a secret key.
- Legally, pseudonymized data is still personal data (GDPR): removal requests still apply.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Partner needs distinct counts, joins, bands | Yes | - |
| Authorized process must reverse values | Tokenization or encryption | - |
| Third party must never identify anyone, even with joins | No | Anonymizer |
| Internal readers, query-time hiding is enough | Maybe | Fine-Grained Accessor for Tables (masks) |
| Low-entropy IDs (phone, SSN) to hash | Only keyed (HMAC) | Tokenization |

## Key design decisions
- Method per column: mask (display), token (reversible, vault), HMAC (joinable, irreversible),
  encryption (reversible with key), generalization (analytics bands).
- Key/vault custody: secrets manager or KMS; who may detokenize; audit of vault reads.
- Key rotation for HMAC breaks joinability with older data: plan it as re-pseudonymization.
- Consistency across datasets: same key/vault for datasets that must join, different keys for
  datasets that must not.
- Generalization buckets: non-overlapping ranges, an explicit "other" bucket for unknown values.

## Gotchas & best practices
- Plain `sha2(email)` is reversible with a dictionary attack; use HMAC with a secret key.
- Don't build the keyed hash as `sha2(concat(lit(secret), col))` in Spark: the secret appears as a
  literal in the query plan and Spark UI. Use a UDF closing over the key.
- A naive salary CASE uses overlapping `BETWEEN` bounds (50000 matches two branches); use
  half-open ranges.
- Unknown categories must map to a generic bucket, never fall through as the raw value.
- Tokens must be random, not derived from the value, or they are just hashes.
- Pseudonymize Kafka message keys too, otherwise the key leaks the identifier.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Partner re-identified users | Combined pseudo-anonymized tables | Anonymize quasi-identifiers, limit shared columns |
| Hashes reversed by partner | Unkeyed hash of emails/phones | HMAC with secret key |
| Secret visible in Spark UI SQL tab | Key passed as `lit()` | UDF closure or engine-native secret function |
| Joins broke across months | HMAC key rotated | Re-pseudonymize history or keep key versions |
| `mapInPandas` schema error on salary | Type change int -> string in mapping | Do the range in a column expression |
| Vault table readable by analysts | Vault not isolated | Separate schema/DB, `REVOKE ALL FROM PUBLIC` |

## Implementation notes per stack
**Spark** — `examples/pseudo_anonymizer/spark/pseudo_anonymizer_job.py`:
```python
def pseudo_anonymize_users(batches):
    for rows in batches:
        rows["country"] = rows["country"].apply(country_area)
        rows["ssn"] = rows["ssn"].apply(mask_ssn)
        yield rows

pseudo = (users.mapInPandas(pseudo_anonymize_users, users.schema)
          .withColumn("salary", F.expr("""CASE WHEN salary < 50000 THEN '0-50000'
                                               WHEN salary < 60000 THEN '50000-60000'
                                               ELSE '60000+' END"""))
          .withColumn("email", hmac_udf(hmac_key)("email")))
```
`--self-test` checks generalization, the SSN mask collision and HMAC stability.

**Kafka** — `examples/pseudo_anonymizer/kafka/pseudonymizing_relay.py`: HMAC-SHA256 of
`user_id`, `email` and the message key, phone masked to the last two digits; `test` self-check.

**Airflow** — `examples/pseudo_anonymizer/airflow/tokenization_vault_dag.py`: PostgreSQL vault
`vault.email_tokens` with `encode(gen_random_bytes(16), 'hex')` tokens, idempotent
`ON CONFLICT DO NOTHING` upserts, then a tokenized copy for sharing.

## Verification
- Same input + same key gives the same pseudonym across runs and datasets that must join.
- No raw value of a pseudonymized column appears in the output.
- Generalized columns contain only declared buckets (no fall-through raw values).
- Only the detokenization role can `SELECT` from the vault; analysts get permission denied.
- Try a re-identification join with the other shared datasets before release.

## Related patterns
- Anonymizer: stronger, irreversible, less useful.
- Encryptor: the key management behind the encryption method.
- Secrets Pointer: storage for HMAC keys and vault credentials.
- Fine-Grained Accessor for Tables: restricts who can read the vault or detokenize.
