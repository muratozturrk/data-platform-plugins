"""Kafka authorization through the Ranger plugin (T-kafka-001): decision-table cases D11, D12, D16, D17.
Clients authenticate with SASL OAUTHBEARER over SASL_SSL (kafka-security › Principals r4); the harness passes one
Keycloak access token per principal in KAFKA_TOKEN_<PRINCIPAL> (dashes → underscores, upper case)."""
import os, pytest
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import TopicAuthorizationFailedError
from kafka.sasl.oauth import AbstractTokenProvider

BOOT = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")

class EnvToken(AbstractTokenProvider):
    def __init__(self, principal): self.var = "KAFKA_TOKEN_" + principal.replace("-", "_").upper()
    def token(self): return os.environ[self.var]

def auth(principal):
    return dict(security_protocol="SASL_SSL", sasl_mechanism="OAUTHBEARER", ssl_cafile="certs/ca.crt",
                sasl_oauth_token_provider=EnvToken(principal))

def publish(principal, topic):
    KafkaProducer(bootstrap_servers=BOOT, **auth(principal)).send(topic, b"{}").get(timeout=30)

@pytest.mark.decision
def test_pipeline_publishes_published_topic():                      # D11
    publish("wi-t-acme-orders-ingest", "t.acme.sales.pub.order-created.v1")

@pytest.mark.decision
def test_engineer_publishes_staging_topic():                        # D17
    publish("ali", "t.acme.sales.int.orders-raw.v1")

@pytest.mark.decision
def test_engineer_cannot_publish_published_topic():                 # D16
    with pytest.raises(TopicAuthorizationFailedError):
        publish("ali", "t.acme.sales.pub.order-created.v1")

@pytest.mark.decision
def test_engineer_cannot_consume_other_institution():               # D12
    c = KafkaConsumer("t.globex.sales.pub.order-created.v1", bootstrap_servers=BOOT, group_id="t.acme.probe",
                      consumer_timeout_ms=10000, **auth("ali"))
    with pytest.raises(TopicAuthorizationFailedError):
        next(iter(c))
