CREATE CATALOG nessie WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
  'uri' = 'https://nessie.platform.svc/api/v2',
  'ref' = 't-acme',
  'warehouse' = 's3://hkm-lake/t-acme/',
  'authentication.type' = 'BEARER',
  'authentication.token' = '${NESSIE_T_ACME_TOKEN}',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'https://objectscale.hkm.internal',
  's3.path-style-access' = 'true'
);
USE CATALOG nessie;
-- streaming upsert into the institution branch; promotion to main by the WAP job
INSERT INTO silver_sales.orders_cdc SELECT * FROM kafka_orders_cdc;
