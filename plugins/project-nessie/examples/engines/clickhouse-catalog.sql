CREATE DATABASE t_acme_lake
ENGINE = DataLakeCatalog('https://nessie.platform.svc/iceberg/t-acme/')
SETTINGS catalog_type = 'rest',
         warehouse = 'hkm-lake',
         auth_header = 'Bearer ${NESSIE_T_ACME_TOKEN}',
         storage_endpoint = 'https://objectscale.hkm.internal';
-- serving-layer load from the lakehouse
INSERT INTO serving.orders_daily SELECT order_date, count() FROM t_acme_lake.`gold_sales.fct_orders` GROUP BY order_date;
