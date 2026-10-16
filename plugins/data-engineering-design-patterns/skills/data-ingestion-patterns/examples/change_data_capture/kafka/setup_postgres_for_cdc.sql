-- setup_postgres_for_cdc.sql
-- Run this on your PostgreSQL source database BEFORE deploying Debezium.
-- Debezium requires logical replication to be enabled.

-- 1. Enable logical replication (requires superuser, needs DB restart)
-- In postgresql.conf:
--   wal_level = logical
--   max_replication_slots = 4
--   max_wal_senders = 4

-- 2. Create a dedicated replication user for Debezium
CREATE USER debezium_user WITH PASSWORD :'debezium_pw'  -- psql -v debezium_pw="$DEBEZIUM_DB_PASSWORD" REPLICATION LOGIN;

-- 3. Grant read access to the tables you want to capture
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

-- 4. Create a replication slot manually (Debezium can do this too,
--    but doing it manually gives you control over the slot name)
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- 5. Create a publication for the specific tables to capture
CREATE PUBLICATION debezium_pub FOR TABLE public.orders;

-- Verify slot was created:
SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;