-- Lab assumption: a small "legacy warehouse" in the spirit of the Hamerliva Bank scenario.
-- Deterministic data (generate_series + modular arithmetic) so tests can assert exact counts.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE SCHEMA trading;
CREATE SCHEMA crm;
CREATE SCHEMA reporting;
CREATE SCHEMA staging;

-- Trade ledger: has a primary key, so upserts / CDC have a natural key.
CREATE TABLE trading.trades (
    trade_id     bigint PRIMARY KEY,
    trade_ts     timestamp      NOT NULL,
    symbol       text           NOT NULL,
    side         text           NOT NULL,
    qty          integer        NOT NULL,
    price        numeric(12, 4) NOT NULL,
    desk_region  text           NOT NULL
);
INSERT INTO trading.trades
SELECT g,
       timestamp '2024-01-01' + (g % 31) * interval '1 day' + (g % 86400) * interval '1 second',
       (ARRAY['AAPL', 'MSFT', 'SAP', 'ASML', 'NVDA'])[1 + g % 5],
       CASE WHEN g % 2 = 0 THEN 'BUY' ELSE 'SELL' END,
       1 + g % 500,
       100 + (g % 1000) / 10.0,
       CASE WHEN g % 3 = 0 THEN 'EU' ELSE 'US' END
FROM generate_series(1, 50000) AS g;

-- Client master data: personal data (PII) and no primary key (a typical legacy gap).
CREATE TABLE crm.clients (
    client_id   integer,
    full_name   text,
    email       text,
    iban        text,
    country     text,
    created_at  date
);
INSERT INTO crm.clients
SELECT g,
       'Client ' || g,
       'client' || g || '@example.com',
       'DE' || lpad((g * 7919)::text, 20, '0'),
       (ARRAY['DE', 'FR', 'NL', 'US', 'CA'])[1 + g % 5],
       date '2015-01-01' + g % 3000
FROM generate_series(1, 5000) AS g;

-- Aggregate used by dashboards.
CREATE TABLE reporting.daily_positions (
    as_of        date    NOT NULL,
    symbol       text    NOT NULL,
    desk_region  text    NOT NULL,
    net_qty      bigint  NOT NULL,
    PRIMARY KEY (as_of, symbol, desk_region)
);
INSERT INTO reporting.daily_positions
SELECT trade_ts::date, symbol, desk_region,
       sum(CASE WHEN side = 'BUY' THEN qty ELSE -qty END)
FROM trading.trades
GROUP BY 1, 2, 3;

-- Semantic logic that lives only in the warehouse (semantic drift across tools).
CREATE VIEW reporting.v_desk_exposure AS
SELECT desk_region, symbol, sum(net_qty) AS exposure
FROM reporting.daily_positions
GROUP BY desk_region, symbol;

-- Raw vendor payloads, semi-structured.
CREATE TABLE staging.vendor_feed_raw (
    feed_id      serial PRIMARY KEY,
    received_at  timestamp NOT NULL,
    payload      jsonb     NOT NULL
);
INSERT INTO staging.vendor_feed_raw (received_at, payload)
SELECT timestamp '2024-01-01' + g * interval '1 minute',
       jsonb_build_object('vendor', 'v' || (g % 4), 'symbol', (ARRAY['AAPL', 'SAP'])[1 + g % 2], 'px', 100 + g % 50)
FROM generate_series(1, 2000) AS g;

ANALYZE;
