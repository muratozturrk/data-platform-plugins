-- Operational database (OLTP) that stays outside the lakehouse.
-- Customer keys match TPC-H tiny (1..1500), the order facts that live in Iceberg.

CREATE TABLE customers (
    customer_id  integer PRIMARY KEY,
    name         text          NOT NULL,
    email        text          NOT NULL,
    region       text          NOT NULL,
    segment      text          NOT NULL,
    credit_limit numeric(12,2) NOT NULL,
    loyalty_tier smallint      NOT NULL,
    status       text          NOT NULL DEFAULT 'active',
    updated_at   timestamp     NOT NULL DEFAULT now()
);

INSERT INTO customers (customer_id, name, email, region, segment, credit_limit, loyalty_tier, status, updated_at)
SELECT g,
       format('Customer#%s', lpad(g::text, 9, '0')),
       format('customer%s@example.com', g),
       (ARRAY['AFRICA','AMERICA','ASIA','EUROPE','MIDDLE EAST'])[1 + g % 5],
       (ARRAY['AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY'])[1 + (g * 7) % 5],
       1000 + (g * 37) % 9000,
       1 + g % 4,
       CASE WHEN g % 50 = 0 THEN 'suspended' ELSE 'active' END,
       timestamp '2025-01-01 00:00:00'
FROM generate_series(1, 1500) AS g;

CREATE INDEX customers_region_idx ON customers (region);

-- Marketing campaigns, maintained by the marketing application.
CREATE TABLE promotions (
    promo_id     serial PRIMARY KEY,
    campaign     text         NOT NULL,
    promo_day    date         NOT NULL,
    discount_pct numeric(5,2) NOT NULL
);

INSERT INTO promotions (campaign, promo_day, discount_pct) VALUES
    ('black_friday', DATE '1995-11-24', 25),
    ('black_friday', DATE '1996-11-29', 25),
    ('black_friday', DATE '1997-11-28', 30);
INSERT INTO promotions (campaign, promo_day, discount_pct)
SELECT 'spring_sale', d::date, 10 FROM generate_series(DATE '1994-03-01', DATE '1994-03-07', interval '1 day') AS d;

-- Reference data: currency conversion, owned by finance.
CREATE TABLE fx_rates (
    currency    text PRIMARY KEY,
    rate_to_usd numeric(10,4) NOT NULL
);
INSERT INTO fx_rates VALUES ('USD', 1.0000), ('EUR', 1.1000), ('GBP', 1.2500);

-- Returns, written by the order-management system.
CREATE TABLE order_returns (
    order_id    bigint PRIMARY KEY,
    returned_at timestamp NOT NULL
);
INSERT INTO order_returns SELECT g, timestamp '1998-12-01 00:00:00' FROM generate_series(97, 60000, 97) AS g;

ANALYZE;
