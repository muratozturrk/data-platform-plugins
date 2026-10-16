CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT          NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      VARCHAR(50)  NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO orders (customer_id, amount, status, updated_at) VALUES
    (1, 99.99,  'completed', NOW() - INTERVAL '2 hours'),
    (2, 149.50, 'pending',   NOW() - INTERVAL '1 hour'),
    (3, 75.00,  'completed', NOW() - INTERVAL '30 minutes');