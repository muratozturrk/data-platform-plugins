-- Operational source system. Runs once on first container start.
-- 5 hand-written seed rows plus deterministic generated history (no random())
-- so tests can compute exact expected numbers.
CREATE TABLE fashion_sales (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(50),
    sales_amount DECIMAL(10, 2),
    sales_date DATE,
    store_location VARCHAR(100),
    customer_age_group VARCHAR(50),
    campaign_name VARCHAR(100)
);

INSERT INTO fashion_sales (product_name, category, sales_amount, sales_date, store_location, customer_age_group, campaign_name)
VALUES
    ('Slim Fit Jeans', 'Denim', 89.99, '2024-03-01', 'New York', '18-24', 'Spring Launch'),
    ('Leather Jacket', 'Outerwear', 249.99, '2024-03-01', 'Los Angeles', '25-34', 'Spring Launch'),
    ('Graphic T-Shirt', 'Tops', 39.99, '2024-03-02', 'Chicago', '18-24', 'March Madness'),
    ('Summer Dress', 'Dresses', 129.99, '2024-03-03', 'New York', '35-44', 'March Madness'),
    ('Casual Sneakers', 'Footwear', 99.99, '2024-03-03', 'Los Angeles', '25-34', 'Spring Launch');

-- 2,995 more sales, 2024-03-01 .. 2024-05-31 (92 days). Product and category stay consistent.
INSERT INTO fashion_sales (product_name, category, sales_amount, sales_date, store_location, customer_age_group, campaign_name)
SELECT
    (ARRAY['Slim Fit Jeans','Leather Jacket','Graphic T-Shirt','Summer Dress','Casual Sneakers',
           'Wool Coat','Linen Shirt','Chino Shorts','Ankle Boots','Denim Jacket'])[1 + (g * 7) % 10],
    (ARRAY['Denim','Outerwear','Tops','Dresses','Footwear',
           'Outerwear','Tops','Bottoms','Footwear','Denim'])[1 + (g * 7) % 10],
    (19.99 + ((g * 37) % 23000) / 100.0)::DECIMAL(10, 2),
    DATE '2024-03-01' + (g % 92),
    (ARRAY['New York','Los Angeles','Chicago','Miami','Seattle'])[1 + (g * 3) % 5],
    (ARRAY['18-24','25-34','35-44','45-54'])[1 + (g * 5) % 4],
    (ARRAY['Spring Launch','March Madness','Summer Kickoff'])[1 + (g * 11) % 3]
FROM generate_series(1, 2995) AS g;
