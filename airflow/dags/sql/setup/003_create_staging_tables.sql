CREATE TABLE IF NOT EXISTS staging.sales_clean
(
    id BIGINT PRIMARY KEY,
    product TEXT,
    amount NUMERIC(18,2),
    amount_with_tax NUMERIC(18,2),
    created_at TIMESTAMP
);