CREATE TABLE IF NOT EXISTS staging.sales_clean
(
    id BIGINT PRIMARY KEY,
    product TEXT,
    amount NUMERIC(18,2),
    amount_with_tax NUMERIC(18,2),
    created_at TIMESTAMP
);

INSERT INTO staging.sales_clean
(
    id,
    product,
    amount,
    amount_with_tax,
    created_at
)
SELECT
    id,
    LOWER(TRIM(product)),
    amount,
    amount * 1.12,
    created_at
FROM raw.sales_raw
WHERE id > %(watermark)s
ON CONFLICT (id)
DO UPDATE
SET
    product = EXCLUDED.product,
    amount = EXCLUDED.amount,
    amount_with_tax = EXCLUDED.amount_with_tax,
    created_at = EXCLUDED.created_at;