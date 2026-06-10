INSERT INTO silver.sales_orders
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
FROM bronze.sales_orders
WHERE id > %s
ON CONFLICT (id)
DO UPDATE
SET
    product = EXCLUDED.product,
    amount = EXCLUDED.amount,
    amount_with_tax = EXCLUDED.amount_with_tax;