INSERT INTO raw.sales_raw
(
    id,
    product,
    amount,
    created_at
)
SELECT
    id,
    product,
    amount,
    created_at
FROM public.sales_raw
WHERE id > %s
ON CONFLICT (id)
DO UPDATE
SET
    product = EXCLUDED.product,
    amount = EXCLUDED.amount,
    created_at = EXCLUDED.created_at;