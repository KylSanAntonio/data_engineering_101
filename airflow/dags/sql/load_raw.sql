INSERT INTO raw.sales_raw
(
    id,
    product,
    amount,
    created_at
)
VALUES
(
    %s,
    %s,
    %s,
    %s
)
ON CONFLICT (id)
DO NOTHING;