INSERT INTO bronze.sales_orders
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