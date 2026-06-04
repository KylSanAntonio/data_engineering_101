INSERT INTO warehouse.sales_summary
(
    product,
    total_sales,
    total_orders
)
SELECT
    product,
    SUM(amount_with_tax),
    COUNT(*)
FROM staging.sales_clean
GROUP BY product
ON CONFLICT (product)
DO UPDATE
SET
    total_sales = EXCLUDED.total_sales,
    total_orders = EXCLUDED.total_orders;