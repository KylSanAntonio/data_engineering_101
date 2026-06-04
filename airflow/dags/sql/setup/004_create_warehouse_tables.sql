CREATE TABLE IF NOT EXISTS warehouse.sales_summary
(
    product TEXT PRIMARY KEY,
    total_sales NUMERIC(18,2),
    total_orders BIGINT
);