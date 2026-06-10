SELECT
    id,
    product,
    amount,
    created_at
FROM sales
WHERE id > %s
ORDER BY id;