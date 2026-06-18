WITH fct_sales AS (
    SELECT * FROM {{ ref('fct_sales') }}
)

SELECT
    DATE_TRUNC('month', sale_date) AS sales_month,
    COUNT(sale_id) AS total_transactions,
    SUM(amount) AS total_revenue,
    AVG(amount) AS average_order_value,
    SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END) AS high_value_transactions_count
FROM fct_sales
WHERE status != 'CANCELLED'
GROUP BY 1
ORDER BY 1
