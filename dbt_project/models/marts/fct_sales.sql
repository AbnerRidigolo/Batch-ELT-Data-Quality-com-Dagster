WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
)

SELECT
    s.sale_id,
    s.customer_id,
    c.name AS customer_name,
    s.product,
    s.amount,
    s.status,
    s.sale_date,
    -- Add an indicator for high value orders (e.g. > $1000)
    CASE 
        WHEN s.amount > 1000.00 THEN TRUE 
        ELSE FALSE 
    END AS is_high_value
FROM sales s
LEFT JOIN customers c ON s.customer_id = c.customer_id
