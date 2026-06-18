WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    name,
    email,
    status,
    created_at,
    -- Simple flag to classify customers based on status
    CASE 
        WHEN status = 'active' THEN TRUE 
        ELSE FALSE 
    END AS is_active
FROM customers
