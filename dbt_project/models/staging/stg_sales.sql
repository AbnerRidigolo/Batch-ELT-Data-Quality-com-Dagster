WITH source AS (
    SELECT * FROM {{ source('raw', 'bronze_sales') }}
)

SELECT
    CAST(sale_id AS INT) AS sale_id,
    CAST(customer_id AS INT) AS customer_id,
    TRIM(product) AS product,
    CAST(amount AS DECIMAL(10, 2)) AS amount,
    UPPER(TRIM(status)) AS status,
    CAST(sale_date AS TIMESTAMP) AS sale_date
FROM source
