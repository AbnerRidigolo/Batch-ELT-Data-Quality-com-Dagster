WITH source AS (
    SELECT * FROM {{ source('raw', 'bronze_customers') }}
)

SELECT
    CAST(id AS INT) AS customer_id,
    TRIM(name) AS name,
    LOWER(TRIM(email)) AS email,
    CAST(created_at AS TIMESTAMP) AS created_at,
    -- Handle nullable status field
    COALESCE(status, 'inactive') AS status
FROM source
