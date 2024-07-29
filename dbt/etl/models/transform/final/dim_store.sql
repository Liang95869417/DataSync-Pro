-- models/dim_store.sql

-- Create the dimension table for store by extracting unique stores from the source data
WITH dim_store_cte AS (
    SELECT DISTINCT
        GENERATE_UUID() AS store_id,
        store.code AS store_code,
        ANY_VALUE(store.name) AS store_name,
        ANY_VALUE(store.url) AS store_url,
        ANY_VALUE(store.logo) AS logo_url
    FROM {{ ref('merged_product_data') }}
    GROUP BY store.code
)
SELECT
    store_id,
    store_code,
    store_name,
    store_url,
    logo_url
FROM dim_store_cte
