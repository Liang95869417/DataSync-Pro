-- models/dim_store.sql

-- Create the dimension table for store by extracting unique stores from the source data
WITH dim_store_cte AS (
    SELECT DISTINCT
        store.code AS store_code,
        store.name AS store_name,
        store.url AS store_url,
        store.logo AS logo_url
    FROM {{ ref('merged_product_data') }}
)
SELECT
    store_code,
    store_name,
    store_url,
    logo_url
FROM dim_store_cte

