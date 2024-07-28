-- dim_price.sql

WITH price_cte AS (
    SELECT
        GENERATE_UUID() AS price_id,
        ean,
        store,
        ph.price,
        ph.unit_price,
        ph.date
    FROM datasync-pro.intermediate_dataset.merge_product_data,
    UNNEST(price_history) AS ph
)
SELECT
    price_id,
    product_id,
    store_id,
    price,
    unit_price,
    date
FROM price_cte
LEFT JOIN {{ ref('fact_product') }} ON price_cte.ean = dim_product.ean
LEFT JOIN {{ ref('dim_store') }} ON price_cte.store = dim_store.name