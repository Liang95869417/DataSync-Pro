-- models/dim_price.sql

-- Create the dimension table for price by extracting unique prices from the source data
WITH price_cte AS (
    SELECT
        price_id,
        current_price,
        current_unit_price,
        price_history
    FROM {{ ref('merged_product_data') }}
    WHERE price_history IS NOT NULL
)
SELECT
    price_id,
    current_price,
    current_unit_price,
    price_history
FROM price_cte
