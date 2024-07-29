-- models/dim_category.sql

-- Create the dimension table for category by extracting unique categories from the source data
WITH dim_category_cte AS (
    SELECT DISTINCT
        GENERATE_UUID() AS category_id,
        kassal_category,
        vda_category
    FROM {{ ref('merged_product_data') }}
    WHERE NOT (kassal_category IS NULL AND vda_category IS NULL)
)
SELECT
    category_id,
    kassal_category,
    vda_category
FROM dim_category_cte
