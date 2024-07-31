-- Create the dimension table for category by extracting unique categories from the source data
WITH category_cte AS (
    SELECT DISTINCT
        category_id,
        kassal_category,
        vda_category
    FROM {{ ref('merged_product_data') }}
    WHERE NOT (kassal_category IS NULL AND vda_category IS NULL)
)

SELECT 
    category_id,
    kassal_category,
    vda_category
FROM category_cte
