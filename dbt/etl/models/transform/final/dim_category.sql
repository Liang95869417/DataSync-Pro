-- Create the dimension table for category by extracting unique categories from the source data
WITH category_cte AS (
    SELECT 
        category_id,
        kassal_category,
        vda_category,
        ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY category_id) AS row_num
    FROM {{ ref('merged_product_data') }}
    WHERE NOT (kassal_category IS NULL AND vda_category IS NULL)
),
duplicate_category_ids AS (
    SELECT 
        category_id
    FROM category_cte
    GROUP BY category_id
    HAVING COUNT(*) > 1
)

SELECT 
    c.category_id,
    c.kassal_category,
    c.vda_category
FROM category_cte c
JOIN duplicate_category_ids d ON c.category_id = d.category_id
WHERE c.row_num = 1


