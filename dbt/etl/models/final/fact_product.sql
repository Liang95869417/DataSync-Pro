-- fact_product.sql

WITH product_cte AS (
    SELECT
        GENERATE_UUID() AS product_id,
        ean,
        name,
        weight,
        weight_unit,
        kassal_image_url, 
        vda_image_url,
        created_at,
        updated_at,
        ingredients,
        COALESCE(vendor, 'Unknown vendor') AS vendor,
        vda_category AS category,
        kassal_nutrition,
        vda_nutrition,
        brand,
        min_temp,
        max_temp,
        production_country
    FROM datasync-pro.intermediate_dataset.merge_product_data
)
SELECT
    product_id,
    ean,
    name,
    weight,
    weight_unit,
    kassal_image_url, 
    vda_image_url,
    created_at,
    updated_at,
    ingredients,
    vendor,
    category_id,
    nutrition,
    brand,
    min_temp,
    max_temp,
    production_country
FROM product_cte
LEFT JOIN {{ ref('dim_category') }} ON product_cte.category= dim_category.name
LEFT JOIN {{ ref('dim_vendor') }} ON product_cte.vendor = dim_vendor.name
