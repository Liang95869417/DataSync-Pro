-- models/intermediate/fct_product.sql

-- Create the fact table by extracting and transforming relevant columns from the source data
WITH fct_product_cte AS (
    SELECT
        product_id,
        ean,
        product_name,
        product_weight,
        weight_unit,
        kassal_image_url,
        vda_image_url,
        GENERATE_UUID() AS category_id,
        GENERATE_UUID() AS vendor_id,
        created_at,
        updated_at,
        kassal_allergens,
        vda_allergens,
        ingredients,
        kassal_nutrition,
        vda_nutrition,
        brand,
        production_country,
        min_temp,
        max_temp,
        GENERATE_UUID() AS price_id,
        GENERATE_UUID() AS store_id
    FROM {{ ref('merged_product_data') }}
)
SELECT
    product_id,
    ean,
    product_name,
    product_weight,
    weight_unit,
    kassal_image_url,
    vda_image_url,
    category_id,
    vendor_id,
    created_at,
    updated_at,
    kassal_allergens,
    vda_allergens,
    ingredients,
    kassal_nutrition,
    vda_nutrition,
    brand,
    production_country,
    min_temp,
    max_temp,
    price_id,
    store_id
FROM fct_product_cte
