-- models/intermediate/merged_product_data.sql

WITH kassal_flattened AS (
    SELECT * FROM {{ ref('flatten_kassal_product_data') }}
),

vda_flattened AS (
    SELECT * FROM {{ ref('flatten_vda_product_data') }}
)
SELECT
    k.product_id,
    k.ean,
    COALESCE(k.product_name, v.product_name) AS product_name,
    COALESCE(k.ingredients, v.ingredients) AS ingredients,
    COALESCE(k.vendor_name, v.vendor_name) AS vendor_name,
    k.image_url AS kassal_image_url,
    v.image_url AS vda_image_url,
    k.created_at,
    k.updated_at,
    k.allergens AS kassal_allergens,
    v.allergens AS vda_allergens,
    k.category AS kassal_category,
    v.category AS vda_category,
    k.price_history,
    k.current_price,
    k.current_unit_price,
    k.nutrition AS kassal_nutrition,
    v.nutrition AS vda_nutrition,
    k.brand,
    k.store,
    k.product_weight,
    k.weight_unit,
    v.gln,
    v.production_country,
    v.min_temp,
    v.max_temp
FROM
    kassal_flattened k
LEFT JOIN
    vda_flattened v
ON
    k.ean = v.ean
