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
    COALESCE(k.allergens, v.allergens) AS allergens,
    CASE
        WHEN k.nutrition IS NOT NULL THEN k.nutrition
        ELSE v.nutrition
    END AS nutrition,
    k.category AS kassal_category,
    v.category AS vda_category,
    k.price_history,
    k.current_price,
    k.current_unit_price,
    k.brand,
    k.store,
    k.product_weight,
    k.weight_unit,
    v.gln,
    v.production_country,
    v.min_temp,
    v.max_temp,
    GENERATE_UUID() AS category_id,
    GENERATE_UUID() AS vendor_id,
    GENERATE_UUID() AS price_id
FROM
    kassal_flattened k
LEFT JOIN
    vda_flattened v
ON
    k.ean = v.ean
