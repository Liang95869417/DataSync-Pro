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
    REGEXP_REPLACE(UPPER(COALESCE(v.vendor_name, k.vendor_name)), r'\s*(A\/S|AS|AS\.|AB|SA|SA\.|AS \(STABBURET\)|STORHUSHOLDNING\.|HOLDING)\s*$', '') AS vendor_name,
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
    CASE
        WHEN k.product_weight IS NOT NULL AND k.weight_unit IS NOT NULL THEN
            STRUCT(k.product_weight AS product_weight, k.weight_unit AS weight_unit)
        ELSE
            STRUCT(v.product_weight AS product_weight, v.weight_unit AS weight_unit)
    END AS weight_struct,
    COALESCE(v.gln, NULL) AS gln,
    COALESCE(v.production_country, NULL) AS production_country,
    COALESCE(v.min_temp, NULL) AS min_temp,
    COALESCE(v.max_temp, NULL) AS max_temp,
    COALESCE(
        TO_HEX(MD5(CONCAT(
            TO_JSON_STRING(k.category),
            v.category
        ))),
        '00000000-0000-0000-0000-000000000000'
    ) AS category_id,
    COALESCE(
        TO_HEX(MD5(REGEXP_REPLACE(UPPER(COALESCE(v.vendor_name, k.vendor_name)), r'\s*(A\/S|AS|AS\.|AB|SA|SA\.|AS \(STABBURET\)|STORHUSHOLDNING\.|HOLDING)\s*$', ''))),
        '00000000-0000-0000-0000-000000000000'
    ) AS vendor_id,
    GENERATE_UUID() AS price_id
FROM
    kassal_flattened k
LEFT JOIN
    vda_flattened v
ON
    k.ean = v.ean
