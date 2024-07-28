WITH kassal_flattened AS (
    SELECT * FROM {{ ref('flatten_kassal_product_data') }}
),

vda_flattened AS (
    SELECT * FROM {{ ref('flatten_vda_product_data') }}
)
SELECT
    k.ean,
    COALESCE(k.name, v.name) AS name,
    COALESCE(k.ingredients, v.ingredients) AS ingredients,
    COALESCE(k.vendor, v.vendor) AS vendor,
    k.image_url AS kassal_image_url,
    v.image_url AS vda_image_url,
    k.created_at,
    k.updated_at,
    k.allergens AS kassal_allergens,
    v.allergens AS vda_allergens,
    k.category AS kassal_category,
    v.category AS vda_category,
    k.price_history,
    k.nutrition AS kassal_nutrition,
    v.nutrition AS vda_nutrition,
    k.brand,
    k.store,
    k.weight,
    k.weight_unit,
    v.gln,
    v.production_country,
    v.min_temp,
    v.max_temp
FROM
    kassal_flattened k
FULL OUTER JOIN
    vda_flattened v
ON
    k.ean = v.ean
