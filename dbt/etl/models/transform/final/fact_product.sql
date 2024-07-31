-- models/intermediate/fct_product.sql

-- Create the fact table by extracting and transforming relevant columns from the source data
WITH fct_product_cte AS (
    SELECT
        product_id,
        ean,
        product_name,
        weight_struct.product_weight AS product_weight,
        weight_struct.weight_unit AS weight_unit,
        kassal_image_url,
        vda_image_url,
        created_at,
        updated_at,
        allergens,
        ingredients,
        nutrition,
        brand,
        production_country,
        min_temp,
        max_temp,
        price_id,
        category_id,
        vendor_id,
        store.code AS store_code
    FROM {{ ref('merged_product_data') }}
)

SELECT
    fpc.product_id,
    fpc.ean,
    fpc.product_name,
    fpc.product_weight,
    fpc.weight_unit,
    fpc.kassal_image_url,
    fpc.vda_image_url,
    fpc.created_at,
    fpc.updated_at,
    fpc.allergens,
    fpc.ingredients,
    fpc.nutrition,
    fpc.brand,
    fpc.production_country,
    fpc.min_temp,
    fpc.max_temp,
    fpc.price_id,
    fpc.category_id,
    fpc.vendor_id,
    fpc.store_code
FROM fct_product_cte AS fpc
LEFT JOIN
    {{ ref('dim_category') }} AS dc
ON
    dc.category_id = fpc.category_id
LEFT JOIN
    {{ ref('dim_vendor') }} AS dv
ON
    dv.vendor_id = fpc.vendor_id
LEFT JOIN
    {{ ref('dim_price') }} AS dp
ON
    dp.price_id = fpc.price_id
LEFT JOIN
    {{ ref('dim_store') }} AS ds
ON
    ds.store_code = fpc.store_code