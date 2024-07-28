-- models/intermediate/flatten_kassal_product_data.sql

WITH kassal_flattened AS (
    SELECT
        id as product_id,
        ean,
        name as product_name,
        ingredients,
        vendor AS vendor_name,
        image AS image_url,
        created_at,
        updated_at,
        ARRAY(
            SELECT allergen
            FROM UNNEST(allergens) AS allergen
            WHERE allergen.contains = "YES" OR allergen.contains = "CAN_CONTAIN_TRACES"
        ) AS allergens,
        labels,
        category,
        price_history,
        current_price,
        current_unit_price,
        nutrition,
        brand,
        store,
        weight AS product_weight,
        weight_unit,
        description AS product_desc
    FROM
        datasync-pro.raw_dataset.kassal_product_data
)
SELECT * FROM kassal_flattened
