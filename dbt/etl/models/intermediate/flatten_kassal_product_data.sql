WITH kassal_flattened AS (
    SELECT
        ean,
        name,
        ingredients,
        vendor,
        image AS image_url,
        created_at,
        updated_at,
        ARRAY(
            SELECT allergen
            FROM UNNEST(allergens) AS allergen
            WHERE allergen.contains = "YES" or allergen.contains = "CAN_CONTAIN_TRACES"
        ) AS allergens,
        labels,
        category,
        price_history,
        nutrition,
        brand,
        store,
        weight,
        weight_unit
    FROM
        datasync-pro.raw_dataset.kassal_product_data
)
SELECT * FROM kassal_flattened
