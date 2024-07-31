-- models/intermediate/flatten_kassal_product_data.sql


WITH kassal_flattened AS (
    SELECT
        id AS product_id,
        ean,
        name AS product_name,
        ingredients,
        vendor AS vendor_name,
        image AS image_url,
        created_at,
        updated_at,
        (
            SELECT STRING_AGG(allergen.display_name, ', ')
            FROM UNNEST(allergens) AS allergen
            WHERE (allergen.contains = 'YES' OR allergen.contains = 'CAN_CONTAIN_TRACES')
              AND allergen.display_name IS NOT NULL
        ) AS allergens,
        labels,
        category,
        price_history,
        current_price,
        current_unit_price,
        ARRAY(
            SELECT AS STRUCT
                nutrition.display_name AS name,
                CAST(nutrition.amount AS DECIMAL) AS amount,
                nutrition.unit AS unit
            FROM UNNEST(nutrition) AS nutrition
        ) AS nutrition,
        brand,
        store,
        CAST(weight AS FLOAT64) AS product_weight,
        weight_unit,
        description AS product_desc
    FROM
        `{{ var('dataset') }}.{{ var('kassal_table_id') }}`
)
SELECT * FROM kassal_flattened