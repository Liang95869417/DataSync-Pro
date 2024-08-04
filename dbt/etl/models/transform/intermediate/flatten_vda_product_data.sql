-- models/intermediate/flatten_vda_product_data.sql

WITH vda_flattened AS (
    SELECT
        gtin AS ean,
        produktnavn AS product_name,
        ingredienser AS ingredients,
        firmaNavn AS vendor_name,
        bildeUrl AS image_url,
        sistEndret AS updated_at,
        (
            SELECT STRING_AGG(allergen.allergen, ', ')
            FROM UNNEST(allergener) AS allergen
            WHERE allergen.verdi = 'Inneholder' OR allergen.verdi = 'Kan inneholde'
        ) AS allergens,
        merkeordninger AS labels,
        varegruppenavn AS category,
        ARRAY(
            SELECT AS STRUCT
                vda_nutrition.deklarasjon AS name,
                CAST(vda_nutrition.verdi AS DECIMAL) AS amount,
                CAST(NULL AS STRING) AS unit  -- VDA source does not provide unit
            FROM UNNEST(deklarasjoner) AS vda_nutrition
        ) AS nutrition,
        gln,
        produksjonsland AS production_country,
        minimumsTemperaturCelcius AS min_temp,
        maksimumsTemperaturCelcius AS max_temp,
        merkeordninger AS product_desc,
        mengde AS product_weight,
        mengdeType AS weight_unit,
        ROW_NUMBER() OVER (PARTITION BY gtin ORDER BY sistEndret DESC) AS rn
    FROM
        `{{ var('dataset') }}.{{ var('vda_table_id') }}`
)
SELECT * FROM vda_flattened
WHERE rn = 1