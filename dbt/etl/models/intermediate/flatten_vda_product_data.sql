-- models/intermediate/flatten_vda_product_data.sql

WITH vda_flattened AS (
    SELECT
        gtin AS ean,
        produktnavn AS product_name,
        ingredienser AS ingredients,
        firmaNavn AS vendor_name,
        bildeUrl AS image_url,
        sistEndret AS updated_at,
        ARRAY(
            SELECT allergen
            FROM UNNEST(allergener) AS allergen
            WHERE allergen.verdi = "Inneholder"
        ) AS allergens,
        merkeordninger AS labels,
        varegruppenavn AS category,
        deklarasjoner AS nutrition,
        gln,
        produksjonsland AS production_country,
        minimumsTemperaturCelcius AS min_temp,
        maksimumsTemperaturCelcius AS max_temp,
        merkeordninger AS product_desc
    FROM
        datasync-pro.raw_dataset.vda_product_data_test
)
SELECT * FROM vda_flattened
