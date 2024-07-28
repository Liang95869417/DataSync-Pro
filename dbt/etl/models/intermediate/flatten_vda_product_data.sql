WITH vda_flattened AS (
    SELECT
        gtin AS ean,
        produktnavn AS name,
        ingredienser AS ingredients,
        firmaNavn AS vendor,
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
    FROM
        datasync-pro.raw_dataset.vda_product_data_test
)
SELECT * FROM vda_flattened
