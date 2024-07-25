WITH unified_data AS (
    SELECT
        k.ean AS gtin,
        k.name,
        k.description,
        k.weight,
        k.weight_unit,
        k.image AS image_url,
        k.ingredients,
        k.allergens,
        k.labels,
        k.created_at,
        k.updated_at,
        v.firmaNavn AS vendor_name,
        v.minimumsTemperaturCelcius AS min_temp,
        v.maksimumsTemperaturCelcius AS max_temp,
        v.produksjonsland AS production_country
    FROM
        {{ source('raw_dataset', 'kassal_product_data') }} k
    LEFT JOIN
        {{ source('raw_dataset', 'vda_product_data_test') }} v
    ON
        k.ean = v.gtin
)
SELECT * FROM unified_data

-- unified logic has problem