WITH cleaned_data AS (
    SELECT
        gtin,
        name,
        COALESCE(description, 'No description available') AS description,
        COALESCE(weight, 0) AS weight,
        COALESCE(weight_unit, 'Unknown') AS weight_unit,
        COALESCE(image_url, 'No image available') AS image_url,
        ingredients,
        allergens,
        labels,
        COALESCE(created_at, CURRENT_TIMESTAMP()) AS created_at,
        COALESCE(updated_at, CURRENT_TIMESTAMP()) AS updated_at,
        COALESCE(vendor_name, 'Unknown vendor') AS vendor_name,
        COALESCE(min_temp, 0.0) AS min_temp,
        COALESCE(max_temp, 0.0) AS max_temp,
        COALESCE(production_country, 'Unknown country') AS production_country
    FROM
        {{ ref('unified_product_data') }}
)
SELECT * FROM cleaned_data
