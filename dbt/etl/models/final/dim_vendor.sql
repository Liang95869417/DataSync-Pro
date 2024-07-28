-- dim_vendor.sql

SELECT DISTINCT
    GENERATE_UUID() AS vendor_id,
    vendor AS name,
    gln,
    production_country AS country
FROM datasync-pro.intermediate_dataset.merge_product_data
WHERE vendor IS NOT NULL
