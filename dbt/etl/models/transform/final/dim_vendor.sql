-- models/dim_vendor.sql

-- Create the dimension table for vendor by extracting unique vendors from the source data
WITH dim_vendor_cte AS (
    SELECT DISTINCT
        vendor_id,
        vendor_name,
        gln
    FROM {{ ref('merged_product_data') }}
    WHERE NOT (vendor_name IS NULL AND gln IS NULL)
)
SELECT
    vendor_id,
    vendor_name,
    gln
FROM dim_vendor_cte
