-- models/dim_vendor.sql

-- Create the dimension table for vendor by extracting unique vendors from the source data
WITH dim_vendor_cte AS (
    SELECT
        vendor_id,
        vendor_name,
        ARRAY_AGG(DISTINCT gln IGNORE NULLS) AS gln_list
    FROM {{ ref('merged_product_data') }}
    WHERE vendor_name IS NOT NULL OR gln IS NOT NULL
    GROUP BY vendor_id, vendor_name
)
SELECT
    vendor_id,
    vendor_name,
    gln_list
FROM dim_vendor_cte
