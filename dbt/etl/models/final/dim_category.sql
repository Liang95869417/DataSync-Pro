-- dim_category.sql

SELECT DISTINCT
  GENERATE_UUID() AS category_id,
  vda_category AS name,
  NULL AS parent_category_id  -- Update logic later
FROM
  datasync-pro.intermediate_dataset.merge_product_data
WHERE
  vda_category IS NOT NULL
