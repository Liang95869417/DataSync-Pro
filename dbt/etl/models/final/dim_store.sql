-- dim_store.sql

SELECT DISTINCT
    GENERATE_UUID() AS store_id,
    group,
    name,
    openingHours,
    detailUrl AS url,
    fax,
    email,
    phone,
    logo AS logo_url,
    address,
    position,
    website

FROM datasync-pro.raw_dataset.kassal_store_data_test

