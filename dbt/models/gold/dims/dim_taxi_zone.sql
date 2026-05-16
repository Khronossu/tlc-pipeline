{{
    config(
        materialized="table",
        file_format="iceberg",
    )
}}

SELECT
    location_id,
    zone_name,
    borough,
    service_zone,
    dbt_valid_from                AS effective_from,
    dbt_scd_id                   AS zone_sk
FROM {{ ref("dim_taxi_zone_snapshot") }}
WHERE dbt_valid_to IS NULL
