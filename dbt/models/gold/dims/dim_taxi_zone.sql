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
    _transformed_at             AS effective_from,
    MD5(CAST(location_id AS STRING)) AS zone_sk
FROM {{ ref("stg_taxi_zone") }}
