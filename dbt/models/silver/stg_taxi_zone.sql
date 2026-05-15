{{
    config(
        materialized="table",
        file_format="iceberg",
    )
}}

SELECT
    CAST(LocationID AS INT)     AS location_id,
    Borough                     AS borough,
    Zone                        AS zone_name,
    service_zone,
    current_timestamp()         AS _transformed_at
FROM {{ ref("taxi_zone_lookup") }}
