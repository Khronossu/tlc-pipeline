-- Grain: one row per active TLC zone (265 rows). Exposes the current SCD2 record
-- from dim_taxi_zone_snapshot (dbt_valid_to IS NULL = current version).
-- Callers should join on location_id; use dbt_scd_id if they need the surrogate key.

SELECT
    location_id,
    zone_name,
    borough,
    service_zone,
    dbt_valid_from                AS effective_from,
    dbt_scd_id                   AS zone_sk
FROM {{ ref("dim_taxi_zone_snapshot") }}
WHERE dbt_valid_to IS NULL
