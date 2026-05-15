{{
    config(
        materialized="incremental",
        file_format="iceberg",
        incremental_strategy="insert_overwrite",
        partition_by=[{"field": "pickup_month", "data_type": "date"}],
    )
}}

-- Grain: one row per (pickup_month, pu_location_id).

SELECT
    {{ dbt_utils.generate_surrogate_key(["pickup_month", "pu_location_id"]) }}
                                        AS zone_month_id,
    t.pickup_month,
    t.pu_location_id,
    z.borough,
    z.zone_name,
    z.service_zone,
    COUNT(*)                            AS trip_count,
    SUM(t.total_amount)                 AS total_revenue,
    AVG(t.total_amount)                 AS avg_revenue_per_trip,
    SUM(t.trip_distance)                AS total_distance_mi,
    AVG(t.trip_distance)                AS avg_distance_mi,
    SUM(t.tip_amount)                   AS total_tips,
    AVG(t.tip_pct)                      AS avg_tip_pct,
    current_timestamp()                 AS _transformed_at
FROM {{ ref("fct_trips") }} t
LEFT JOIN {{ ref("dim_taxi_zone_snapshot") }} z
    ON t.pu_location_id = z.location_id
    AND z.dbt_valid_to IS NULL  -- current SCD2 record only
{% if is_incremental() %}
WHERE t.pickup_month = DATE_TRUNC('month', MAKE_DATE({{ var("year") }}, {{ var("month") }}, 1))::date
{% endif %}
GROUP BY t.pickup_month, t.pu_location_id, z.borough, z.zone_name, z.service_zone
