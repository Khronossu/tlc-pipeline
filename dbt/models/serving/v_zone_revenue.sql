{{
    config(
        materialized="table",
        file_format="iceberg",
    )
}}

-- Monthly zone revenue view: top-line numbers per zone for dashboards.

SELECT
    pickup_month,
    pu_location_id,
    borough,
    zone_name,
    service_zone,
    trip_count,
    total_revenue,
    avg_revenue_per_trip,
    total_distance_mi,
    avg_distance_mi,
    total_tips,
    avg_tip_pct
FROM {{ ref("fct_zone_revenue_monthly") }}
