{{
    config(
        materialized="table",
        file_format="iceberg",
    )
}}

-- Pre-joined table for dashboard: daily trip metrics with zone and payment labels.

SELECT
    d.pickup_date,
    d.pickup_month,
    z.borough,
    z.zone_name,
    z.service_zone,
    p.payment_type_desc,
    d.trip_count,
    d.total_passengers,
    d.total_distance_mi,
    d.total_fare,
    d.total_tip,
    d.total_revenue,
    d.avg_tip_pct,
    d.avg_duration_min
FROM {{ ref("fct_trips_daily") }} d
LEFT JOIN {{ ref("dim_taxi_zone") }} z
    ON d.pu_location_id = z.location_id
LEFT JOIN {{ ref("dim_payment_type") }} p
    ON d.payment_type = p.payment_type_id
