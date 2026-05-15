{{
    config(materialized="view")
}}

-- Pre-joined view for dashboard: daily trip metrics with zone and vendor labels.

SELECT
    d.pickup_date,
    d.pickup_month,
    v.vendor_name,
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
LEFT JOIN {{ ref("dim_taxi_zone_snapshot") }} z
    ON d.pu_location_id = z.location_id
    AND z.dbt_valid_to IS NULL
LEFT JOIN {{ ref("dim_vendor") }} v
    ON TRUE  -- vendor_id not in fct_trips_daily; enriched at serving layer via zone grain
LEFT JOIN {{ ref("dim_payment_type") }} p
    ON d.payment_type = p.payment_type_id
