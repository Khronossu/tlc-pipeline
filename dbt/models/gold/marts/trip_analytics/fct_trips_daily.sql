{{
    config(
        materialized="incremental",
        file_format="iceberg",
        incremental_strategy="insert_overwrite",
        partition_by=[{"field": "pickup_month", "data_type": "date"}],
    )
}}

-- Grain: one row per (pickup_date, pu_location_id, payment_type).

SELECT
    {{ dbt_utils.generate_surrogate_key(["pickup_date", "pu_location_id", "payment_type"]) }}
                                        AS daily_id,
    pickup_date,
    DATE_TRUNC('month', pickup_date)::date
                                        AS pickup_month,
    pu_location_id,
    payment_type,
    COUNT(*)                            AS trip_count,
    SUM(passenger_count)                AS total_passengers,
    SUM(trip_distance)                  AS total_distance_mi,
    SUM(fare_amount)                    AS total_fare,
    SUM(tip_amount)                     AS total_tip,
    SUM(total_amount)                   AS total_revenue,
    AVG(tip_pct)                        AS avg_tip_pct,
    AVG(duration_min)                   AS avg_duration_min,
    current_timestamp()                 AS _transformed_at
FROM (
    SELECT
        CAST(pickup_at AS DATE)         AS pickup_date,
        pickup_month,
        pu_location_id,
        payment_type,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        tip_pct,
        duration_min
    FROM {{ ref("fct_trips") }}
    {% if is_incremental() %}
    WHERE pickup_month = DATE_TRUNC('month', MAKE_DATE({{ var("year") }}, {{ var("month") }}, 1))::date
    {% endif %}
) t
GROUP BY pickup_date, pickup_month, pu_location_id, payment_type
