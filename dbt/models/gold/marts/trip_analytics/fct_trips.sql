{{
    config(
        materialized="incremental",
        file_format="iceberg",
        incremental_strategy="insert_overwrite",
        partition_by=["pickup_month"],
    )
}}

-- Grain: one row per trip (VendorID + tpep_pickup_datetime from Silver).

SELECT
    {{ dbt_utils.generate_surrogate_key(["t.vendor_id", "t.pickup_at", "t.dropoff_at", "t.pu_location_id", "t.do_location_id", "t.fare_amount", "t.total_amount", "t.trip_distance", "t.passenger_count"]) }}
                                                AS trip_id,
    t.vendor_id,
    t.pickup_at,
    t.dropoff_at,
    t.pickup_month,
    CAST(
        (unix_timestamp(t.dropoff_at) - unix_timestamp(t.pickup_at)) / 60.0
        AS DECIMAL(10, 2)
    )                                           AS duration_min,
    t.passenger_count,
    t.trip_distance,
    t.rate_code_id,
    t.pu_location_id,
    t.do_location_id,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.congestion_surcharge,
    t.airport_fee,
    t.total_amount,
    CASE
        WHEN t.fare_amount > 0 THEN
            CAST(t.tip_amount / t.fare_amount * 100.0 AS DECIMAL(10, 2))
        ELSE NULL
    END                                         AS tip_pct,
    -- PDPA tokens only — raw PII never leaves meta.pii_lookup
    t.passenger_email_token,
    t.passenger_phone_token,
    t.payment_card_last4_token,
    t.passenger_id_token,
    t._salt_version,
    -- Provenance
    t._ingested_at,
    t._source_url,
    current_timestamp()                         AS _transformed_at
FROM {{ ref("stg_yellow_trips") }} t
{% if is_incremental() %}
WHERE t.pickup_month = CAST(DATE_TRUNC('month', MAKE_DATE({{ var("year") }}, {{ var("month") }}, 1)) AS DATE)
{% endif %}
