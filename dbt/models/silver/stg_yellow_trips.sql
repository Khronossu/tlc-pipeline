{{
    config(
        materialized="incremental",
        file_format="iceberg",
        incremental_strategy="insert_overwrite",
        partition_by=[{"field": "pickup_month", "data_type": "date"}],
    )
}}

SELECT
    VendorID                                            AS vendor_id,
    tpep_pickup_datetime                                AS pickup_at,
    tpep_dropoff_datetime                               AS dropoff_at,
    DATE_TRUNC('month', tpep_pickup_datetime)::date     AS pickup_month,
    passenger_count,
    trip_distance,
    RatecodeID                                          AS rate_code_id,
    store_and_fwd_flag,
    PULocationID                                        AS pu_location_id,
    DOLocationID                                        AS do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    COALESCE(congestion_surcharge, 0)                   AS congestion_surcharge,
    -- airport_fee added 2021-01; older partitions land as NULL → coerce to 0
    COALESCE(airport_fee, 0)                            AS airport_fee,
    -- PDPA tokens — raw PII never leaves meta.pii_lookup
    passenger_email_token,
    passenger_phone_token,
    payment_card_last4_token,
    passenger_id_token,
    _salt_version,
    -- Provenance: carry Bronze timestamps forward so latency is queryable end-to-end
    _ingested_at,
    _source_url,
    _source_sha256,
    _schema_version,
    current_timestamp()                                 AS _transformed_at
FROM {{ source("bronze", "yellow_trips") }}
{% if is_incremental() %}
WHERE year(tpep_pickup_datetime) = {{ var("year") }}
  AND month(tpep_pickup_datetime) = {{ var("month") }}
{% endif %}
