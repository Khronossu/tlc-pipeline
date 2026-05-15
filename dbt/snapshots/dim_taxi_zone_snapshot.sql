{% snapshot dim_taxi_zone_snapshot %}

{{
    config(
        target_schema="gold",
        unique_key="location_id",
        strategy="check",
        check_cols=["borough", "zone_name", "service_zone"],
        invalidate_hard_deletes=True,
    )
}}

SELECT
    location_id,
    borough,
    zone_name,
    service_zone,
    _transformed_at
FROM {{ ref("stg_taxi_zone") }}

{% endsnapshot %}
