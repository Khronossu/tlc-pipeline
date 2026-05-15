{{
    config(
        materialized="incremental",
        file_format="iceberg",
        incremental_strategy="insert_overwrite",
        partition_by=[{"field": "pickup_month", "data_type": "date"}],
    )
}}

-- Grain: one row per (pickup_month, _salt_version).
-- Answers: how many trips were tokenized, with which salt version, and what is the token coverage rate?

SELECT
    {{ dbt_utils.generate_surrogate_key(["pickup_month", "_salt_version"]) }}
                                                        AS audit_id,
    pickup_month,
    _salt_version                                       AS salt_version,
    COUNT(*)                                            AS total_trips,
    COUNT(passenger_email_token)                        AS tokenized_count,
    CAST(COUNT(passenger_email_token) AS DOUBLE)
        / NULLIF(COUNT(*), 0)                           AS token_coverage_rate,
    COUNT(DISTINCT passenger_id_token)                  AS distinct_passengers,
    current_timestamp()                                 AS _transformed_at
FROM {{ ref("fct_trips") }}
{% if is_incremental() %}
WHERE pickup_month = DATE_TRUNC('month', MAKE_DATE({{ var("year") }}, {{ var("month") }}, 1))::date
{% endif %}
GROUP BY pickup_month, _salt_version
