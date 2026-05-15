{{
    config(
        materialized="table",
        file_format="iceberg",
    )
}}

SELECT
    rate_code_id,
    rate_code_desc,
    current_timestamp() AS _transformed_at
FROM (
    VALUES
        (1,  'Standard rate'),
        (2,  'JFK'),
        (3,  'Newark'),
        (4,  'Nassau or Westchester'),
        (5,  'Negotiated fare'),
        (6,  'Group ride'),
        (99, 'Unknown')
) AS t (rate_code_id, rate_code_desc)
