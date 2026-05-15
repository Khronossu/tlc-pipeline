{{
    config(
        materialized="table",
        file_format="iceberg",
    )
}}

SELECT
    vendor_id,
    vendor_name,
    current_timestamp() AS _transformed_at
FROM (
    VALUES
        (1, 'Creative Mobile Technologies LLC'),
        (2, 'VeriFone Inc.')
) AS t (vendor_id, vendor_name)
