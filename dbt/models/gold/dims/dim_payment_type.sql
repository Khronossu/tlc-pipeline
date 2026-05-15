{{
    config(
        materialized="table",
        file_format="iceberg",
    )
}}

SELECT
    payment_type_id,
    payment_type_desc,
    current_timestamp() AS _transformed_at
FROM (
    VALUES
        (0, 'Voided trip'),
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute')
) AS t (payment_type_id, payment_type_desc)
