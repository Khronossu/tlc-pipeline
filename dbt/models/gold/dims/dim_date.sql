-- Grain: one row per calendar date, 2018-01-01 through 2026-12-31.
-- Generated entirely in SQL — no source table required.
-- Covers the full TLC demo range (2018 backfill) plus two future years.

WITH spine AS (
    SELECT EXPLODE(SEQUENCE(
        DATE '2018-01-01',
        DATE '2026-12-31',
        INTERVAL 1 DAY
    )) AS date_day
)

SELECT
    date_day,
    CAST(DATE_FORMAT(date_day, 'yyyyMMdd') AS INT)  AS date_key,
    YEAR(date_day)                                   AS year,
    QUARTER(date_day)                                AS quarter,
    MONTH(date_day)                                  AS month,
    WEEKOFYEAR(date_day)                             AS week_of_year,
    DAYOFMONTH(date_day)                             AS day_of_month,
    DAYOFWEEK(date_day)                              AS day_of_week,   -- 1=Sun … 7=Sat
    DATE_FORMAT(date_day, 'EEEE')                    AS day_name,
    DATE_FORMAT(date_day, 'MMMM')                    AS month_name,
    CAST(DATE_TRUNC('month', date_day) AS DATE)      AS month_start,
    CAST(DATE_TRUNC('quarter', date_day) AS DATE)    AS quarter_start,
    CAST(DATE_TRUNC('year', date_day) AS DATE)       AS year_start,
    CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE ELSE FALSE END  AS is_weekend
FROM spine
