# Schema Evolution

## TLC Yellow Taxi schema history

The NYC TLC CloudFront parquet files share a unified 19-column schema header, but
two columns were added over time and are semantically null for earlier years:

| Column | Introduced | Null rate before introduction |
|---|---|---|
| `congestion_surcharge` | 2019-02-01 | 100% null for 2018 and 2019-01 |
| `airport_fee` | 2021-01-01 | 100% null for all prior years |

This is **semantic evolution** (null density changes), not physical schema change —
CloudFront serves all files with the same column list. The Bronze Iceberg table DDL
already includes both columns; the evolution pattern this project exercises is
**loading older months after newer months** (or vice versa) without breaking queries.

---

## How schema evolution is handled

### Bronze (`landing_to_bronze.py`)

`align_schema(df, table_name, spark)` runs before every Bronze write:

1. **New column in source, missing from table** → `ALTER TABLE … ADD COLUMN`.
   Safe to call multiple times — the column already existing is a no-op at the
   Iceberg catalog level (but we guard with the column-existence check).
2. **Column in table, missing from source** → `df.withColumn(col, NULL)`.
   This is the standard path when backfilling 2018 data after 2019+ data has
   already evolved the table.

`cast_source_schema` only casts optional columns when they are present in the
source DataFrame — it will not raise `ColumnNotFound` on 2018 files.

### Silver (`stg_yellow_trips.sql`)

```sql
COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
COALESCE(airport_fee, 0)          AS airport_fee,
```

Null Bronze values (from pre-introduction months) are coerced to `0` so Silver
has no nulls for these columns and downstream aggregations do not silently drop rows.

---

## Demo: cross-year query

After loading Jan/Jul of 2018, 2019, 2021, 2024 via the backfill DAG:

```sql
-- Works across all loaded years without column errors
SELECT
    year(tpep_pickup_datetime)  AS year,
    month(tpep_pickup_datetime) AS month,
    COUNT(*)                    AS trips,
    AVG(congestion_surcharge)   AS avg_congestion,
    AVG(airport_fee)            AS avg_airport_fee
FROM iceberg.bronze.yellow_trips
WHERE year(tpep_pickup_datetime) BETWEEN 2018 AND 2024
GROUP BY 1, 2
ORDER BY 1, 2;
```

Expected output (2018 rows show 0 for both surcharge columns):

| year | month | trips    | avg_congestion | avg_airport_fee |
|------|-------|----------|----------------|-----------------|
| 2018 | 1     | ~8.7M    | NULL           | NULL            |
| 2018 | 7     | ~9.1M    | NULL           | NULL            |
| 2019 | 1     | ~7.8M    | 0.64           | NULL            |
| 2019 | 7     | ~6.9M    | 0.69           | NULL            |
| 2021 | 1     | ~1.0M    | 0.45           | 0.02            |
| 2021 | 7     | ~2.8M    | 0.52           | 0.04            |
| 2024 | 1     | ~3.1M    | 0.57           | 0.09            |
| 2024 | 7     | ~3.4M    | 0.61           | 0.08            |

*(NULL in Bronze = column was absent in the source file; Silver coalesces to 0.)*

### Iceberg snapshot history

```sql
-- Shows a snapshot per backfill month:
SELECT snapshot_id, committed_at, summary
FROM iceberg.bronze.yellow_trips.history
ORDER BY committed_at;
```

---

## Running the backfill

```bash
# Trigger the backfill DAG via Airflow CLI:
airflow dags trigger backfill_yellow_taxi \
  --conf '{"start_month": "2018-01", "end_month": "2024-07"}'

# Or via the Airflow UI: DAGs → backfill_yellow_taxi → Trigger w/ config
```

Months are processed **sequentially** (not in parallel) to bound Spark memory.
Each month re-uses the identical pipeline steps as the monthly ingest DAG.

---

## Salt rotation and re-tokenization after backfill

If PII salt is rotated (increment `PII_SALT_VERSION` in `.env`), run:

```bash
# Re-tokenize all loaded months
for year_month in 2018-01 2018-07 2019-01 ...; do
  year=${year_month%-*}
  month=${year_month#*-}
  spark-submit spark/jobs/tokenize_pii.py --year $year --month $month
done
```

The `_salt_version` column in Bronze and `fct_tokenization_audit` tracks which
version was applied per month, so coverage gaps after rotation are visible.
