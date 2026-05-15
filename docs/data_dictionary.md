# Data Dictionary — NYC TLC Yellow Taxi (Bronze source contract)

> Generated from empirical inspection of `yellow_tripdata_{2018,2019,2021,2023}-01.parquet` from `d37ci6vzurychx.cloudfront.net`. This document is the source of truth for Spark type casts, GE expectation bounds, and Silver coalescing rules.

**Inspection date:** 2026-05-15
**Tooling:** DuckDB 1.5.2 against the four sample parquets
**Sample sizes:** 2018-01 = 8,760,687 rows · 2019-01 = 7,696,617 · 2021-01 = 1,369,769 · 2023-01 = 3,066,766

---

## 1. Column inventory (19 columns, all years)

TLC has **retroactively unified the parquet schema** across years on CloudFront — every monthly file from 2018 onward exposes the same 19 columns. "Schema evolution" in this dataset is **semantic** (when a column starts carrying values), not **physical** (columns appearing/disappearing). The Bronze loader can rely on a stable column list.

| # | Column | Source type | Bronze type (cast) | Notes |
|---|---|---|---|---|
| 1 | `VendorID` | BIGINT | `INT` | 1 = Creative Mobile, 2 = VeriFone; rarely 6/7 in some months |
| 2 | `tpep_pickup_datetime` | TIMESTAMP | `TIMESTAMP` | Local NYC time, no tz |
| 3 | `tpep_dropoff_datetime` | TIMESTAMP | `TIMESTAMP` | Local NYC time, no tz |
| 4 | `passenger_count` | DOUBLE | `INT` (nullable) | Stored as double upstream — cast safely |
| 5 | `trip_distance` | DOUBLE | `DECIMAL(10,2)` | Miles |
| 6 | `RatecodeID` | DOUBLE | `INT` (nullable) | See §3 categorical map; `99` appears as "unknown" |
| 7 | `store_and_fwd_flag` | VARCHAR | `STRING` | `Y` / `N` / `NULL` |
| 8 | `PULocationID` | BIGINT | `INT` | FK to taxi-zone lookup; range 1–265 observed |
| 9 | `DOLocationID` | BIGINT | `INT` | FK to taxi-zone lookup; range 1–265 observed |
| 10 | `payment_type` | BIGINT | `INT` | See §3 |
| 11 | `fare_amount` | DOUBLE | `DECIMAL(10,2)` | USD; can be negative on disputes |
| 12 | `extra` | DOUBLE | `DECIMAL(10,2)` | USD |
| 13 | `mta_tax` | DOUBLE | `DECIMAL(10,2)` | USD |
| 14 | `tip_amount` | DOUBLE | `DECIMAL(10,2)` | USD; **non-cash tips only** per TLC |
| 15 | `tolls_amount` | DOUBLE | `DECIMAL(10,2)` | USD |
| 16 | `improvement_surcharge` | DOUBLE | `DECIMAL(10,2)` | USD |
| 17 | `total_amount` | DOUBLE | `DECIMAL(10,2)` | USD; sum of components |
| 18 | `congestion_surcharge` | DOUBLE | `DECIMAL(10,2)` | Semantically introduced **Jan 2019** |
| 19 | `airport_fee` | DOUBLE | `DECIMAL(10,2)` | Semantically introduced **2021** |

**Zero PII columns.** TLC strips identifying fields before publication.

---

## 2. Semantic schema evolution (null density of evolved columns)

| Month | Rows | `congestion_surcharge` null % | `airport_fee` null % | Interpretation |
|---|---:|---:|---:|---|
| 2018-01 | 8,760,687 | 100.0% | 100.0% | Both columns absent in source; surfaced as null after CloudFront unification |
| 2019-01 | 7,696,617 | 63.5% | 100.0% | `congestion_surcharge` turned on mid-month |
| 2021-01 | 1,369,769 | 7.2% | 100.0% | `congestion_surcharge` fully populated; `airport_fee` still off |
| 2023-01 | 3,066,766 | 2.3% | 2.3% | Both populated; residual nulls correlate with `payment_type = 0` rows |

**Silver rule:** `coalesce(congestion_surcharge, 0)` and `coalesce(airport_fee, 0)` with a documented comment referencing the introduction year. **Do not** coalesce in Bronze — Bronze preserves source fidelity.

**Schema-evolution demo query** (after backfill spans the boundary):

```sql
SELECT year(pickup_date) y,
       AVG(congestion_surcharge) avg_cs,
       AVG(airport_fee)          avg_af
FROM bronze.yellow_trips
GROUP BY 1 ORDER BY 1;
```

---

## 3. Categorical reference values (from 2023-01)

### `VendorID`

| Code | Meaning | 2023-01 count |
|---|---|---:|
| 1 | Creative Mobile Technologies | 827,367 |
| 2 | VeriFone Inc. | 2,239,399 |

### `payment_type`

| Code | Meaning | 2023-01 count |
|---|---|---:|
| 1 | Credit card | 2,411,462 |
| 2 | Cash | 532,241 |
| 0 | (undocumented; correlates with all-null rows) | 71,743 |
| 4 | Dispute | 33,297 |
| 3 | No charge | 18,023 |

> **Anomaly:** `payment_type = 0` is not in the TLC data dictionary. These 71,743 rows have `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, and `airport_fee` **all null simultaneously**. Treat as "unknown / system rows" — keep in Bronze, document in `dim_payment_type` as code `0 = 'Unknown'`.

### `RatecodeID`

| Code | Meaning | 2023-01 count |
|---|---|---:|
| 1 | Standard rate | 2,839,305 |
| 2 | JFK | 114,239 |
| 5 | Negotiated fare | 15,043 |
| 99 | (undocumented; treat as Unknown) | 13,106 |
| 3 | Newark | 8,958 |
| 4 | Nassau/Westchester | 4,366 |
| 6 | Group ride | 6 |

### `store_and_fwd_flag`

`N` (97.0%), `Y` (0.7%), `NULL` (2.3%).

---

## 4. Value ranges and known anomalies (2023-01)

These ranges are the empirical basis for GE expectation bounds at Bronze.

| Metric | Observed | Comment |
|---|---|---|
| `tpep_pickup_datetime` | 2008-12-31 23:01 → 2023-02-01 00:56 | **38 rows before month, 10 rows after** — clock/sensor errors |
| `fare_amount` | min `-900.00`, max `1160.10` | Negatives are dispute reversals |
| `total_amount` | min `-751.00`, max `1169.40` | Same |
| `trip_distance` | min `0.00`, max `258,928.15` | **88 rows > 100 miles** — sensor errors; p99 is only 20.06 mi |
| `passenger_count` | 0 – 9 | **51,164 zero-passenger rows** |
| `PULocationID` / `DOLocationID` | 1 – 265 | Matches taxi-zone lookup range |

### Anomaly counts (2023-01, raw)

| Anomaly | Count | Bronze action |
|---|---:|---|
| Negative fare | 25,049 | Keep — Silver flags `is_dispute` |
| Zero fare | 1,110 | Keep |
| Zero distance | 45,862 | Keep |
| Trip distance > 100 mi | 88 | GE p99 quantile gate flags |
| Zero passenger | 51,164 | Keep — Silver flags `passenger_count_missing` |
| Dropoff < pickup (negative duration) | 3 | Quarantine via GE pair check |

### Quantiles (2023-01)

| Column | p50 | p95 | p99 |
|---|---:|---:|---:|
| `trip_distance` | 1.80 | 14.32 | 20.06 |
| `fare_amount` | 12.80 | 65.30 | 72.30 |

---

## 5. Implications for downstream layers

### Bronze contract
- Preserve source fidelity — do **not** drop or coalesce.
- Add ingestion metadata: `_run_id`, `_source_url`, `_source_sha256`, `_ingested_at`, `_schema_version`.
- PII tokens (`passenger_email_token`, `passenger_phone_token`, `payment_card_last4_token`, `passenger_id_token`) are appended from the fabricated `meta.pii_lookup` and tokenized in-flight; no clear text persists in Bronze.

### GE Bronze suite — empirically-grounded bounds

| Expectation | Threshold |
|---|---|
| `expect_table_row_count_to_be_between` | rolling mean ±2σ; January 2023 anchor = 3,066,766 |
| `expect_column_values_to_be_between(passenger_count)` | `(0, 9)` |
| `expect_column_values_to_be_between(PULocationID)` | `(1, 265)` |
| `expect_column_values_to_be_between(DOLocationID)` | `(1, 265)` |
| `expect_column_quantile_values_to_be_between(trip_distance, 0.99)` | `(15, 30)` — guards the 258,928-mile sensor error |
| `expect_column_quantile_values_to_be_between(fare_amount, 0.99)` | `(50, 150)` |
| `expect_column_pair_values_A_to_be_greater_than_B(tpep_dropoff_datetime, tpep_pickup_datetime)` | strict |
| `expect_column_value_lengths_to_equal(*_token, 64)` | tripwire — verifies tokenization fired |
| `expect_column_kl_divergence_to_be_less_than(payment_type)` | drift vs reference month |

### Silver rules
- `coalesce(congestion_surcharge, 0)` — column semantically absent before 2019.
- `coalesce(airport_fee, 0)` — column semantically absent before 2021.
- Derive `trip_duration_sec = unix_timestamp(dropoff) - unix_timestamp(pickup)`.
- Filter out-of-month timestamps (`pickup < first_of_month` or `pickup >= first_of_next_month`) into a `__late_or_early` partition or drop with a flag — decision documented at model level.
- Stamp `_transformed_at = current_timestamp()`.

### Dimensional Gold notes
- `dim_payment_type` must include code `0 = 'Unknown'` (undocumented but present).
- `dim_rate_code` must include code `99 = 'Unknown'`.
- `dim_taxi_zone` natural key range 1–265.

---

## 6. Reproducibility

Reproduce this dictionary:

```bash
python3 -m venv .venv && .venv/bin/pip install duckdb pandas
mkdir -p /tmp/tlc_recon
for ym in 2018-01 2019-01 2021-01 2023-01; do
  curl -s -o /tmp/tlc_recon/yellow_${ym}.parquet \
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${ym}.parquet
done
# then re-run the profile queries from this file
```
