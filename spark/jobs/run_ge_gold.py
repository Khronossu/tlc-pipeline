"""Great Expectations gold gate — pure Spark SQL, no GE CLI required."""
import sys
from pyspark.sql import SparkSession

TABLE = "iceberg.gold.fct_trips"

spark = (
    SparkSession.builder
    .appName("ge_gold_gate")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CACHE TABLE ge_gold AS SELECT * FROM {TABLE}")

stats = spark.sql("""
    SELECT
        COUNT(*)                                                AS row_count,
        SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END)       AS null_trip_id,
        SUM(CASE WHEN pickup_at IS NULL THEN 1 ELSE 0 END)     AS null_pickup_at,
        SUM(CASE WHEN pu_location_id IS NULL THEN 1 ELSE 0 END) AS null_pu_loc,
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)  AS null_ingested_at,
        SUM(CASE WHEN _transformed_at IS NULL THEN 1 ELSE 0 END) AS null_transformed_at,
        SUM(CASE WHEN pu_location_id < 1 OR pu_location_id > 265 THEN 1 ELSE 0 END) AS bad_pu_loc,
        SUM(CASE WHEN do_location_id < 1 OR do_location_id > 265 THEN 1 ELSE 0 END) AS bad_do_loc,
        AVG(tip_pct)                                           AS mean_tip_pct,
        AVG(total_amount)                                      AS mean_total,
        SUM(CASE WHEN tip_pct IS NOT NULL AND (tip_pct < 0 OR tip_pct > 200) THEN 1 ELSE 0 END) AS bad_tip_pct,
        SUM(CASE WHEN duration_min IS NOT NULL AND (duration_min < 0 OR duration_min > 600) THEN 1 ELSE 0 END) AS bad_duration
    FROM ge_gold
""").collect()[0]

spark.sql("UNCACHE TABLE ge_gold")

failures = []
n = stats["row_count"]
mostly_threshold = 0.001  # 0.1% tolerance

if n < 500_000 or n > 12_000_000:
    failures.append(f"row_count={n} not in [500000, 12000000]")
if stats["null_trip_id"] > 0:
    failures.append(f"null trip_id: {stats['null_trip_id']}")
if stats["null_pickup_at"] > 0:
    failures.append(f"null pickup_at: {stats['null_pickup_at']}")
if stats["null_pu_loc"] > 0:
    failures.append(f"null pu_location_id: {stats['null_pu_loc']}")
if stats["null_ingested_at"] > 0:
    failures.append(f"null _ingested_at: {stats['null_ingested_at']}")
if stats["null_transformed_at"] > 0:
    failures.append(f"null _transformed_at: {stats['null_transformed_at']}")
if stats["bad_pu_loc"] > n * mostly_threshold:
    failures.append(f"pu_location_id out of range: {stats['bad_pu_loc']}")
if stats["bad_do_loc"] > n * mostly_threshold:
    failures.append(f"do_location_id out of range: {stats['bad_do_loc']}")
mean_tip = stats["mean_tip_pct"] or 0
if mean_tip < 5 or mean_tip > 30:
    failures.append(f"mean tip_pct={mean_tip:.2f} not in [5, 30]")
mean_total = stats["mean_total"] or 0
if mean_total < 10 or mean_total > 50:
    failures.append(f"mean total_amount={mean_total:.2f} not in [10, 50]")
if stats["bad_tip_pct"] > n * mostly_threshold:
    failures.append(f"tip_pct out of range: {stats['bad_tip_pct']}")
if stats["bad_duration"] > n * mostly_threshold:
    failures.append(f"duration_min out of range: {stats['bad_duration']}")

spark.stop()

if failures:
    print("GE Gold FAILED:", failures, flush=True)
    sys.exit(1)

print(f"GE Gold PASSED: {n} rows, mean_tip={mean_tip:.1f}%, mean_total=${mean_total:.2f}", flush=True)
sys.exit(0)
