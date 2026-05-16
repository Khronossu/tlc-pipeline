"""Bronze quality gate: implements bronze_yellow_trips_suite expectations via Spark SQL.

Exits 0 if all checks pass, 1 if any fail. Uses SQL aggregation so Iceberg can
push down predicates and Spark doesn't materialise the full dataset in driver heap.
"""

from __future__ import annotations

import argparse
import sys

from spark.jobs.config import Settings
from spark.jobs.landing_to_bronze import build_spark_session

BRONZE_TABLE = "iceberg.bronze.yellow_trips"


def run_checks(year: int, month: int, settings: Settings) -> list[str]:
    spark = build_spark_session(settings)
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    base = (
        f"SELECT * FROM {BRONZE_TABLE} "
        f"WHERE year(tpep_pickup_datetime) = {year} AND month(tpep_pickup_datetime) = {month}"
    )
    spark.sql(base).createOrReplaceTempView("bronze_month")

    failures: list[str] = []

    # ── Single SQL pass for all scalar checks ────────────────────────────────
    stats = spark.sql("""
        SELECT
            COUNT(*)                                                   AS row_count,
            AVG(fare_amount)                                           AS mean_fare,
            AVG(trip_distance)                                         AS mean_dist,
            percentile_approx(trip_distance, 0.99)                    AS p99_dist,
            SUM(CASE WHEN passenger_count IS NOT NULL
                      AND passenger_count NOT BETWEEN 0 AND 9
                 THEN 1 ELSE 0 END)                                    AS bad_pax,
            SUM(CASE WHEN PULocationID NOT BETWEEN 1 AND 265
                 THEN 1 ELSE 0 END)                                    AS bad_pu,
            SUM(CASE WHEN DOLocationID NOT BETWEEN 1 AND 265
                 THEN 1 ELSE 0 END)                                    AS bad_do,
            SUM(CASE WHEN payment_type IS NOT NULL
                      AND payment_type NOT IN (0,1,2,3,4)
                 THEN 1 ELSE 0 END)                                    AS bad_pay
        FROM bronze_month
    """).collect()[0]

    count = stats.row_count
    print(f"[GE] row_count={count:,} mean_fare={stats.mean_fare:.2f} "
          f"mean_dist={stats.mean_dist:.2f} p99_dist={stats.p99_dist:.2f}")

    if not (500_000 <= count <= 12_000_000):
        failures.append(f"row_count {count:,} not in [500000, 12000000]")
    if not (10 <= (stats.mean_fare or 0) <= 30):
        failures.append(f"mean_fare {stats.mean_fare:.2f} not in [10, 30]")
    if not (1 <= (stats.mean_dist or 0) <= 8):
        failures.append(f"mean_trip_distance {stats.mean_dist:.2f} not in [1, 8]")
    if not (15 <= (stats.p99_dist or 0) <= 30):
        failures.append(f"p99_trip_distance {stats.p99_dist:.2f} not in [15, 30]")
    if stats.bad_pax > 0:
        failures.append(f"{stats.bad_pax} rows with passenger_count outside [0, 9]")
    if stats.bad_pu > 0:
        failures.append(f"{stats.bad_pu} rows with PULocationID outside [1, 265]")
    if stats.bad_do > 0:
        failures.append(f"{stats.bad_do} rows with DOLocationID outside [1, 265]")
    if stats.bad_pay > 0:
        failures.append(f"{stats.bad_pay} rows with invalid payment_type")

    # Token length: separate pass, only if columns exist
    columns = [r.col_name for r in spark.sql("DESCRIBE bronze_month").collect()]
    for col in ("passenger_email_token", "passenger_id_token"):
        if col in columns:
            bad = spark.sql(f"""
                SELECT COUNT(*) FROM bronze_month
                WHERE {col} IS NOT NULL AND LENGTH({col}) != 64
            """).collect()[0][0]
            if bad > 0:
                failures.append(f"{bad} rows with {col} length != 64")

    spark.stop()
    return failures


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    from spark.jobs.config import settings as default_settings

    failed = run_checks(args.year, args.month, default_settings)
    if failed:
        print("[GE] FAILED:")
        for f in failed:
            print(f"  - {f}")
        sys.exit(1)
    print("[GE] All checks PASSED")
    sys.exit(0)
