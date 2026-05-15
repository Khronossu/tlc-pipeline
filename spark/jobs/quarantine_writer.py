"""Routes failed Bronze rows to the quarantine Iceberg table with full provenance."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from spark.jobs.config import Settings
from spark.jobs.landing_to_bronze import build_spark_session

QUARANTINE_TABLE = "iceberg.quarantine.yellow_trips"
BRONZE_TABLE = "iceberg.bronze.yellow_trips"

CREATE_QUARANTINE_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
    VendorID                INT,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         INT,
    trip_distance           DECIMAL(10, 2),
    RatecodeID              INT,
    store_and_fwd_flag      STRING,
    PULocationID            INT,
    DOLocationID            INT,
    payment_type            INT,
    fare_amount             DECIMAL(10, 2),
    extra                   DECIMAL(10, 2),
    mta_tax                 DECIMAL(10, 2),
    tip_amount              DECIMAL(10, 2),
    tolls_amount            DECIMAL(10, 2),
    improvement_surcharge   DECIMAL(10, 2),
    total_amount            DECIMAL(10, 2),
    congestion_surcharge    DECIMAL(10, 2),
    airport_fee             DECIMAL(10, 2),
    passenger_email_token   STRING,
    passenger_phone_token   STRING,
    payment_card_last4_token STRING,
    passenger_id_token      STRING,
    _salt_version           INT,
    _run_id                 STRING,
    _source_url             STRING,
    _source_sha256          STRING,
    _ingested_at            TIMESTAMP,
    _schema_version         INT,
    _failure_reason         STRING,
    _expectation_kwargs     STRING,
    _quarantine_run_id      STRING
)
USING iceberg
PARTITIONED BY (days(tpep_pickup_datetime), _failure_reason)
"""


def ensure_quarantine_table(spark: SparkSession) -> None:
    spark.sql(CREATE_QUARANTINE_DDL)


def write_quarantined(
    df: DataFrame,
    failure_reason: str,
    expectation_kwargs: str,
    quarantine_run_id: str,
    spark: SparkSession,
) -> int:
    """Append failed rows to quarantine with provenance columns. Returns row count written."""
    quarantine_df = (
        df.withColumn("_failure_reason", F.lit(failure_reason).cast(StringType()))
        .withColumn("_expectation_kwargs", F.lit(expectation_kwargs).cast(StringType()))
        .withColumn("_quarantine_run_id", F.lit(quarantine_run_id).cast(StringType()))
    )
    quarantine_df.writeTo(QUARANTINE_TABLE).append()
    return quarantine_df.count()


def run(
    year: int,
    month: int,
    failure_reason: str,
    quarantine_run_id: str,
    settings: Settings,
    expectation_kwargs: str = "{}",
) -> int:
    spark = build_spark_session(settings)
    ensure_quarantine_table(spark)

    failed_df = spark.sql(
        f"SELECT * FROM {BRONZE_TABLE} "
        f"WHERE year(tpep_pickup_datetime) = {year} AND month(tpep_pickup_datetime) = {month}"
    )

    count = write_quarantined(
        failed_df, failure_reason, expectation_kwargs, quarantine_run_id, spark
    )
    spark.stop()
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--failure-reason", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expectation-kwargs", default="{}")
    args = parser.parse_args()

    from spark.jobs.config import settings as default_settings

    count = run(
        year=args.year,
        month=args.month,
        failure_reason=args.failure_reason,
        quarantine_run_id=args.run_id,
        settings=default_settings,
        expectation_kwargs=args.expectation_kwargs,
    )
    print(f"Quarantined {count:,} rows with reason: {args.failure_reason}")
