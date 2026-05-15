"""Reads Landing parquet, casts schema, adds metadata, writes Bronze Iceberg table."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType, TimestampType

from spark.jobs.config import Settings

BRONZE_TABLE = "iceberg.bronze.yellow_trips"

CREATE_BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
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
    _run_id                 STRING,
    _source_url             STRING,
    _source_sha256          STRING,
    _ingested_at            TIMESTAMP,
    _schema_version         INT
)
USING iceberg
PARTITIONED BY (months(tpep_pickup_datetime))
"""


def build_spark_session(settings: Settings) -> SparkSession:
    return (
        SparkSession.builder.appName("landing_to_bronze")
        .master(settings.spark_master)
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", settings.iceberg_rest_url)
        .config("spark.sql.catalog.iceberg.warehouse", settings.iceberg_warehouse)
        .config(
            "spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.iceberg.s3.endpoint", settings.minio_endpoint)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_root_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_root_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .getOrCreate()
    )


def cast_source_schema(df: DataFrame) -> DataFrame:
    """Apply exact Bronze types to the raw Landing DataFrame.

    Pure function — takes a DataFrame, returns a DataFrame.
    All casts are explicit; no schema inference is relied upon.
    """
    return (
        df.withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DecimalType(10, 2)))
        .withColumn("RatecodeID", F.col("RatecodeID").cast(IntegerType()))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast(StringType()))
        .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
        .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DecimalType(10, 2)))
        .withColumn("extra", F.col("extra").cast(DecimalType(10, 2)))
        .withColumn("mta_tax", F.col("mta_tax").cast(DecimalType(10, 2)))
        .withColumn("tip_amount", F.col("tip_amount").cast(DecimalType(10, 2)))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DecimalType(10, 2)))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DecimalType(10, 2)))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(10, 2)))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(DecimalType(10, 2)))
        .withColumn("airport_fee", F.col("airport_fee").cast(DecimalType(10, 2)))
    )


def add_ingestion_metadata(
    df: DataFrame,
    run_id: str,
    source_url: str,
    source_sha256: str,
    schema_version: int,
) -> DataFrame:
    """Append ingestion metadata columns. Pure function."""
    ingested_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return (
        df.withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_url", F.lit(source_url))
        .withColumn("_source_sha256", F.lit(source_sha256))
        .withColumn("_ingested_at", F.lit(ingested_at).cast(TimestampType()))
        .withColumn("_schema_version", F.lit(schema_version))
    )


def ensure_bronze_table(spark: SparkSession) -> None:
    spark.sql(CREATE_BRONZE_DDL)


def write_bronze(df: DataFrame, spark: SparkSession) -> int:
    """Partition-overwrite write to Bronze. Returns row count written."""
    df.writeTo(BRONZE_TABLE).overwritePartitions()
    return spark.table(BRONZE_TABLE).count()


def run(year: int, month: int, run_id: str, source_url: str, source_sha256: str) -> int:
    from spark.jobs.config import settings

    spark = build_spark_session(settings)
    ensure_bronze_table(spark)

    landing_path = (
        f"s3a://landing/yellow_taxi/year={year}/month={month:02d}/data.parquet"
    )
    df = spark.read.parquet(landing_path)
    df = cast_source_schema(df)
    df = add_ingestion_metadata(df, run_id, source_url, source_sha256, schema_version=1)
    row_count = write_bronze(df, spark)
    spark.stop()
    return row_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--source-sha256", required=True)
    args = parser.parse_args()

    count = run(
        year=args.year,
        month=args.month,
        run_id=args.run_id,
        source_url=args.source_url,
        source_sha256=args.source_sha256,
    )
    print(f"Wrote {count:,} rows to {BRONZE_TABLE}")
