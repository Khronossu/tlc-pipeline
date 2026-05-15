"""Reads Landing parquet, casts schema, aligns with Bronze Iceberg schema, writes Bronze."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DataType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

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
    passenger_email_token   STRING,
    passenger_phone_token   STRING,
    payment_card_last4_token STRING,
    passenger_id_token      STRING,
    _salt_version           INT,
    _run_id                 STRING,
    _source_url             STRING,
    _source_sha256          STRING,
    _ingested_at            TIMESTAMP,
    _schema_version         INT
)
USING iceberg
PARTITIONED BY (months(tpep_pickup_datetime))
"""

# Columns that appear only in later TLC years. Cast only when present in source.
# congestion_surcharge: 2019-02+   airport_fee: 2021-01+
_OPTIONAL_DECIMAL_COLS = {"congestion_surcharge", "airport_fee"}


def _spark_type_to_ddl(dtype: DataType) -> str:
    """Convert a Spark DataType to its Iceberg DDL type string."""
    if isinstance(dtype, IntegerType):
        return "INT"
    if isinstance(dtype, LongType):
        return "BIGINT"
    if isinstance(dtype, DecimalType):
        return f"DECIMAL({dtype.precision}, {dtype.scale})"
    if isinstance(dtype, TimestampType):
        return "TIMESTAMP"
    return "STRING"


def build_spark_session(settings: Settings) -> SparkSession:
    return (
        SparkSession.builder.appName("landing_to_bronze")
        .master(settings.spark_master)
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", settings.iceberg_rest_url)
        .config("spark.sql.catalog.iceberg.warehouse", settings.iceberg_warehouse)
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def cast_source_schema(df: DataFrame) -> DataFrame:
    """Apply Bronze types to the raw Landing DataFrame.

    Pure function. Columns that appeared in later TLC years (congestion_surcharge,
    airport_fee) are cast only when present in the source — older files omit them.
    align_schema() will backfill those columns with NULL before writing.
    """
    df = (
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
        .withColumn(
            "improvement_surcharge",
            F.col("improvement_surcharge").cast(DecimalType(10, 2)),
        )
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(10, 2)))
    )
    for col in _OPTIONAL_DECIMAL_COLS:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DecimalType(10, 2)))
    return df


def align_schema(df: DataFrame, table_name: str, spark: SparkSession) -> DataFrame:
    """Align df with the existing Iceberg table schema — the key schema-evolution step.

    Two cases handled:
      1. Column in df but not in table  → ALTER TABLE ADD COLUMN (additive evolution).
         This is idempotent: running again for the same month is a no-op because the
         column already exists.
      2. Column in table but not in df  → fill with NULL so writeTo does not fail.
         Example: loading 2018 data after 2019 data has added congestion_surcharge.

    Metadata columns (_run_id etc.) are excluded from the ALTER TABLE guard because
    they are always added by add_ingestion_metadata() after this call.
    """
    try:
        table_schema = spark.table(table_name).schema
    except Exception:
        return df  # table does not exist yet; ensure_bronze_table() will create it

    table_col_map: dict[str, DataType] = {f.name: f.dataType for f in table_schema.fields}
    df_col_set = set(df.columns)

    # Case 1: new columns arriving in the source file
    for field in df.schema.fields:
        if field.name not in table_col_map:
            ddl_type = _spark_type_to_ddl(field.dataType)
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {field.name} {ddl_type}")

    # Case 2: existing table columns absent from the source file
    for col_name, col_type in table_col_map.items():
        if col_name not in df_col_set:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))

    return df


def add_ingestion_metadata(
    df: DataFrame,
    run_id: str,
    source_url: str,
    source_sha256: str,
    schema_version: int,
) -> DataFrame:
    """Append ingestion metadata columns. Pure function."""
    ingested_at = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
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

    landing_path = f"s3a://landing/yellow_taxi/year={year}/month={month:02d}/data.parquet"
    df = spark.read.parquet(landing_path)
    df = cast_source_schema(df)
    df = align_schema(df, BRONZE_TABLE, spark)
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
