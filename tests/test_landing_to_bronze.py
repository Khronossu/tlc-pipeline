"""Tests for pure transform functions in landing_to_bronze (no Iceberg, no MinIO)."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.jobs.landing_to_bronze import add_ingestion_metadata, cast_source_schema


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.appName("test_landing_to_bronze")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


RAW_SCHEMA = StructType(
    [
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
    ]
)


def _make_raw_row(spark: SparkSession, overrides: dict = {}) -> "pyspark.sql.DataFrame":  # type: ignore[name-defined]  # noqa: F821
    defaults = {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime(2023, 1, 15, 8, 0, 0),
        "tpep_dropoff_datetime": datetime(2023, 1, 15, 8, 20, 0),
        "passenger_count": 1.0,
        "trip_distance": 2.5,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": 100,
        "DOLocationID": 200,
        "payment_type": 1,
        "fare_amount": 12.5,
        "extra": 0.5,
        "mta_tax": 0.5,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "total_amount": 15.8,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
    }
    defaults.update(overrides)
    return spark.createDataFrame([tuple(defaults.values())], schema=RAW_SCHEMA)  # type: ignore[arg-type]


class TestCastSourceSchema:
    def test_vendor_id_becomes_int(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark))
        assert df.schema["VendorID"].dataType == IntegerType()

    def test_fare_amount_becomes_decimal(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark))
        assert isinstance(df.schema["fare_amount"].dataType, DecimalType)

    def test_passenger_count_nullable(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark, {"passenger_count": None}))
        row = df.collect()[0]
        assert row["passenger_count"] is None

    def test_ratecode_id_nullable(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark, {"RatecodeID": None}))
        row = df.collect()[0]
        assert row["RatecodeID"] is None

    def test_congestion_surcharge_nullable(self, spark: SparkSession) -> None:
        # Null in 2018 rows — must survive the cast without error
        df = cast_source_schema(_make_raw_row(spark, {"congestion_surcharge": None}))
        row = df.collect()[0]
        assert row["congestion_surcharge"] is None

    def test_all_19_columns_present(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark))
        assert len(df.columns) == 19


class TestAddIngestionMetadata:
    def test_five_metadata_columns_added(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark))
        df = add_ingestion_metadata(df, "run-1", "http://example.com", "abc123", 1)
        for col in ["_run_id", "_source_url", "_source_sha256", "_ingested_at", "_schema_version"]:
            assert col in df.columns

    def test_ingested_at_is_timestamp(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark))
        df = add_ingestion_metadata(df, "run-1", "http://example.com", "abc123", 1)
        assert df.schema["_ingested_at"].dataType == TimestampType()

    def test_run_id_value_preserved(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark))
        df = add_ingestion_metadata(df, "my-run-id", "http://example.com", "abc123", 1)
        assert df.collect()[0]["_run_id"] == "my-run-id"

    def test_total_columns_are_24(self, spark: SparkSession) -> None:
        df = cast_source_schema(_make_raw_row(spark))
        df = add_ingestion_metadata(df, "run-1", "http://example.com", "abc123", 1)
        assert len(df.columns) == 24  # 19 source + 5 metadata
