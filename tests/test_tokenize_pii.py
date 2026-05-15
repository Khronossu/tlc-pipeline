"""Tests for tokenize_pii pure functions (no Spark needed for tokenize(), Spark for the rest)."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.jobs.tokenize_pii import TOKEN_COLUMNS, tokenize, tokenize_bronze_partition


# ── Pure function tests (no Spark) ───────────────────────────────────────────


def test_tokenize_deterministic() -> None:
    t1 = tokenize("user@example.com", "salt123")
    t2 = tokenize("user@example.com", "salt123")
    assert t1 == t2


def test_tokenize_output_length() -> None:
    result = tokenize("user@example.com", "salt123")
    assert result is not None
    assert len(result) == 64


def test_tokenize_salt_sensitive() -> None:
    t1 = tokenize("user@example.com", "salt_a")
    t2 = tokenize("user@example.com", "salt_b")
    assert t1 != t2


def test_tokenize_none_input_returns_none() -> None:
    assert tokenize(None, "any_salt") is None


def test_tokenize_empty_string_produces_token() -> None:
    result = tokenize("", "salt123")
    assert result is not None
    assert len(result) == 64


def test_tokenize_output_is_hex() -> None:
    result = tokenize("test_value", "salt123")
    assert result is not None
    int(result, 16)  # raises ValueError if not valid hex


# ── Spark integration tests for tokenize_bronze_partition ────────────────────


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.appName("test_tokenize_pii")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


BRONZE_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DecimalType(10, 2), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DecimalType(10, 2), True),
        StructField("total_amount", DecimalType(10, 2), True),
    ]
)

LOOKUP_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("passenger_email", StringType(), True),
        StructField("passenger_phone", StringType(), True),
        StructField("payment_card_last4", StringType(), True),
        StructField("passenger_id", StringType(), True),
    ]
)

_PICKUP = datetime(2023, 1, 15, 8, 0, 0)


def test_tokenize_bronze_partition_adds_token_columns(spark: SparkSession) -> None:
    bronze_df = spark.createDataFrame(
        [(2, _PICKUP, _PICKUP, 1, None, 1, None, None)],
        schema=BRONZE_SCHEMA,
    )
    lookup_df = spark.createDataFrame(
        [(2, _PICKUP, "user@example.com", "555-000-0001", "1234", "pid-abc")],
        schema=LOOKUP_SCHEMA,
    )

    result = tokenize_bronze_partition(bronze_df, lookup_df, salt="test_salt", salt_version=1)

    for col in TOKEN_COLUMNS:
        assert col in result.columns, f"Missing token column: {col}"
    assert "_salt_version" in result.columns


def test_tokenize_bronze_partition_token_length_is_64(spark: SparkSession) -> None:
    bronze_df = spark.createDataFrame(
        [(2, _PICKUP, _PICKUP, 1, None, 1, None, None)],
        schema=BRONZE_SCHEMA,
    )
    lookup_df = spark.createDataFrame(
        [(2, _PICKUP, "user@example.com", "555-000-0001", "1234", "pid-abc")],
        schema=LOOKUP_SCHEMA,
    )

    result = tokenize_bronze_partition(bronze_df, lookup_df, salt="test_salt", salt_version=1)
    row = result.collect()[0]

    assert row["passenger_email_token"] is not None
    assert len(row["passenger_email_token"]) == 64


def test_tokenize_bronze_partition_salt_version_stamped(spark: SparkSession) -> None:
    bronze_df = spark.createDataFrame(
        [(2, _PICKUP, _PICKUP, 1, None, 1, None, None)],
        schema=BRONZE_SCHEMA,
    )
    lookup_df = spark.createDataFrame(
        [(2, _PICKUP, "user@example.com", "555-000-0001", "1234", "pid-abc")],
        schema=LOOKUP_SCHEMA,
    )

    result = tokenize_bronze_partition(bronze_df, lookup_df, salt="test_salt", salt_version=7)
    assert result.collect()[0]["_salt_version"] == 7


def test_tokenize_bronze_partition_different_salts_produce_different_tokens(
    spark: SparkSession,
) -> None:
    bronze_df = spark.createDataFrame(
        [(2, _PICKUP, _PICKUP, 1, None, 1, None, None)],
        schema=BRONZE_SCHEMA,
    )
    lookup_df = spark.createDataFrame(
        [(2, _PICKUP, "user@example.com", "555-000-0001", "1234", "pid-abc")],
        schema=LOOKUP_SCHEMA,
    )

    r1 = tokenize_bronze_partition(bronze_df, lookup_df, salt="salt_a", salt_version=1)
    r2 = tokenize_bronze_partition(bronze_df, lookup_df, salt="salt_b", salt_version=2)

    token1 = r1.collect()[0]["passenger_email_token"]
    token2 = r2.collect()[0]["passenger_email_token"]
    assert token1 != token2
