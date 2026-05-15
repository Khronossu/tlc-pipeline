"""Tests for schema-evolution helpers in landing_to_bronze.

align_schema() is tested against a local SparkSession (no Iceberg catalog needed)
by mocking spark.table() to return a DataFrame with a known schema.
cast_source_schema() is tested for both the 2018-era (no optional cols) and
2019+/2021+ schemas (optional cols present).
"""

from __future__ import annotations

from unittest.mock import MagicMock

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

from spark.jobs.landing_to_bronze import (
    _spark_type_to_ddl,
    align_schema,
    cast_source_schema,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.appName("test_schema_evolution")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


# ── _spark_type_to_ddl ───────────────────────────────────────────────────────


def test_ddl_integer() -> None:
    assert _spark_type_to_ddl(IntegerType()) == "INT"


def test_ddl_decimal() -> None:
    assert _spark_type_to_ddl(DecimalType(10, 2)) == "DECIMAL(10, 2)"


def test_ddl_string() -> None:
    assert _spark_type_to_ddl(StringType()) == "STRING"


def test_ddl_timestamp() -> None:
    assert _spark_type_to_ddl(TimestampType()) == "TIMESTAMP"


# ── cast_source_schema — 2018-era: no optional columns ──────────────────────

# Minimal schema with all mandatory Bronze columns (mirrors real TLC 2018 file).
_MANDATORY_FIELDS = [
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DecimalType(10, 2), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DecimalType(10, 2), True),
    StructField("extra", DecimalType(10, 2), True),
    StructField("mta_tax", DecimalType(10, 2), True),
    StructField("tip_amount", DecimalType(10, 2), True),
    StructField("tolls_amount", DecimalType(10, 2), True),
    StructField("improvement_surcharge", DecimalType(10, 2), True),
    StructField("total_amount", DecimalType(10, 2), True),
]
_MANDATORY_ROW = (
    1, None, None, None, None, None, None, None, None,
    None, None, None, None, None, None, None, None,
)


def test_cast_schema_without_optional_cols(spark: SparkSession) -> None:
    schema = StructType(_MANDATORY_FIELDS)
    df = spark.createDataFrame([_MANDATORY_ROW], schema=schema)
    result = cast_source_schema(df)
    assert "VendorID" in result.columns
    assert "congestion_surcharge" not in result.columns
    assert "airport_fee" not in result.columns


def test_cast_schema_with_congestion_surcharge(spark: SparkSession) -> None:
    extra = [StructField("congestion_surcharge", DecimalType(10, 2), True)]
    schema = StructType(_MANDATORY_FIELDS + extra)
    df = spark.createDataFrame([_MANDATORY_ROW + (None,)], schema=schema)
    result = cast_source_schema(df)
    assert "congestion_surcharge" in result.columns
    assert result.schema["congestion_surcharge"].dataType == DecimalType(10, 2)


def test_cast_schema_with_all_optional_cols(spark: SparkSession) -> None:
    schema = StructType(
        _MANDATORY_FIELDS
        + [
            StructField("congestion_surcharge", DecimalType(10, 2), True),
            StructField("airport_fee", DecimalType(10, 2), True),
        ]
    )
    df = spark.createDataFrame([_MANDATORY_ROW + (None, None)], schema=schema)
    result = cast_source_schema(df)
    assert "airport_fee" in result.columns


# ── align_schema — column backfill (table has more cols than df) ─────────────


def test_align_schema_fills_missing_table_cols(spark: SparkSession) -> None:
    """Columns present in the table but absent from df are filled with NULL."""
    table_schema = StructType(
        [
            StructField("VendorID", IntegerType(), True),
            StructField("congestion_surcharge", DecimalType(10, 2), True),
        ]
    )
    mock_spark = MagicMock()
    mock_spark.table.return_value = spark.createDataFrame([], schema=table_schema)
    # df only has VendorID — congestion_surcharge is missing (2018 data)
    df_schema = StructType([StructField("VendorID", IntegerType(), True)])
    df = spark.createDataFrame([(1,)], schema=df_schema)

    result = align_schema(df, "iceberg.bronze.yellow_trips", mock_spark)

    assert "congestion_surcharge" in result.columns
    row = result.collect()[0]
    assert row["congestion_surcharge"] is None


def test_align_schema_issues_alter_for_new_col(spark: SparkSession) -> None:
    """Columns in df but not in table trigger ALTER TABLE ADD COLUMN."""
    table_schema = StructType([StructField("VendorID", IntegerType(), True)])
    mock_spark = MagicMock()
    mock_spark.table.return_value = spark.createDataFrame([], schema=table_schema)

    df_schema = StructType(
        [
            StructField("VendorID", IntegerType(), True),
            StructField("new_col", StringType(), True),  # new column arriving in source
        ]
    )
    df = spark.createDataFrame([(1, "x")], schema=df_schema)
    align_schema(df, "iceberg.bronze.yellow_trips", mock_spark)

    executed_sql = [call[0][0] for call in mock_spark.sql.call_args_list]
    assert any("ALTER TABLE" in s and "new_col" in s for s in executed_sql)


def test_align_schema_no_alter_when_col_exists(spark: SparkSession) -> None:
    """No ALTER TABLE is issued for columns already present in the table."""
    table_schema = StructType(
        [
            StructField("VendorID", IntegerType(), True),
            StructField("congestion_surcharge", DecimalType(10, 2), True),
        ]
    )
    mock_spark = MagicMock()
    mock_spark.table.return_value = spark.createDataFrame([], schema=table_schema)

    df_schema = StructType(
        [
            StructField("VendorID", IntegerType(), True),
            StructField("congestion_surcharge", DecimalType(10, 2), True),
        ]
    )
    df = spark.createDataFrame([(1, None)], schema=df_schema)
    align_schema(df, "iceberg.bronze.yellow_trips", mock_spark)

    executed_sql = [call[0][0] for call in mock_spark.sql.call_args_list]
    assert not any("ALTER TABLE" in s for s in executed_sql)


def test_align_schema_returns_df_when_table_missing(spark: SparkSession) -> None:
    """If the table does not exist yet, df is returned unchanged."""
    mock_spark = MagicMock()
    mock_spark.table.side_effect = Exception("Table not found")

    df_schema = StructType([StructField("VendorID", IntegerType(), True)])
    df = spark.createDataFrame([(1,)], schema=df_schema)

    result = align_schema(df, "iceberg.bronze.yellow_trips", mock_spark)
    assert result is df


# ── Idempotency: same input → same output ────────────────────────────────────


def test_cast_schema_idempotent(spark: SparkSession) -> None:
    """Running cast_source_schema twice on the same df produces the same schema."""
    schema = StructType(
        _MANDATORY_FIELDS + [StructField("congestion_surcharge", DecimalType(10, 2), True)]
    )
    df = spark.createDataFrame([_MANDATORY_ROW + (None,)], schema=schema)
    r1 = cast_source_schema(df)
    r2 = cast_source_schema(df)
    assert r1.schema == r2.schema
    assert r1.collect() == r2.collect()


def test_align_schema_idempotent_no_new_cols(spark: SparkSession) -> None:
    """align_schema called twice with stable schemas issues no ALTER TABLEs either time."""
    table_schema = StructType([StructField("VendorID", IntegerType(), True)])
    mock_spark = MagicMock()
    mock_spark.table.return_value = spark.createDataFrame([], schema=table_schema)

    df = spark.createDataFrame([(1,)], schema=table_schema)

    align_schema(df, "iceberg.bronze.yellow_trips", mock_spark)
    align_schema(df, "iceberg.bronze.yellow_trips", mock_spark)

    alter_calls = [
        c for c in mock_spark.sql.call_args_list if "ALTER TABLE" in c[0][0]
    ]
    assert len(alter_calls) == 0
