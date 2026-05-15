"""Joins fabricated PII lookup into Bronze, tokenizes with SHA-256+salt, overwrites partition.

Raw PII is never written to Bronze. This job reads meta.pii_lookup (clear text),
applies salted SHA-256, and writes only the token columns into bronze.yellow_trips.
"""

from __future__ import annotations

import argparse
import hashlib

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from spark.jobs.config import Settings
from spark.jobs.landing_to_bronze import build_spark_session

BRONZE_TABLE = "iceberg.bronze.yellow_trips"
LOOKUP_TABLE = "iceberg.meta.pii_lookup"

PII_COLUMNS = ["passenger_email", "passenger_phone", "payment_card_last4", "passenger_id"]
TOKEN_COLUMNS = [f"{c}_token" for c in PII_COLUMNS]


def tokenize(value: str | None, salt: str) -> str | None:
    """Salted SHA-256 tokenization. Pure function — no side effects.

    Returns 64-char lowercase hex string, or None if value is None.
    Deterministic: same value + salt always produces the same token.
    """
    if value is None:
        return None
    return hashlib.sha256((value + salt).encode()).hexdigest()


def _make_tokenize_udf(salt: str) -> object:
    # Inline hashlib import so the UDF closure is self-contained on Spark executors.
    # Referencing module-level imports would pull in pydantic_settings, which is not
    # available in the executor Python environment.
    def _tokenize(value: str | None) -> str | None:
        import hashlib

        if value is None:
            return None
        return hashlib.sha256((value + salt).encode()).hexdigest()

    return F.udf(_tokenize, StringType())


def tokenize_bronze_partition(
    bronze_df: DataFrame,
    lookup_df: DataFrame,
    salt: str,
    salt_version: int,
) -> DataFrame:
    """Pure function: join PII lookup, apply tokenization, return Bronze df with token columns.

    Adds 4 *_token columns + _salt_version. Raw PII columns are never written.
    """
    tok_udf = _make_tokenize_udf(salt)

    pii_with_tokens = lookup_df
    for raw_col, token_col in zip(PII_COLUMNS, TOKEN_COLUMNS):
        pii_with_tokens = pii_with_tokens.withColumn(token_col, tok_udf(F.col(raw_col))).drop(
            raw_col
        )

    joined = bronze_df.join(
        pii_with_tokens,
        on=["VendorID", "tpep_pickup_datetime"],
        how="left",
    )

    for token_col in TOKEN_COLUMNS:
        if token_col not in joined.columns:
            joined = joined.withColumn(token_col, F.lit(None).cast(StringType()))

    return joined.withColumn("_salt_version", F.lit(salt_version))


def run(year: int, month: int, settings: Settings) -> int:
    spark = build_spark_session(settings)

    bronze_df = spark.sql(
        f"SELECT * FROM {BRONZE_TABLE} "
        f"WHERE year(tpep_pickup_datetime) = {year} AND month(tpep_pickup_datetime) = {month}"
    )
    lookup_df = spark.sql(
        f"SELECT * FROM {LOOKUP_TABLE} "
        f"WHERE year(tpep_pickup_datetime) = {year} AND month(tpep_pickup_datetime) = {month}"
    )

    result_df = tokenize_bronze_partition(
        bronze_df, lookup_df, settings.pii_salt, settings.pii_salt_version
    )
    result_df.writeTo(BRONZE_TABLE).overwritePartitions()

    count = spark.sql(
        f"SELECT COUNT(*) FROM {BRONZE_TABLE} "
        f"WHERE year(tpep_pickup_datetime) = {year} AND month(tpep_pickup_datetime) = {month}"
    ).collect()[0][0]
    spark.stop()
    return int(count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    from spark.jobs.config import settings as default_settings

    rows = run(args.year, args.month, default_settings)
    print(f"Tokenized {rows:,} rows in {BRONZE_TABLE}")
