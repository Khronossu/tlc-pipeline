"""Generates a deterministic fabricated PII lookup table keyed by (VendorID, tpep_pickup_datetime).

Raw PII lives only in this table (meta namespace). Every other layer sees tokens only.
Re-running for the same month overwrites the same partition — idempotent by design.
"""

from __future__ import annotations

import argparse
import hashlib

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from spark.jobs.config import Settings
from spark.jobs.landing_to_bronze import build_spark_session

LOOKUP_TABLE = "iceberg.meta.pii_lookup"

CREATE_LOOKUP_DDL = f"""
CREATE TABLE IF NOT EXISTS {LOOKUP_TABLE} (
    VendorID                INT,
    tpep_pickup_datetime    TIMESTAMP,
    passenger_email         STRING,
    passenger_phone         STRING,
    payment_card_last4      STRING,
    passenger_id            STRING
)
USING iceberg
PARTITIONED BY (months(tpep_pickup_datetime))
"""

# Deterministic fake-value generators keyed by a hash seed string.
# These produce plausible-looking but entirely synthetic values.


def _fake_email(seed: str) -> str:
    h = hashlib.md5(seed.encode()).hexdigest()  # noqa: S324
    return f"user_{h[:8]}@example.com"


def _fake_phone(seed: str) -> str:
    h = hashlib.md5(("phone" + seed).encode()).hexdigest()  # noqa: S324
    digits = "".join(c for c in h if c.isdigit())[:10].ljust(10, "0")
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def _fake_card_last4(seed: str) -> str:
    h = hashlib.md5(("card" + seed).encode()).hexdigest()  # noqa: S324
    return "".join(c for c in h if c.isdigit())[:4].ljust(4, "0")


def _fake_passenger_id(seed: str) -> str:
    return hashlib.md5(("pid" + seed).encode()).hexdigest()  # noqa: S324


def _make_seed_udf() -> pyspark.sql.functions.UserDefinedFunction:  # type: ignore[name-defined]  # noqa: F821
    """UDF: seed string from VendorID + pickup timestamp string."""

    def _seed(vendor_id: int | None, pickup: str | None) -> str:
        return f"{vendor_id}|{pickup}"

    return F.udf(_seed, StringType())


def _register_pii_udfs(
    spark: SparkSession,
) -> tuple[object, object, object, object]:
    email_udf = spark.udf.register("fake_email", _fake_email, StringType())
    phone_udf = spark.udf.register("fake_phone", _fake_phone, StringType())
    card_udf = spark.udf.register("fake_card_last4", _fake_card_last4, StringType())
    pid_udf = spark.udf.register("fake_passenger_id", _fake_passenger_id, StringType())
    return email_udf, phone_udf, card_udf, pid_udf


def generate_fake_pii(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Pure transformation: add 4 deterministic fake PII columns to a Bronze DataFrame.

    Key = VendorID + tpep_pickup_datetime cast to string. Same key → same PII every run.
    """
    email_udf, phone_udf, card_udf, pid_udf = _register_pii_udfs(spark)
    seed_udf = _make_seed_udf()

    return (
        df.select("VendorID", "tpep_pickup_datetime")
        .distinct()
        .withColumn(
            "_seed",
            seed_udf(F.col("VendorID"), F.col("tpep_pickup_datetime").cast(StringType())),
        )
        .withColumn("passenger_email", email_udf(F.col("_seed")))
        .withColumn("passenger_phone", phone_udf(F.col("_seed")))
        .withColumn("payment_card_last4", card_udf(F.col("_seed")))
        .withColumn("passenger_id", pid_udf(F.col("_seed")))
        .drop("_seed")
    )


def ensure_lookup_table(spark: SparkSession) -> None:
    spark.sql(CREATE_LOOKUP_DDL)


def run(year: int, month: int, settings: Settings) -> None:
    spark = build_spark_session(settings)
    ensure_lookup_table(spark)

    bronze_df = spark.sql(
        f"SELECT VendorID, tpep_pickup_datetime FROM iceberg.bronze.yellow_trips "
        f"WHERE year(tpep_pickup_datetime) = {year} AND month(tpep_pickup_datetime) = {month}"
    )
    pii_df = generate_fake_pii(bronze_df, spark)
    pii_df.writeTo(LOOKUP_TABLE).overwritePartitions()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    from spark.jobs.config import settings as default_settings

    run(args.year, args.month, default_settings)
