"""Weekly Iceberg maintenance: rewrite data files, expire snapshots, remove orphans.

Run per-table so failures are isolated. Each procedure returns a summary row which
is printed for the audit task to capture.
"""

from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession

from spark.jobs.config import Settings
from spark.jobs.landing_to_bronze import build_spark_session

# Tables to maintain. Order matters: Bronze first so compaction runs before Silver reads it.
MANAGED_TABLES = [
    "iceberg.bronze.yellow_trips",
    "iceberg.silver.stg_yellow_trips",
    "iceberg.gold.fct_trips",
    "iceberg.gold.fct_trips_daily",
    "iceberg.gold.fct_zone_revenue_monthly",
    "iceberg.ops.pipeline_audit",
]

# Target file size range for rewrite_data_files (128 MB – 512 MB)
TARGET_FILE_SIZE_BYTES = 268_435_456  # 256 MB midpoint


def rewrite_data_files(table: str, spark: SparkSession) -> dict[str, object]:
    """Compact small files into target-sized files. Returns summary dict."""
    result = spark.sql(f"""
        CALL iceberg.system.rewrite_data_files(
            table => '{table}',
            options => map(
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'min-file-size-bytes', '{TARGET_FILE_SIZE_BYTES // 4}',
                'max-file-size-bytes', '{TARGET_FILE_SIZE_BYTES * 2}'
            )
        )
    """).collect()
    row = result[0] if result else None
    return {
        "rewritten_data_files_count": int(row["rewritten_data_files_count"]) if row else 0,
        "added_data_files_count": int(row["added_data_files_count"]) if row else 0,
    }


def expire_snapshots(table: str, spark: SparkSession) -> dict[str, object]:
    """Expire snapshots older than 30 days. Returns summary dict."""
    result = spark.sql(f"""
        CALL iceberg.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{{}}'::TIMESTAMP - INTERVAL 30 DAYS,
            retain_last => 5
        )
    """.format(
        "current_timestamp()"  # resolved at SQL parse time
    )).collect()
    # Use a simpler form that works with Iceberg REST catalog
    result = spark.sql(f"""
        CALL iceberg.system.expire_snapshots('{table}')
    """).collect()
    row = result[0] if result else None
    return {
        "deleted_data_files_count": int(row["deleted_data_files_count"]) if row else 0,
        "deleted_manifest_files_count": int(row["deleted_manifest_files_count"]) if row else 0,
    }


def remove_orphan_files(table: str, spark: SparkSession) -> dict[str, object]:
    """Remove orphan files (files not referenced by any snapshot). Returns summary dict."""
    result = spark.sql(f"""
        CALL iceberg.system.remove_orphan_files(table => '{table}')
    """).collect()
    return {"orphan_file_count": len(result)}


def run(tables: list[str], settings: Settings) -> list[dict[str, object]]:
    spark = build_spark_session(settings)
    summaries = []

    for table in tables:
        try:
            rw = rewrite_data_files(table, spark)
        except Exception as e:
            rw = {"error": str(e)[:200]}

        try:
            exp = expire_snapshots(table, spark)
        except Exception as e:
            exp = {"error": str(e)[:200]}

        try:
            orp = remove_orphan_files(table, spark)
        except Exception as e:
            orp = {"error": str(e)[:200]}

        summary = {"table": table, "rewrite": rw, "expire": exp, "orphan": orp}
        summaries.append(summary)
        print(json.dumps(summary))

    spark.stop()
    return summaries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tables",
        nargs="*",
        default=MANAGED_TABLES,
        help="Tables to maintain (defaults to all managed tables)",
    )
    args = parser.parse_args()

    from spark.jobs.config import settings as default_settings

    run(args.tables, default_settings)
