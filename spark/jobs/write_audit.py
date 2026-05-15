"""Writes a single audit row to ops.pipeline_audit (Iceberg) and pushes metrics to Pushgateway."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from spark.jobs.config import Settings

AUDIT_TABLE = "iceberg.ops.pipeline_audit"

CREATE_AUDIT_DDL = f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    run_id                  STRING,
    dag_id                  STRING,
    task_id                 STRING,
    layer                   STRING,
    table_name              STRING,
    source_url              STRING,
    source_sha256           STRING,
    schema_version          INT,
    rows_in                 BIGINT,
    rows_out                BIGINT,
    rows_quarantined        BIGINT,
    ge_suite_name           STRING,
    ge_pass_rate            DOUBLE,
    ge_failed_expectations  ARRAY<STRING>,
    started_at              TIMESTAMP,
    finished_at             TIMESTAMP,
    duration_sec            DOUBLE,
    status                  STRING,
    error_message           STRING,
    triggered_by            STRING
)
USING iceberg
PARTITIONED BY (days(started_at))
"""


@dataclass
class AuditRecord:
    run_id: str
    dag_id: str
    task_id: str
    layer: str
    status: str  # 'SUCCESS' | 'FAILED' | 'QUARANTINED'
    started_at: str  # ISO-8601 UTC string
    finished_at: str  # ISO-8601 UTC string
    duration_sec: float
    table_name: str = ""
    source_url: str = ""
    source_sha256: str = ""
    schema_version: int = 1
    rows_in: int = 0
    rows_out: int = 0
    rows_quarantined: int = 0
    ge_suite_name: str = ""
    ge_pass_rate: float = 1.0
    ge_failed_expectations: list[str] = field(default_factory=list)
    error_message: str = ""
    triggered_by: str = "scheduled"


def ensure_audit_table(spark: SparkSession) -> None:
    spark.sql(CREATE_AUDIT_DDL)


def write_audit_row(record: AuditRecord, spark: SparkSession) -> None:
    """Insert one audit row. Append-only — never overwrites."""
    failed_exp_literal = (
        "array(" + ", ".join(f"'{e}'" for e in record.ge_failed_expectations) + ")"
        if record.ge_failed_expectations
        else "cast(array() as array<string>)"
    )

    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE} VALUES (
            '{record.run_id}',
            '{record.dag_id}',
            '{record.task_id}',
            '{record.layer}',
            '{record.table_name}',
            '{record.source_url}',
            '{record.source_sha256}',
            {record.schema_version},
            {record.rows_in},
            {record.rows_out},
            {record.rows_quarantined},
            '{record.ge_suite_name}',
            {record.ge_pass_rate},
            {failed_exp_literal},
            CAST('{record.started_at}' AS TIMESTAMP),
            CAST('{record.finished_at}' AS TIMESTAMP),
            {record.duration_sec},
            '{record.status}',
            '{record.error_message.replace("'", "''")}',
            '{record.triggered_by}'
        )
    """)


def push_metrics(record: AuditRecord) -> None:
    """Push pipeline run metrics to Prometheus Pushgateway (best-effort; never raises)."""
    pushgateway_url = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        labels = {"dag_id": record.dag_id, "task_id": record.task_id, "layer": record.layer}

        Gauge("pipeline_run_duration_seconds", "Pipeline task duration", labels.keys(),
              registry=registry).labels(**labels).set(record.duration_sec)
        Gauge("pipeline_rows_written", "Rows written in this task run", labels.keys(),
              registry=registry).labels(**labels).set(record.rows_out)
        Gauge("pipeline_rows_quarantined", "Rows quarantined in this task run", labels.keys(),
              registry=registry).labels(**labels).set(record.rows_quarantined)
        Gauge("pipeline_ge_pass_rate", "GE pass rate (0–1)", labels.keys(),
              registry=registry).labels(**labels).set(record.ge_pass_rate)
        Gauge("pipeline_run_status", "1=SUCCESS 0=FAILED -1=QUARANTINED", labels.keys(),
              registry=registry).labels(**labels).set(
            1 if record.status == "SUCCESS" else (-1 if record.status == "QUARANTINED" else 0)
        )
        # Epoch timestamp of last successful finish per table — used for freshness panels
        if record.table_name:
            from datetime import UTC, datetime
            ts = datetime.now(tz=UTC).timestamp()
            Gauge("pipeline_last_finished_ts", "Unix timestamp of last finish", ["table_name"],
                  registry=registry).labels(table_name=record.table_name).set(ts)

        push_to_gateway(pushgateway_url, job=record.dag_id, registry=registry)
    except Exception:  # noqa: BLE001
        pass  # metrics are best-effort — never block the audit write


def run(record_json: str, settings: Settings) -> None:
    from spark.jobs.landing_to_bronze import build_spark_session

    spark = build_spark_session(settings)
    ensure_audit_table(spark)
    data = json.loads(record_json)
    record = AuditRecord(**data)
    write_audit_row(record, spark)
    push_metrics(record)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--record-json", required=True, help="JSON-serialised AuditRecord")
    args = parser.parse_args()

    from spark.jobs.config import settings as default_settings

    run(args.record_json, default_settings)
