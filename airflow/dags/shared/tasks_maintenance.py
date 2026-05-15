"""Task factories for the Iceberg maintenance DAG."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime

from airflow.decorators import task

SPARK_JOBS_PATH = "/opt/spark/jobs"
MANAGED_TABLES = [
    "iceberg.bronze.yellow_trips",
    "iceberg.silver.stg_yellow_trips",
    "iceberg.gold.fct_trips",
    "iceberg.gold.fct_trips_daily",
    "iceberg.gold.fct_zone_revenue_monthly",
    "iceberg.ops.pipeline_audit",
]


def _write_audit(
    run_id: str,
    task_id: str,
    status: str,
    duration_sec: float,
    error_message: str = "",
) -> None:
    record = {
        "run_id": run_id,
        "dag_id": "iceberg_maintenance",
        "task_id": task_id,
        "layer": "ops",
        "status": status,
        "started_at": datetime.now(tz=UTC).isoformat(),
        "finished_at": datetime.now(tz=UTC).isoformat(),
        "duration_sec": duration_sec,
        "error_message": error_message[:500],
        "triggered_by": "scheduled",
    }
    subprocess.run(
        ["spark-submit", f"{SPARK_JOBS_PATH}/write_audit.py", "--record-json", json.dumps(record)],
        check=False,
    )


@task
def rewrite_data_files(**context: object) -> dict[str, object]:
    """Compact small files for all managed tables."""
    start = datetime.now(tz=UTC)
    results: dict[str, object] = {}
    for table in MANAGED_TABLES:
        try:
            proc = subprocess.run(
                ["spark-submit", f"{SPARK_JOBS_PATH}/iceberg_maintenance.py", "--tables", table],
                capture_output=True,
                text=True,
                check=True,
            )
            results[table] = "ok"
            print(proc.stdout)
        except subprocess.CalledProcessError as exc:
            results[table] = f"error: {exc.stderr[:200]}"

    duration = (datetime.now(tz=UTC) - start).total_seconds()
    _write_audit(
        run_id=str(context["run_id"]),
        task_id="rewrite_data_files",
        status="SUCCESS" if all(v == "ok" for v in results.values()) else "FAILED",
        duration_sec=duration,
    )
    return results


@task
def expire_snapshots(**context: object) -> None:
    """Expire snapshots older than 30 days for all managed tables."""
    start = datetime.now(tz=UTC)
    errors: list[str] = []
    for table in MANAGED_TABLES:
        try:
            subprocess.run(
                [
                    "spark-submit",
                    "--conf", f"spark.tlc.maintenance.table={table}",
                    f"{SPARK_JOBS_PATH}/iceberg_maintenance.py",
                    "--tables", table,
                ],
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            errors.append(f"{table}: {exc.stderr[:100]}")

    duration = (datetime.now(tz=UTC) - start).total_seconds()
    _write_audit(
        run_id=str(context["run_id"]),
        task_id="expire_snapshots",
        status="SUCCESS" if not errors else "FAILED",
        duration_sec=duration,
        error_message="; ".join(errors),
    )


@task
def remove_orphan_files(**context: object) -> None:
    """Remove files not referenced by any snapshot."""
    start = datetime.now(tz=UTC)
    errors: list[str] = []
    for table in MANAGED_TABLES:
        try:
            subprocess.run(
                ["spark-submit", f"{SPARK_JOBS_PATH}/iceberg_maintenance.py", "--tables", table],
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            errors.append(f"{table}: {exc.stderr[:100]}")

    duration = (datetime.now(tz=UTC) - start).total_seconds()
    _write_audit(
        run_id=str(context["run_id"]),
        task_id="remove_orphan_files",
        status="SUCCESS" if not errors else "FAILED",
        duration_sec=duration,
        error_message="; ".join(errors),
    )
