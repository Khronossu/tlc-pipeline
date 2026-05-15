"""Weekly Iceberg maintenance DAG: compact files, expire snapshots, remove orphans.

Runs every Sunday at 03:00 UTC. Each step is isolated per table so a failure in one
table does not block others. Each step writes an audit row to ops.pipeline_audit.
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task

SPARK_JOBS_PATH = "/opt/spark/jobs"
MANAGED_TABLES = [
    "iceberg.bronze.yellow_trips",
    "iceberg.silver.stg_yellow_trips",
    "iceberg.gold.fct_trips",
    "iceberg.gold.fct_trips_daily",
    "iceberg.gold.fct_zone_revenue_monthly",
    "iceberg.ops.pipeline_audit",
]

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


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
        [
            "spark-submit",
            f"{SPARK_JOBS_PATH}/write_audit.py",
            "--record-json",
            json.dumps(record),
        ],
        check=False,
    )


@dag(
    dag_id="iceberg_maintenance",
    schedule="0 3 * * 0",  # every Sunday 03:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance", "iceberg"],
)
def iceberg_maintenance() -> None:
    @task
    def rewrite_data_files(**context: object) -> dict[str, object]:
        """Compact small files for all managed tables."""
        start = datetime.now(tz=UTC)
        results: dict[str, object] = {}
        for table in MANAGED_TABLES:
            try:
                proc = subprocess.run(
                    [
                        "spark-submit",
                        f"{SPARK_JOBS_PATH}/iceberg_maintenance.py",
                        "--tables",
                        table,
                    ],
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
                        "--conf",
                        f"spark.tlc.maintenance.table={table}",
                        f"{SPARK_JOBS_PATH}/iceberg_maintenance.py",
                        "--tables",
                        table,
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
                    [
                        "spark-submit",
                        f"{SPARK_JOBS_PATH}/iceberg_maintenance.py",
                        "--tables",
                        table,
                    ],
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

    rw = rewrite_data_files()
    exp = expire_snapshots()
    orp = remove_orphan_files()

    rw >> exp >> orp


iceberg_maintenance()
