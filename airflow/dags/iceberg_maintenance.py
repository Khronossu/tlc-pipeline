"""Weekly Iceberg maintenance DAG: compact files, expire snapshots, remove orphans.

Runs every Sunday at 03:00 UTC. Each step is isolated per table so a failure in one
table does not block others.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from shared.tasks_maintenance import expire_snapshots, remove_orphan_files, rewrite_data_files

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


@dag(
    dag_id="iceberg_maintenance",
    schedule="0 3 * * 0",  # every Sunday 03:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance", "iceberg"],
)
def iceberg_maintenance() -> None:
    rw = rewrite_data_files()
    exp = expire_snapshots()
    orp = remove_orphan_files()

    rw >> exp >> orp


iceberg_maintenance()
