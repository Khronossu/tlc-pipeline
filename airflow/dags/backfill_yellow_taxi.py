"""Backfill DAG: runs the full monthly ingest pipeline sequentially across a month range.

Parameterized by start_month / end_month (YYYY-MM strings). Each month is processed
one at a time to keep memory bounded — a single Spark job per step.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from shared.tasks_backfill import run_backfill, summarise

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="backfill_yellow_taxi",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "start_month": Param("2018-01", type="string", description="First month (YYYY-MM)"),
        "end_month": Param("2018-01", type="string", description="Last month (YYYY-MM)"),
        "force": Param(False, type="boolean", description="Overwrite already-loaded months"),
    },
    tags=["backfill", "yellow_taxi"],
)
def backfill_yellow_taxi() -> None:
    results = run_backfill(
        start_month="{{ params.start_month }}",
        end_month="{{ params.end_month }}",
        force="{{ params.force }}",
    )
    summarise(results)


backfill_yellow_taxi()
