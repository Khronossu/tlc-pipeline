"""Monthly ingestion DAG: Landing → Bronze → PII Lookup → Tokenize → GE gate → Audit."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from ingest.tasks_audit import (
    alert_slack,
    make_quarantine_task,
    write_audit_quarantined,
    write_audit_success,
)
from ingest.tasks_ingest import make_download_task, make_landing_to_bronze_task
from ingest.tasks_pii import make_generate_pii_task, make_tokenize_task
from ingest.tasks_quality import make_branch_task, make_ge_gate_task

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": False,
}


@dag(
    dag_id="yellow_taxi_monthly_ingest",
    schedule="0 2 5 * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "year": Param(2023, type="integer", description="TLC data year"),
        "month": Param(1, type="integer", description="TLC data month (1-12)"),
        "force": Param(False, type="boolean", description="Overwrite even if already loaded"),
    },
    tags=["ingestion", "yellow_taxi"],
)
def yellow_taxi_monthly_ingest() -> None:
    year = "{{ params.year }}"
    month = "{{ params.month }}"
    run_id = "{{ run_id }}"
    source_url = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{year}-{month:>02}.parquet"
    )

    download = make_download_task(year, month)
    to_bronze = make_landing_to_bronze_task(year, month, run_id, source_url)
    gen_pii = make_generate_pii_task(year, month)
    tokenize = make_tokenize_task(year, month)
    ge_gate = make_ge_gate_task()
    branch = make_branch_task()
    quarantine = make_quarantine_task(year, month, run_id)

    # Bug #4 fix: gen_pii reads from Bronze, so Bronze must be written first
    download >> to_bronze >> gen_pii >> tokenize >> ge_gate >> branch
    branch >> write_audit_success()
    branch >> quarantine >> write_audit_quarantined() >> alert_slack()


yellow_taxi_monthly_ingest()
