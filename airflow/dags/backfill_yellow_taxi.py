"""Backfill DAG: runs the full monthly ingest pipeline sequentially across a month range.

Parameterized by start_month / end_month (YYYY-MM strings). Each month is processed
one at a time (not fanned out) to keep memory bounded — a single Spark job per step.
Reuses the same task-factory functions as the monthly ingest DAG.
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

DBT_DIR = "/opt/airflow/dbt"
GE_PATH = "/opt/airflow/great_expectations"
SPARK_JOBS_PATH = "/opt/spark/jobs"


def _iter_months(start: str, end: str) -> Generator[tuple[int, int], None, None]:
    """Yield (year, month) tuples from start to end inclusive (both 'YYYY-MM')."""
    cur = datetime.strptime(start, "%Y-%m")
    stop = datetime.strptime(end, "%Y-%m")
    while cur <= stop:
        yield cur.year, cur.month
        # advance to next month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)


def _spark_submit(script: str, *args: str) -> str:
    joined = " ".join(f'"{a}"' for a in args)
    return f"spark-submit {SPARK_JOBS_PATH}/{script} {joined}"


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
    @task
    def run_backfill(start_month: str, end_month: str, force: bool) -> dict[str, int]:
        """Sequential month-by-month backfill. Runs Spark jobs via subprocess."""
        import subprocess

        results: dict[str, int] = {}

        for year, month in _iter_months(start_month, end_month):
            label = f"{year}-{month:02d}"
            source_url = (
                f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"yellow_tripdata_{year}-{month:02d}.parquet"
            )

            steps = [
                # 1. Download to Landing
                _spark_submit("download_to_landing.py", "--year", str(year), "--month", str(month)),
                # 2. Bronze write (schema-evolution aware)
                _spark_submit(
                    "landing_to_bronze.py",
                    "--year", str(year), "--month", str(month),
                    "--run-id", f"backfill-{label}",
                    "--source-url", source_url,
                    "--source-sha256", "",
                ),
                # 3. Generate fabricated PII lookup
                _spark_submit("generate_pii_lookup.py", "--year", str(year), "--month", str(month)),
                # 4. Tokenize PII
                _spark_submit("tokenize_pii.py", "--year", str(year), "--month", str(month)),
                # 5. dbt Silver
                (
                    f"cd {DBT_DIR} && dbt run --select silver "
                    f"--vars '{{year: {year}, month: {month}}}' --profiles-dir ."
                ),
                # 6. dbt Gold
                (
                    f"cd {DBT_DIR} && dbt run --select gold serving "
                    f"--vars '{{year: {year}, month: {month}}}' --profiles-dir ."
                ),
            ]

            for cmd in steps:
                subprocess.run(cmd, shell=True, check=True)  # noqa: S603 S602

            results[label] = 1  # success marker per month

        return results

    @task
    def summarise(results: dict[str, int]) -> None:
        months_done = sorted(results.keys())
        n = len(months_done)
        print(f"Backfill complete: {n} months — {months_done[0]} to {months_done[-1]}")

    results = run_backfill(
        start_month="{{ params.start_month }}",
        end_month="{{ params.end_month }}",
        force="{{ params.force }}",
    )
    summarise(results)


backfill_yellow_taxi()
