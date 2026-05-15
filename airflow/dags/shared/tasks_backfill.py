"""Task factories for the backfill DAG."""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime

from airflow.decorators import task

DBT_DIR = "/opt/airflow/dbt"
SPARK_JOBS_PATH = "/opt/spark/jobs"


def _iter_months(start: str, end: str) -> Generator[tuple[int, int], None, None]:
    cur = datetime.strptime(start, "%Y-%m")
    stop = datetime.strptime(end, "%Y-%m")
    while cur <= stop:
        yield cur.year, cur.month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)


def _spark_submit(script: str, *args: str) -> str:
    joined = " ".join(f'"{a}"' for a in args)
    return f"spark-submit {SPARK_JOBS_PATH}/{script} {joined}"


@task
def run_backfill(start_month: str, end_month: str, force: bool) -> dict[str, int]:
    """Sequential month-by-month backfill. Runs Spark jobs via subprocess."""
    import subprocess

    results: dict[str, int] = {}

    for year, month in _iter_months(start_month, end_month):
        label = f"{year}-{month:02d}"
        source_url = (
            "https://d37ci6vzurychx.cloudfront.net/trip-data/"
            f"yellow_tripdata_{year}-{month:02d}.parquet"
        )

        steps = [
            _spark_submit("download_to_landing.py", "--year", str(year), "--month", str(month)),
            _spark_submit(
                "landing_to_bronze.py",
                "--year", str(year), "--month", str(month),
                "--run-id", f"backfill-{label}",
                "--source-url", source_url,
                "--source-sha256", "",
            ),
            _spark_submit("generate_pii_lookup.py", "--year", str(year), "--month", str(month)),
            _spark_submit("tokenize_pii.py", "--year", str(year), "--month", str(month)),
            (
                f"cd {DBT_DIR} && dbt run --select silver "
                f"--vars '{{year: {year}, month: {month}}}' --profiles-dir ."
            ),
            (
                f"cd {DBT_DIR} && dbt run --select gold serving "
                f"--vars '{{year: {year}, month: {month}}}' --profiles-dir ."
            ),
        ]

        for cmd in steps:
            subprocess.run(cmd, shell=True, check=True)  # noqa: S603 S602

        results[label] = 1

    return results


@task
def summarise(results: dict[str, int]) -> None:
    months_done = sorted(results.keys())
    n = len(months_done)
    print(f"Backfill complete: {n} months — {months_done[0]} to {months_done[-1]}")
