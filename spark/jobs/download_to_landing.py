"""Downloads a TLC monthly parquet to the Landing zone and writes a manifest."""

from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import boto3
import requests

from spark.jobs.config import Settings


TLC_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
)


@dataclass
class ManifestRecord:
    url: str
    sha256: str
    byte_size: int
    row_count: int
    ingested_at: str  # ISO-8601 UTC


def build_tlc_url(year: int, month: int) -> str:
    return TLC_URL_TEMPLATE.format(year=year, month=month)


def build_landing_prefix(year: int, month: int) -> str:
    """S3 key prefix (no trailing slash) for one month's landing files."""
    return f"yellow_taxi/year={year}/month={month:02d}"


def _s3_client(settings: Settings) -> boto3.client:  # type: ignore[type-arg]
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
    )


def _count_parquet_rows(path: Path) -> int:
    import pyarrow.parquet as pq

    return pq.read_metadata(str(path)).num_rows


def download_month(year: int, month: int, settings: Settings) -> ManifestRecord:
    """Stream TLC parquet to MinIO landing bucket and write a manifest.

    Idempotent: re-running overwrites the same S3 key.
    """
    url = build_tlc_url(year, month)
    prefix = build_landing_prefix(year, month)
    parquet_key = f"{prefix}/data.parquet"
    manifest_key = f"{prefix}/_manifest.json"

    s3 = _s3_client(settings)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "data.parquet"

        # Stream download
        sha256 = hashlib.sha256()
        byte_size = 0
        with requests.get(url, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            with local_path.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    fh.write(chunk)
                    sha256.update(chunk)
                    byte_size += len(chunk)

        row_count = _count_parquet_rows(local_path)
        ingested_at = datetime.now(tz=UTC).isoformat()

        # Upload parquet
        s3.upload_file(str(local_path), "landing", parquet_key)

        # Write manifest atomically via put_object (MinIO is strongly consistent)
        record = ManifestRecord(
            url=url,
            sha256=sha256.hexdigest(),
            byte_size=byte_size,
            row_count=row_count,
            ingested_at=ingested_at,
        )
        s3.put_object(
            Bucket="landing",
            Key=manifest_key,
            Body=json.dumps(asdict(record), indent=2).encode(),
            ContentType="application/json",
        )

    return record


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    from spark.jobs.config import settings as default_settings

    result = download_month(args.year, args.month, default_settings)
    print(json.dumps(asdict(result), indent=2))
