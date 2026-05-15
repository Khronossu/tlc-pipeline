from spark.jobs.download_to_landing import build_landing_prefix, build_tlc_url


def test_build_tlc_url_format() -> None:
    url = build_tlc_url(2023, 1)
    assert url == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    )


def test_build_tlc_url_zero_pads_month() -> None:
    assert build_tlc_url(2019, 3).endswith("2019-03.parquet")


def test_build_landing_prefix_format() -> None:
    prefix = build_landing_prefix(2023, 1)
    assert prefix == "yellow_taxi/year=2023/month=01"


def test_build_landing_prefix_zero_pads_month() -> None:
    assert build_landing_prefix(2021, 9).endswith("month=09")
