FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.8.0 \
    pyspark==3.5.5 \
    pydantic-settings>=2.3.0 \
    boto3>=1.34.0 \
    requests>=2.32.0 \
    pyarrow>=16.0.0 \
    prometheus-client>=0.20.0
RUN pip install --no-cache-dir --no-deps \
    dbt-core \
    "dbt-spark[session]" \
    agate \
    mashumaro \
    dbt-adapters \
    dbt-common \
    dbt-extractor \
    dbt-semantic-interfaces \
    networkx \
    deepdiff \
    orderly-set \
    sqlparams \
    parsedatetime \
    pytimeparse \
    snowplow-tracker \
    daff \
    leather
