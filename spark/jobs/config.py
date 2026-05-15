from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    minio_endpoint: str = "http://minio:9000"
    minio_root_user: str = "minioadmin"
    minio_root_password: str = "minioadmin"
    iceberg_rest_url: str = "http://iceberg-rest:8181"
    iceberg_warehouse: str = "s3a://warehouse/"
    spark_master: str = "spark://spark-master:7077"
    pii_salt: str = "change-me"
    pii_salt_version: int = 1

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
