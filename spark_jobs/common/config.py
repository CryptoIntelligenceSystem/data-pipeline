"""Job configuration, read only from environment variables (see .env.example)."""

import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    """A required environment variable is missing."""


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ConfigError(f"environment variable {name} is required")
    return value


def job_env(job: str, name: str, default: str) -> str:
    """Per-job override first (RAW_TO_MINIO_STARTING_OFFSETS), then the shared SPARK_* value."""
    return os.getenv(f"{job.upper()}_{name}") or os.getenv(f"SPARK_{name}") or default


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    market_topic: str
    news_topic: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    raw_bucket: str
    checkpoint_bucket: str
    clickhouse_jdbc_url: str
    clickhouse_jdbc_driver: str
    clickhouse_user: str
    clickhouse_password: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            market_topic=os.getenv("KAFKA_MARKET_TOPIC", "market_data"),
            news_topic=os.getenv("KAFKA_NEWS_TOPIC", "news_data"),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            minio_access_key=_required("MINIO_ACCESS_KEY"),
            minio_secret_key=_required("MINIO_SECRET_KEY"),
            raw_bucket=os.getenv("MINIO_RAW_BUCKET", "raw-data"),
            checkpoint_bucket=os.getenv("MINIO_CHECKPOINT_BUCKET", "checkpoints"),
            clickhouse_jdbc_url=_required("CLICKHOUSE_JDBC_URL"),
            clickhouse_jdbc_driver=os.getenv("CLICKHOUSE_JDBC_DRIVER", "com.clickhouse.jdbc.ClickHouseDriver"),
            clickhouse_user=_required("CLICKHOUSE_USER"),
            clickhouse_password=_required("CLICKHOUSE_PASSWORD"),
        )

    def raw_path(self, dataset: str) -> str:
        return f"s3a://{self.raw_bucket}/{dataset}"

    def checkpoint_path(self, job: str) -> str:
        return f"s3a://{self.checkpoint_bucket}/{job}"
