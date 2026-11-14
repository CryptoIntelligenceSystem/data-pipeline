"""Reading the raw zone written by raw_to_minio (s3a://raw-data/<dataset>/year=/month=/day=)."""

import argparse
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import contracts
from .config import Settings

PARTITION_FIELDS = [T.StructField(name, T.IntegerType(), True) for name in ("year", "month", "day")]


def raw_record_schema() -> T.StructType:
    """raw_record contract fields plus the Hive partition columns."""
    return T.StructType(contracts.spark_schema("raw_record").fields + PARTITION_FIELDS)


def day_key(column_year="year", column_month="month", column_day="day"):
    return F.col(column_year) * 10000 + F.col(column_month) * 100 + F.col(column_day)


def read_raw(spark: SparkSession, settings: Settings, dataset: str, start: date, end: date) -> DataFrame:
    """Raw records whose partition date is in [start, end] (inclusive). The filter only touches
    partition columns, so Spark prunes whole directories instead of scanning all history."""
    first, last = (int(d.strftime("%Y%m%d")) for d in (start, end))
    return (
        spark.read.schema(raw_record_schema())
        .json(settings.raw_path(dataset))
        .where(day_key().between(first, last))
    )


def parse_date_range(description: str, argv: list[str] | None = None) -> tuple[date, date]:
    """--start/--end as YYYY-MM-DD, both inclusive. Default: yesterday only (UTC dates)."""
    parser = argparse.ArgumentParser(description=description)
    # Contract timestamps and MinIO partitions are UTC; do not let the host timezone
    # change the default range around midnight.
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    parser.add_argument("--start", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    args = parser.parse_args(argv)
    end = args.end or args.start
    if end < args.start:
        parser.error("--end must not be before --start")
    return args.start, end
