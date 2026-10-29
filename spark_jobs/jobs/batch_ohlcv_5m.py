"""Batch: raw 1m candles from MinIO -> 5m OHLCV candles in ClickHouse pipeline.candles_5m.

    spark-submit spark_jobs/jobs/batch_ohlcv_5m.py --start 2026-10-27 --end 2026-10-28

Dates are UTC partition dates, both inclusive. Re-running a range rewrites the same rows
(ReplacingMergeTree on symbol, interval, open_time), so it is safe to repeat.
"""

import logging

from pyspark.sql import functions as F

from spark_jobs.common import clickhouse
from spark_jobs.common.config import Settings
from spark_jobs.common.raw_zone import parse_date_range, read_raw
from spark_jobs.common.session import build_spark
from spark_jobs.transforms import aggregate_ohlcv_5m, dedupe_candles, split_market

JOB = "batch_ohlcv_5m"
LOGGER = logging.getLogger(JOB)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start, end = parse_date_range(__doc__.splitlines()[0], argv)
    settings = Settings.from_env()
    spark = build_spark(JOB, settings)

    valid, _ = split_market(read_raw(spark, settings, "market", start, end))
    candles = aggregate_ohlcv_5m(dedupe_candles(valid)).withColumn("processed_at", F.current_timestamp())
    candles = candles.persist()
    clickhouse.write(candles, "candles_5m", settings)
    LOGGER.info("%s..%s: wrote %d 5m candles", start, end, candles.count())
    spark.stop()


if __name__ == "__main__":
    main()
