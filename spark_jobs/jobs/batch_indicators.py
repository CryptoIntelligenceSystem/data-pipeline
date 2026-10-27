"""Batch: raw 1m candles from MinIO -> MA20 of close in ClickHouse pipeline.indicators.

    spark-submit spark_jobs/jobs/batch_indicators.py --start 2026-10-27 --end 2026-10-28

The day before --start is read as well so the first candles of the range have their 19
predecessors; only candles inside [start, end] are written. MA20 is NULL while fewer than 20
candles exist (start of the data). Re-running is safe (ReplacingMergeTree).
"""

import logging
from datetime import timedelta

from pyspark.sql import functions as F

from spark_jobs.common import clickhouse
from spark_jobs.common.config import Settings
from spark_jobs.common.raw_zone import parse_date_range, read_raw
from spark_jobs.common.session import build_spark
from spark_jobs.transforms import dedupe_candles, moving_average, split_market

JOB = "batch_indicators"
LOGGER = logging.getLogger(JOB)
PERIODS = 20


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start, end = parse_date_range(__doc__.splitlines()[0], argv)
    settings = Settings.from_env()
    spark = build_spark(JOB, settings)

    valid, _ = split_market(read_raw(spark, settings, "market", start - timedelta(days=1), end))
    in_range = F.to_date("open_time").between(F.lit(start), F.lit(end))
    indicators = (
        moving_average(dedupe_candles(valid), PERIODS)
        .where(in_range)
        .withColumn("processed_at", F.current_timestamp())
        .persist()
    )
    clickhouse.write(indicators, "indicators", settings)
    LOGGER.info("%s..%s: wrote %d ma%d rows", start, end, indicators.count(), PERIODS)
    spark.stop()


if __name__ == "__main__":
    main()
