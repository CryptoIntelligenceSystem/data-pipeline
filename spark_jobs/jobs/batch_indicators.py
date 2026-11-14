"""Batch: raw 1m and processed 5m candles -> MA20 in ClickHouse pipeline.indicators.

    spark-submit spark_jobs/jobs/batch_indicators.py --start 2026-10-27 --end 2026-10-28

The day before --start is read as well so the first 1m candles of the range have their 19
predecessors. The 5m source is read from pipeline.candles_5m with 19 preceding 5m candles.
Only candles inside [start, end] are written. MA20 is NULL while fewer than 20 candles exist
(start of the data). Re-running is safe (ReplacingMergeTree).
"""

import logging
from datetime import date, datetime, time, timedelta, timezone

from pyspark.sql import functions as F

from spark_jobs.common import clickhouse
from spark_jobs.common.config import Settings
from spark_jobs.common.raw_zone import parse_date_range, read_raw
from spark_jobs.common.session import build_spark
from spark_jobs.transforms import dedupe_candles, moving_average, split_market

JOB = "batch_indicators"
LOGGER = logging.getLogger(JOB)
PERIODS = 20


def _utc_millis(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def _read_five_minute_candles(spark, settings: Settings, start: date, end: date):
    """Read enough 5m history from ClickHouse to calculate a complete MA20 window."""
    range_start = datetime.combine(start, time.min, tzinfo=timezone.utc) - timedelta(minutes=5 * (PERIODS - 1))
    range_end = datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc)
    query = f"""
        SELECT
          toString(symbol) AS symbol,
          toString(open_time) AS open_time,
          toFloat64(close) AS close
        FROM pipeline.candles_5m FINAL
        WHERE open_time >= fromUnixTimestamp64Milli({_utc_millis(range_start)}, 'UTC')
          AND open_time < fromUnixTimestamp64Milli({_utc_millis(range_end)}, 'UTC')
    """
    return (
        clickhouse.read_query(spark, settings, query)
        .select(
            "symbol",
            F.lit("5m").alias("interval"),
            F.to_timestamp("open_time", "yyyy-MM-dd HH:mm:ss.SSS").alias("open_time"),
            "close",
        )
        .dropDuplicates(["symbol", "interval", "open_time"])
    )


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start, end = parse_date_range(__doc__.splitlines()[0], argv)
    settings = Settings.from_env()
    spark = build_spark(JOB, settings)

    valid, _ = split_market(read_raw(spark, settings, "market", start - timedelta(days=1), end))
    in_range = F.to_date("open_time").between(F.lit(start), F.lit(end))
    indicators_1m = (
        moving_average(dedupe_candles(valid), PERIODS)
        .where(in_range)
    )
    candles_5m = _read_five_minute_candles(spark, settings, start, end)
    indicators_5m = moving_average(candles_5m, PERIODS).where(in_range)
    indicators = indicators_1m.unionByName(indicators_5m).withColumn(
        "processed_at", F.current_timestamp()
    ).persist()
    clickhouse.write(indicators, "indicators", settings)
    LOGGER.info("%s..%s: wrote %d ma%d rows for 1m and 5m", start, end, indicators.count(), PERIODS)
    spark.stop()


if __name__ == "__main__":
    main()
