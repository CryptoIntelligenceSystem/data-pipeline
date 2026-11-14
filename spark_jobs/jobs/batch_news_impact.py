"""Batch: measure the 5/15/60 minute price change after each published news article.

    spark-submit spark_jobs/jobs/batch_news_impact.py --start 2026-10-27 --end 2026-10-28

Reads validated news and closed 1m candles from ClickHouse. Only articles published
on the requested UTC dates are written. Results are temporal associations, not
evidence that the news caused the price movement. Reruns are safe with FINAL.
"""

import logging
from datetime import datetime, time, timedelta, timezone

from pyspark.sql import functions as F

from spark_jobs.common import clickhouse
from spark_jobs.common.config import Settings
from spark_jobs.common.raw_zone import parse_date_range
from spark_jobs.common.session import build_spark
from spark_jobs.transforms import calculate_news_impact

JOB = "batch_news_impact"
LOGGER = logging.getLogger(JOB)


def _millis(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start, end = parse_date_range(__doc__.splitlines()[0], argv)
    settings = Settings.from_env()
    spark = build_spark(JOB, settings)
    try:
        range_start = datetime.combine(start, time.min, tzinfo=timezone.utc)
        range_end = datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc)
        start_ms, end_ms = _millis(range_start), _millis(range_end)
        articles_query = f"""
            SELECT toString(source) AS source, article_id, title,
                   arrayJoin(symbols) AS symbol,
                   toUnixTimestamp64Milli(published_at) AS published_ms
            FROM pipeline.news_data FINAL
            WHERE published_at >= fromUnixTimestamp64Milli({start_ms}, 'UTC')
              AND published_at < fromUnixTimestamp64Milli({end_ms}, 'UTC')
        """
        articles = clickhouse.read_query(spark, settings, articles_query).select(
            "source", "article_id", "title", "symbol", F.col("published_ms").cast("long")
        )
        if not articles.take(1):
            LOGGER.info("%s..%s: no articles with tagged symbols; no impact rows", start, end)
            return

        # Include one preceding candle and 61 minutes after the last article day.
        price_start_ms = start_ms - 60000
        price_end_ms = end_ms + 61 * 60000
        prices_query = f"""
            SELECT toString(symbol) AS symbol,
                   toUnixTimestamp64Milli(close_time) AS close_ms,
                   toFloat64(close) AS close
            FROM pipeline.market_data FINAL
            WHERE interval = '1m'
              AND close_time >= fromUnixTimestamp64Milli({price_start_ms}, 'UTC')
              AND close_time < fromUnixTimestamp64Milli({price_end_ms}, 'UTC')
        """
        prices = clickhouse.read_query(spark, settings, prices_query).select(
            "symbol", F.col("close_ms").cast("long"), "close"
        )
        results = calculate_news_impact(articles, prices).persist()
        count = results.count()
        if count:
            clickhouse.write(results, "news_impact", settings)
        LOGGER.info("%s..%s: wrote %d temporal price-change rows", start, end, count)
        results.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
