"""Price movement is measured against closed 1m candles near each requested horizon."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.transforms import calculate_news_impact

from .conftest import T0


ARTICLE_SCHEMA = T.StructType([
    T.StructField("source", T.StringType()),
    T.StructField("article_id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("symbol", T.StringType()),
    T.StructField("published_ms", T.LongType()),
])
PRICE_SCHEMA = T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("close_ms", T.LongType()),
    T.StructField("close", T.DoubleType()),
])


def ms(minutes=0, seconds=0, millis=0):
    return int(T0.timestamp() * 1000) + minutes * 60000 + seconds * 1000 + millis


def test_news_impact_matches_article_symbol_and_horizons(spark):
    articles = spark.createDataFrame([
        ("coindesk", "article-a", "BTC update", "BTCUSDT", ms(seconds=30)),
        ("coindesk", "article-a", "BTC update", "ETHUSDT", ms(seconds=30)),
    ], ARTICLE_SCHEMA)
    prices = spark.createDataFrame([
        ("BTCUSDT", ms(minutes=-1, seconds=59, millis=999), 100.0),
        ("BTCUSDT", ms(minutes=5, seconds=59, millis=999), 110.0),
        ("BTCUSDT", ms(minutes=15, seconds=59, millis=999), 90.0),
        ("BTCUSDT", ms(minutes=60, seconds=59, millis=999), 120.0),
        ("ETHUSDT", ms(minutes=-1, seconds=59, millis=999), 200.0),
        ("ETHUSDT", ms(minutes=5, seconds=59, millis=999), 220.0),
    ], PRICE_SCHEMA)

    rows = calculate_news_impact(articles, prices).select(
        "*", F.date_format("published_at", "yyyy-MM-dd HH:mm:ss.SSS").alias("published_text")
    ).collect()
    by_key = {(row.symbol, row.horizon_minutes): row for row in rows}

    assert len(rows) == 4
    assert by_key[("BTCUSDT", 5)].return_pct == pytest.approx(10.0)
    assert by_key[("BTCUSDT", 15)].return_pct == pytest.approx(-10.0)
    assert by_key[("BTCUSDT", 60)].return_pct == pytest.approx(20.0)
    assert by_key[("ETHUSDT", 5)].return_pct == pytest.approx(10.0)
    assert by_key[("BTCUSDT", 5)].published_text == "2026-10-27 08:00:30.000"


def test_news_impact_skips_missing_or_stale_candles(spark):
    articles = spark.createDataFrame([
        ("coindesk", "stale-baseline", "Old price", "BTCUSDT", ms(seconds=30)),
        ("coindesk", "missing-target", "No five minute price", "ETHUSDT", ms(seconds=30)),
    ], ARTICLE_SCHEMA)
    prices = spark.createDataFrame([
        ("BTCUSDT", ms(minutes=-3, seconds=59, millis=999), 100.0),
        ("BTCUSDT", ms(minutes=5, seconds=59, millis=999), 110.0),
        ("ETHUSDT", ms(minutes=-1, seconds=59, millis=999), 100.0),
        ("ETHUSDT", ms(minutes=7, seconds=59, millis=999), 110.0),
    ], PRICE_SCHEMA)

    assert calculate_news_impact(articles, prices).count() == 0
