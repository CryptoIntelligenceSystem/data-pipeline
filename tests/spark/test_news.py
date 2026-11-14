import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.common.clickhouse import _json_ready
from spark_jobs.common import contracts
from spark_jobs.transforms import news_rows, split_news

from .conftest import kafka_rows


def news_message(**overrides):
    message = json.loads((contracts.CONTRACT_DIR / "examples" / "news_article.valid.json").read_text())
    message.update(overrides)
    return message


def test_valid_news_matches_clickhouse_columns(spark):
    valid, rejected = split_news(kafka_rows(spark, [news_message()], topic="news_data"))

    assert valid.count() == 1
    assert rejected.count() == 0
    assert news_rows(valid).columns == list(contracts.properties("news_article"))


def test_invalid_news_is_rejected(spark):
    message = news_message(url="not-a-url", symbols=["BTCUSDT", "BTCUSDT"])
    _, rejected = split_news(kafka_rows(spark, [message], topic="news_data"))

    errors = rejected.select(F.explode("errors").alias("error")).rdd.map(lambda row: row["error"]).collect()
    assert "pattern:url" in errors
    assert "unique:symbols" in errors


def test_required_nullable_sentiment_may_be_null_but_must_be_present(spark):
    explicit_null = news_message(sentiment=None)
    valid, _ = split_news(kafka_rows(spark, [explicit_null], topic="news_data"))
    assert valid.count() == 1

    missing = news_message()
    del missing["sentiment"]
    _, rejected = split_news(kafka_rows(spark, [missing], topic="news_data"))
    errors = rejected.select(F.explode("errors").alias("error")).rdd.map(lambda row: row["error"]).collect()
    assert "missing:sentiment" in errors


def test_clickhouse_json_transport_preserves_arrays_and_formats_timestamps(spark):
    frame = spark.createDataFrame(
        [(datetime(2026, 10, 29, 12, 32, 0, 123000, tzinfo=timezone.utc), ["BTCUSDT"], None)],
        T.StructType([
            T.StructField("published_at", T.TimestampType()),
            T.StructField("symbols", T.ArrayType(T.StringType())),
            T.StructField("sentiment", T.StringType()),
        ]),
    )

    row = _json_ready(frame).collect()[0]

    assert row["published_at"] == "2026-10-29 12:32:00.123"
    assert row["symbols"] == ["BTCUSDT"]
    assert row["sentiment"] is None
