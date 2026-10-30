import json

from pyspark.sql import functions as F

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
