"""Streaming: Kafka news_data -> validated rows in ClickHouse pipeline.news_data.

Invalid messages go to pipeline.news_data_rejects with their Kafka metadata. Delivery is
at-least-once; the destination is a ReplacingMergeTree keyed by (source, article_id), so a
replayed batch is safe when queries use FINAL.
"""

import logging

from pyspark.sql import DataFrame

from spark_jobs.common import clickhouse
from spark_jobs.common.config import Settings, job_env
from spark_jobs.common.kafka_io import read_kafka
from spark_jobs.common.session import build_spark
from spark_jobs.transforms import kafka_strings, news_reject_rows, news_rows, split_news

JOB = "news_to_clickhouse"
LOGGER = logging.getLogger(JOB)


def process_batch(batch: DataFrame, batch_id: int, settings: Settings) -> None:
    rows = kafka_strings(batch).persist()
    try:
        valid, rejected = split_news(rows)
        articles = news_rows(valid).persist()
        rejects = news_reject_rows(rejected).persist()
        clickhouse.write(articles, "news_data", settings)
        clickhouse.write(rejects, "news_data_rejects", settings)
        LOGGER.info("batch %d: %d articles, %d rejected", batch_id, articles.count(), rejects.count())
        articles.unpersist()
        rejects.unpersist()
    finally:
        rows.unpersist()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    spark = build_spark(JOB, settings)
    query = (
        read_kafka(spark, settings, settings.news_topic, JOB)
        .writeStream.foreachBatch(lambda df, batch_id: process_batch(df, batch_id, settings))
        .option("checkpointLocation", settings.checkpoint_path(JOB))
        .trigger(processingTime=job_env(JOB, "TRIGGER_INTERVAL", "10 seconds"))
        .queryName(JOB)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
