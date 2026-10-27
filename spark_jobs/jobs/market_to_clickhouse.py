"""Streaming: Kafka market_data -> validated 1m candles in ClickHouse pipeline.market_data.

Invalid messages go to pipeline.market_data_rejects with their errors (never dropped silently).
Delivery is at-least-once (foreachBatch may replay the last batch after a restart); both tables
are ReplacingMergeTree on their natural key, so replays do not create duplicates under FINAL.
"""

import logging

from pyspark.sql import DataFrame

from spark_jobs.common import clickhouse
from spark_jobs.common.config import Settings, job_env
from spark_jobs.common.kafka_io import read_kafka
from spark_jobs.common.session import build_spark
from spark_jobs.transforms import kafka_strings, market_rows, reject_rows, split_market

JOB = "market_to_clickhouse"
LOGGER = logging.getLogger(JOB)


def process_batch(batch: DataFrame, batch_id: int, settings: Settings) -> None:
    rows = kafka_strings(batch).persist()
    try:
        valid, rejected = split_market(rows)
        candles = market_rows(valid).persist()
        rejects = reject_rows(rejected).persist()
        clickhouse.write(candles, "market_data", settings)
        clickhouse.write(rejects, "market_data_rejects", settings)
        LOGGER.info("batch %d: %d candles, %d rejected", batch_id, candles.count(), rejects.count())
        candles.unpersist()
        rejects.unpersist()
    finally:
        rows.unpersist()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    spark = build_spark(JOB, settings)
    query = (
        read_kafka(spark, settings, settings.market_topic, JOB)
        .writeStream.foreachBatch(lambda df, batch_id: process_batch(df, batch_id, settings))
        .option("checkpointLocation", settings.checkpoint_path(JOB))
        .trigger(processingTime=job_env(JOB, "TRIGGER_INTERVAL", "10 seconds"))
        .queryName(JOB)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
