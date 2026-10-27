"""Streaming: Kafka market_data / news_data -> raw JSON in MinIO, partitioned by event date.

Output: s3a://<MINIO_RAW_BUCKET>/{market,news}/year=YYYY/month=M/day=D/part-*.json, one
contracts/v1/raw_record per line. The file sink plus checkpoint gives exactly-once files:
a restarted query resumes from the checkpoint and never rewrites a committed batch.
"""

import logging

from spark_jobs.common.config import Settings, job_env
from spark_jobs.common.kafka_io import read_kafka
from spark_jobs.common.session import build_spark
from spark_jobs.transforms import raw_records

JOB = "raw_to_minio"
LOGGER = logging.getLogger(JOB)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    spark = build_spark(JOB, settings)
    trigger = job_env(JOB, "TRIGGER_INTERVAL", "1 minute")  # longer trigger = fewer, larger files

    for topic, dataset, event_field in (
        (settings.market_topic, "market", "open_time"),
        (settings.news_topic, "news", "published_at"),
    ):
        (
            raw_records(read_kafka(spark, settings, topic, JOB), event_field)
            .writeStream.format("json")
            .partitionBy("year", "month", "day")
            .option("path", settings.raw_path(dataset))
            .option("checkpointLocation", settings.checkpoint_path(f"{JOB}/{dataset}"))
            .trigger(processingTime=trigger)
            .queryName(f"{JOB}_{dataset}")
            .start()
        )
        LOGGER.info("Started %s: %s -> %s", dataset, topic, settings.raw_path(dataset))

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
