"""Kafka source for the streaming jobs."""

from pyspark.sql import DataFrame, SparkSession

from .config import Settings, job_env


def read_kafka(spark: SparkSession, settings: Settings, topic: str, job: str) -> DataFrame:
    """Kafka stream with columns key, value, topic, partition, offset, timestamp, timestampType.

    startingOffsets only applies the first time a query runs; afterwards the checkpoint decides.
    """
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", job_env(job, "STARTING_OFFSETS", "earliest"))
        # Topic retention may delete offsets the checkpoint still points at; keep running and log it.
        .option("failOnDataLoss", job_env(job, "FAIL_ON_DATA_LOSS", "false"))
    )
    max_offsets = job_env(job, "MAX_OFFSETS_PER_TRIGGER", "")
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)
    return reader.load()
