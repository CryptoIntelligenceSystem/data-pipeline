"""Streaming: flag 1m candles whose volume > mean + K * stddev of the previous N candles.

Structured Streaming cannot use row-based windows (rowsBetween) on a stream, so each micro-batch
is handled in foreachBatch: the N candles before the batch are read from ClickHouse
(pipeline.market_data FINAL), unioned with the batch, and scored with an ordinary window.
Signals go to pipeline.signals (ReplacingMergeTree on symbol, signal_type, open_time), so a
replayed batch rewrites the same signals instead of duplicating them.
"""

import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.common import clickhouse, contracts
from spark_jobs.common.config import Settings, job_env
from spark_jobs.common.kafka_io import read_kafka
from spark_jobs.common.session import build_spark
from spark_jobs.transforms import dedupe_candles, detect_volume_anomalies, kafka_strings, split_market

JOB = "volume_anomaly"
LOGGER = logging.getLogger(JOB)
PERIODS = int(os.getenv("ANOMALY_PERIODS", "20"))
K = float(os.getenv("ANOMALY_STDDEV_K", "3"))
LOOKBACK_HOURS = int(os.getenv("ANOMALY_LOOKBACK_HOURS", "24"))


def history_query(symbols: list[str], before_ms: int) -> str:
    """Candles preceding the batch. Symbols already passed the contract pattern ^[A-Z0-9]+$ and
    before_ms is an int, so building the SQL string is injection-safe."""
    symbol_list = ", ".join(f"'{symbol}'" for symbol in symbols)
    return (
        "SELECT toString(symbol) AS symbol, toUnixTimestamp64Milli(open_time) AS open_time_ms, "
        "toFloat64(volume) AS volume "
        "FROM pipeline.market_data FINAL "
        f"WHERE interval = '1m' AND symbol IN ({symbol_list}) "
        f"AND open_time < fromUnixTimestamp64Milli({before_ms}, 'UTC') "
        f"AND open_time >= fromUnixTimestamp64Milli({before_ms}, 'UTC') - INTERVAL {LOOKBACK_HOURS} HOUR"
    )


def process_batch(batch: DataFrame, batch_id: int, settings: Settings) -> None:
    valid, _ = split_market(kafka_strings(batch))  # rejects are recorded by market_to_clickhouse
    current = dedupe_candles(valid).select("symbol", "open_time", "volume").persist()
    try:
        # Epoch millis computed in Spark: collecting a timestamp would convert it to a naive
        # datetime in the driver's local timezone.
        bounds = current.agg(F.collect_set("symbol").alias("symbols"),
                             F.expr("unix_millis(min(open_time))").alias("first_ms")).first()
        if not bounds["symbols"]:
            return
        before_ms = int(bounds["first_ms"])
        history = clickhouse.read_query(current.sparkSession, settings,
                                        history_query(sorted(bounds["symbols"]), before_ms))
        history = history.select("symbol", F.timestamp_millis("open_time_ms").alias("open_time"), "volume")
        signals = detect_volume_anomalies(history, current, PERIODS, K).persist()
        clickhouse.write(signals, "signals", settings)
        LOGGER.info("batch %d: %d candles scored, %d signals", batch_id, current.count(), signals.count())
        signals.unpersist()
    finally:
        current.unpersist()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    spark = build_spark(JOB, settings)
    LOGGER.info("Rule: volume > mean + %s * stddev_samp of previous %d candles (contract %s)",
                K, PERIODS, contracts.load_schema("market_kline")["title"])
    query = (
        read_kafka(spark, settings, settings.market_topic, JOB)
        .writeStream.foreachBatch(lambda df, batch_id: process_batch(df, batch_id, settings))
        .option("checkpointLocation", settings.checkpoint_path(JOB))
        .trigger(processingTime=job_env(JOB, "TRIGGER_INTERVAL", "30 seconds"))
        .queryName(JOB)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
