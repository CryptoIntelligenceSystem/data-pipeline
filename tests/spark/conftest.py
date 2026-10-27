import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import types as T

from spark_jobs.common.contracts import CONTRACT_DIR
from spark_jobs.common.session import base_builder

T0 = datetime(2026, 10, 27, 8, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        base_builder("unit-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def market_message(minute: int = 0, symbol: str = "BTCUSDT", **overrides) -> dict:
    """A valid market_kline v1 message for T0 + minute, based on the contract example."""
    message = json.loads((CONTRACT_DIR / "examples" / "market_kline.valid.json").read_text(encoding="utf-8"))
    open_time = T0 + timedelta(minutes=minute)
    message.update(
        symbol=symbol,
        open_time=iso(open_time),
        close_time=iso(open_time + timedelta(seconds=59, milliseconds=999)),
        collected_at=iso(open_time + timedelta(minutes=1)),
    )
    message.update(overrides)
    return message


KAFKA_SCHEMA = T.StructType([
    T.StructField("topic", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("key", T.StringType()),
    T.StructField("value", T.StringType()),
])


def kafka_rows(spark, values: list, topic: str = "market_data"):
    """DataFrame shaped like decoded Kafka rows; dict values are serialised to JSON."""
    rows = [
        (topic, 0, offset, T0 + timedelta(minutes=offset),
         value.get("symbol") if isinstance(value, dict) else None,
         json.dumps(value) if isinstance(value, dict) else value)
        for offset, value in enumerate(values)
    ]
    # Datetimes are timezone-aware: PySpark would read naive ones in the machine's local zone.
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def candles_df(spark, rows: list[tuple], interval: str = "1m"):
    """rows: (symbol, minute_offset, open, high, low, close, volume) -> 1m candle DataFrame."""
    schema = T.StructType([
        T.StructField("symbol", T.StringType()),
        T.StructField("interval", T.StringType()),
        T.StructField("open_time", T.TimestampType()),
        T.StructField("open", T.DoubleType()),
        T.StructField("high", T.DoubleType()),
        T.StructField("low", T.DoubleType()),
        T.StructField("close", T.DoubleType()),
        T.StructField("volume", T.DoubleType()),
        T.StructField("quote_volume", T.DoubleType()),
        T.StructField("trade_count", T.LongType()),
    ])
    data = [
        (symbol, interval, T0 + timedelta(minutes=minute),
         float(o), float(h), float(low), float(c), float(v), float(v) * 100, 10)
        for symbol, minute, o, h, low, c, v in rows
    ]
    return spark.createDataFrame(data, schema)
