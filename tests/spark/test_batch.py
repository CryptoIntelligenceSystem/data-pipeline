from pyspark.sql import functions as F

from spark_jobs.common import contracts
from spark_jobs.transforms import aggregate_ohlcv_5m, dedupe_candles, market_rows, moving_average, split_market

from .conftest import candles_df, kafka_rows, market_message


def _iso(column):
    return F.date_format(column, contracts.TIMESTAMP_OUTPUT_FORMAT)


def test_ohlcv_5m_hand_calculated(spark):
    # 08:00..08:04 BTCUSDT, given out of order to prove open/close follow open_time:
    #   minute open high low close volume
    #   0      10   12   9   11    1
    #   1      11   13   10  12    2
    #   2      12   15   11  13    3
    #   3      13   14   12  14    4
    #   4      14   16   13  15    5
    # Expected 5m: open 10 (minute 0), high 16, low 9, close 15 (minute 4), volume 15, trades 50.
    rows = [
        ("BTCUSDT", 3, 13, 14, 12, 14, 4),
        ("BTCUSDT", 0, 10, 12, 9, 11, 1),
        ("BTCUSDT", 4, 14, 16, 13, 15, 5),
        ("BTCUSDT", 1, 11, 13, 10, 12, 2),
        ("BTCUSDT", 2, 12, 15, 11, 13, 3),
        # 08:05..08:07: only 3 candles -> incomplete bucket, must be skipped
        ("BTCUSDT", 5, 15, 16, 14, 15, 1),
        ("BTCUSDT", 6, 15, 16, 14, 15, 1),
        ("BTCUSDT", 7, 15, 16, 14, 15, 1),
    ]
    result = aggregate_ohlcv_5m(candles_df(spark, rows)).select(
        "symbol", "interval", _iso("open_time").alias("open_time"), _iso("close_time").alias("close_time"),
        "open", "high", "low", "close", "volume", "quote_volume", "trade_count", "candle_count",
    ).collect()

    assert len(result) == 1
    candle = result[0].asDict()
    assert candle == {
        "symbol": "BTCUSDT", "interval": "5m",
        "open_time": "2026-10-27T08:00:00.000Z", "close_time": "2026-10-27T08:04:59.999Z",
        "open": 10.0, "high": 16.0, "low": 9.0, "close": 15.0,
        "volume": 15.0, "quote_volume": 1500.0, "trade_count": 50, "candle_count": 5,
    }


def test_ohlcv_5m_keeps_symbols_apart(spark):
    rows = [(symbol, m, 1, 2, 1, 2, 1) for symbol in ("BTCUSDT", "ETHUSDT") for m in range(5)]
    result = aggregate_ohlcv_5m(candles_df(spark, rows)).groupBy("symbol").count().collect()
    assert sorted((r["symbol"], r["count"]) for r in result) == [("BTCUSDT", 1), ("ETHUSDT", 1)]


def test_ma20_hand_calculated(spark):
    # close = 1, 2, ..., 25 at minutes 0..24.
    # MA20 at minute 19 = mean(1..20) = 10.5; at minute 24 = mean(6..25) = 15.5; minutes 0..18 = NULL.
    rows = [("BTCUSDT", m, m + 1, m + 1, m + 1, m + 1, 1) for m in range(25)]
    result = {
        r["minute"]: r["value"]
        for r in moving_average(candles_df(spark, rows), 20)
        .select(F.minute("open_time").alias("minute"), "value", "name").collect()
    }
    assert all(result[m] is None for m in range(19))
    assert result[19] == 10.5
    assert result[24] == 15.5
    assert len(result) == 25


def test_dedupe_keeps_latest_collected(spark):
    first = market_message(volume=1.0, collected_at="2026-10-27T08:01:00.000Z")
    later = market_message(volume=2.0, collected_at="2026-10-27T08:05:00.000Z")
    valid, _ = split_market(kafka_rows(spark, [later, first]))
    rows = dedupe_candles(valid).collect()
    assert len(rows) == 1 and rows[0]["volume"] == 2.0


def test_market_rows_match_clickhouse_columns(spark):
    valid, _ = split_market(kafka_rows(spark, [market_message()]))
    columns = market_rows(valid).columns
    assert columns == list(contracts.properties("market_kline")) + ["processed_at"]
