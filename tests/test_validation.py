from collector.output import validate_and_deduplicate, write_partitioned_jsonl


def test_market_duplicate_is_removed():
    record = {
        "source": "binance_futures", "symbol": "BTCUSDT", "interval": "1m",
        "open_time": "2026-10-21T08:00:00Z", "close_time": "2026-10-21T08:00:59Z",
        "open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0,
        "volume": 3.0, "quote_volume": 30.0, "trade_count": 2,
    }
    assert len(validate_and_deduplicate([record, record], "market")) == 1


def test_invalid_market_record_is_rejected():
    record = {
        "source": "binance_futures", "symbol": "BTCUSDT", "interval": "1m",
        "open_time": "2026-10-21T08:00:00Z", "close_time": "2026-10-21T08:00:59Z",
        "open": 10.0, "high": 8.0, "low": 9.0, "close": 9.5,
        "volume": 3.0, "quote_volume": 30.0, "trade_count": 2,
    }
    assert validate_and_deduplicate([record], "market") == []


def test_partitioned_writer_appends_and_skips_existing(tmp_path):
    record = {
        "source": "binance_futures", "symbol": "BTCUSDT", "interval": "1m",
        "open_time": "2026-10-21T08:00:00Z", "close_time": "2026-10-21T08:00:59Z",
        "open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0,
        "volume": 3.0, "quote_volume": 30.0, "trade_count": 2,
    }
    assert write_partitioned_jsonl([record], tmp_path, "market") == (1, 0)
    assert write_partitioned_jsonl([record], tmp_path, "market") == (0, 1)
    path = tmp_path / "market" / "2026" / "10" / "21.jsonl"
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
