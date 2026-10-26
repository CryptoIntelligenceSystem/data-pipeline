import math

import pytest

from collector.output import validate_and_deduplicate, write_partitioned_jsonl


def market(**overrides):
    record = {
        "source": "binance_futures", "symbol": "BTCUSDT", "interval": "1m",
        "open_time": "2026-10-21T08:00:00.000Z", "close_time": "2026-10-21T08:00:59.999Z",
        "open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0,
        "volume": 3.0, "quote_volume": 30.0, "trade_count": 2,
        "collected_at": "2026-10-21T08:01:05.000Z",
    }
    record.update(overrides)
    return record


def news(**overrides):
    record = {
        "source": "coindesk", "article_id": "abc", "title": "t", "url": "https://x",
        "published_at": "2026-10-21T08:00:00.000Z", "collected_at": "2026-10-21T08:01:00.000Z",
    }
    record.update(overrides)
    return record


def test_market_duplicate_is_removed():
    assert len(validate_and_deduplicate([market(), market()], "market")) == 1


def test_invalid_market_record_is_rejected():
    assert validate_and_deduplicate([market(high=8.0)], "market") == []


@pytest.mark.parametrize("overrides", [
    {"volume": math.nan},
    {"quote_volume": math.inf},
    {"open": True},
    {"low": 0.0, "open": 0.0},
    {"trade_count": -1},
    {"trade_count": 2.5},
    {"close_time": "2026-10-21T08:00:00.000Z"},
    {"open_time": 12345},
])
def test_market_rejects_bad_values(overrides):
    assert validate_and_deduplicate([market(**overrides)], "market") == []


def test_news_without_collected_at_is_rejected_not_crashing():
    record = news()
    del record["collected_at"]
    assert validate_and_deduplicate([record], "news") == []


def test_partitioned_writer_appends_and_skips_existing(tmp_path):
    written, skipped = write_partitioned_jsonl([market()], tmp_path, "market")
    assert (len(written), skipped) == (1, 0)
    written, skipped = write_partitioned_jsonl([market()], tmp_path, "market")
    assert (written, skipped) == ([], 1)
    path = tmp_path / "market" / "2026" / "10" / "21.jsonl"
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
