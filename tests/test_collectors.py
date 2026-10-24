from datetime import datetime, timezone
from types import SimpleNamespace

from collector import collectors


def _kline(open_ms: int, close_ms: int) -> list[object]:
    return [open_ms, "10", "12", "9", "11", "3", close_ms, "30", 2]


def test_live_collection_skips_unclosed_candle(monkeypatch):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    rows = [
        _kline(now_ms - 120_000, now_ms - 60_000),
        _kline(now_ms - 10_000, now_ms + 50_000),
    ]
    monkeypatch.setattr(collectors, "_get", lambda *args, **kwargs: SimpleNamespace(json=lambda: rows))

    records = collectors.BinanceCollector("https://example.test").collect_klines("BTCUSDT", "1m", 2)

    assert len(records) == 1
    assert records[0].open_time == collectors._iso_from_ms(rows[0][0])


def test_rss_unknown_offset_is_treated_as_utc():
    value = collectors._parse_rss_datetime("Thu, 23 Oct 2026 13:58:56 -0000")

    assert value == "2026-10-23T13:58:56Z"
