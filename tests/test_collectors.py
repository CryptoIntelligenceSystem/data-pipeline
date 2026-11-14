import argparse

import pytest
import requests

from collector import collectors, main
from collector.collectors import BinanceCollector, CoinDeskCollector, tag_symbols

MINUTE = 60_000
T0 = 1_792_800_000_000  # 2026-10-24T00:00:00Z, minute aligned


def kline(open_ms, price=100.0):
    return [open_ms, str(price), str(price + 1), str(price - 1), str(price), "5", open_ms + MINUTE - 1,
            "500", 10, "0", "0", "0"]


class FakeResponse:
    def __init__(self, payload=None, status=200, content=b"", headers=None):
        self.payload = payload
        self.status_code = status
        self.content = content
        self.text = content.decode() if content else ""
        self.headers = headers or {}
        self.url = "https://example.test"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


def test_latest_klines_drop_open_candle(monkeypatch):
    rows = [kline(T0), kline(T0 + MINUTE), kline(T0 + 2 * MINUTE)]
    monkeypatch.setattr(collectors, "_get", lambda *a, **k: FakeResponse(rows))
    now = T0 + 2 * MINUTE + 10_000  # third candle is still running
    records = BinanceCollector("https://x", now_ms=lambda: now).collect_klines("btcusdt", "1m", 3)
    assert [r.open_time for r in records] == ["2026-10-24T00:00:00.000Z", "2026-10-24T00:01:00.000Z"]
    assert records[0].close_time == "2026-10-24T00:00:59.999Z"
    assert records[0].symbol == "BTCUSDT"


def test_range_paginates_and_respects_bounds(monkeypatch):
    calls = []

    def fake_get(url, *, timeout, params=None, headers=None):
        calls.append(params["startTime"])
        start = -(-params["startTime"] // MINUTE) * MINUTE  # Binance returns candles opening at or after startTime
        rows = [kline(t) for t in range(start, min(start + 2 * MINUTE, params["endTime"] + 1), MINUTE)]
        return FakeResponse(rows)

    monkeypatch.setattr(collectors, "_get", fake_get)
    collector = BinanceCollector("https://x", now_ms=lambda: T0 + 100 * MINUTE)
    records = collector.collect_klines_range(
        "BTCUSDT", "1m", "2026-10-24T00:00:00Z", "2026-10-24T00:05:00Z", limit=2
    )
    assert len(records) == 5
    assert len(calls) == 3
    assert records[-1].open_time == "2026-10-24T00:04:00.000Z"


def test_invalid_binance_json_becomes_runtime_error(monkeypatch):
    monkeypatch.setattr(collectors, "_get", lambda *a, **k: FakeResponse(ValueError("bad json")))
    with pytest.raises(RuntimeError):
        BinanceCollector("https://x").collect_klines("BTCUSDT", "1m")


def test_client_error_is_not_retried(monkeypatch):
    calls = []

    def fake_requests_get(*args, **kwargs):
        calls.append(1)
        return FakeResponse(status=400, content=b'{"msg":"Invalid symbol."}')

    monkeypatch.setattr(collectors.requests, "get", fake_requests_get)
    monkeypatch.setattr(collectors.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="HTTP 400"):
        collectors._get("https://x", timeout=1)
    assert len(calls) == 1


def test_rate_limit_is_retried_with_retry_after(monkeypatch):
    responses = [FakeResponse(status=429, headers={"Retry-After": "7"}), FakeResponse([])]
    sleeps = []
    monkeypatch.setattr(collectors.requests, "get", lambda *a, **k: responses.pop(0))
    monkeypatch.setattr(collectors.time, "sleep", sleeps.append)
    assert collectors._get("https://x", timeout=1).status_code == 200
    assert sleeps == [7.0]


RSS = b"""<?xml version="1.0"?><rss><channel>
<item><title>Bitcoin &amp; Ether rally</title><link>https://n/1</link>
<description>&lt;p&gt;BTC tops &lt;b&gt;record&lt;/b&gt;&lt;/p&gt;</description>
<pubDate>Fri, 23 Oct 2026 13:58:56 -0000</pubDate></item>
<item><title>Broken date</title><link>https://n/2</link><pubDate>not a date</pubDate></item>
<item><title>Missing link</title><pubDate>Fri, 23 Oct 2026 13:58:56 +0000</pubDate></item>
<item><title>Solana news</title><link>https://n/3</link><pubDate>Fri, 23 Oct 2026 20:00:00 +0700</pubDate></item>
</channel></rss>"""


def test_rss_parsing_handles_timezones_bad_items_and_html():
    records = CoinDeskCollector("https://rss", symbols=("BTCUSDT", "ETHUSDT")).parse(RSS)
    assert [r.url for r in records] == ["https://n/1", "https://n/3"]
    first, second = records
    assert first.published_at == "2026-10-23T13:58:56.000Z"  # -0000 is UTC, not local time
    assert second.published_at == "2026-10-23T13:00:00.000Z"
    assert first.title == "Bitcoin & Ether rally"
    assert first.summary == "BTC tops record"
    assert first.symbols == ("BTCUSDT", "ETHUSDT")
    assert second.symbols == ()


def test_invalid_rss_becomes_runtime_error():
    with pytest.raises(RuntimeError):
        CoinDeskCollector("https://rss").parse(b"<rss><channel>")


def test_tag_symbols_uses_whole_words():
    assert tag_symbols("Ethereum staking grows", ["BTCUSDT", "ETHUSDT"]) == ("ETHUSDT",)
    assert tag_symbols("Something unrelated", ["BTCUSDT"]) == ()
    assert tag_symbols("Methods for builders", ["ETHUSDT"]) == ()


def test_market_failure_does_not_stop_news(monkeypatch):
    ran = []
    monkeypatch.setattr(main, "collect_market", lambda *a: (_ for _ in ()).throw(KeyError("boom")))
    monkeypatch.setattr(main, "collect_news", lambda *a: ran.append("news"))
    main.run_once(argparse.Namespace(source="all"), settings=None)
    assert ran == ["news"]


def test_only_new_records_are_sent_to_kafka(monkeypatch, tmp_path):
    from tests.test_validation import market

    sent = []
    monkeypatch.setattr(main, "publish", lambda records, topic, servers: sent.append(list(records)))
    settings = main.Settings(output_dir=str(tmp_path / "samples"))
    args = argparse.Namespace(send_kafka=True)
    main._store([market()], "market", "market_data", args, settings)
    main._store([market()], "market", "market_data", args, settings)
    assert [len(batch) for batch in sent] == [1]


def test_kafka_failure_does_not_mark_records_as_stored(monkeypatch, tmp_path):
    from tests.test_validation import market

    def fail_publish(*args, **kwargs):
        raise RuntimeError("broker unavailable")

    monkeypatch.setattr(main, "publish", fail_publish)
    settings = main.Settings(output_dir=str(tmp_path / "samples"))
    args = argparse.Namespace(send_kafka=True)

    main._store([market()], "market", "market_data", args, settings)

    path = tmp_path / "samples" / "market" / "2026" / "10" / "21.jsonl"
    assert not path.exists()
    pending_path = tmp_path / "outbox" / "market.jsonl"
    assert pending_path.exists()


def test_pending_records_are_retried_and_promoted(monkeypatch, tmp_path):
    from tests.test_validation import market

    attempts = []

    def publish_once(records, topic, bootstrap_servers):
        attempts.append(list(records))
        if len(attempts) == 1:
            raise RuntimeError("broker unavailable")

    monkeypatch.setattr(main, "publish", publish_once)
    settings = main.Settings(output_dir=str(tmp_path / "samples"))
    args = argparse.Namespace(send_kafka=True)

    main._store([market()], "market", "market_data", args, settings)
    main._store([], "market", "market_data", args, settings)

    assert len(attempts) == 2
    assert (tmp_path / "samples" / "market" / "2026" / "10" / "21.jsonl").exists()
    assert not (tmp_path / "outbox" / "market.jsonl").exists()
