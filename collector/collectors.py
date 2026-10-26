import hashlib
import html
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Iterable

import requests

from .models import MarketRecord, NewsRecord, to_iso, utc_now

LOGGER = logging.getLogger(__name__)

# Binance answers these with "slow down"; every other 4xx is a caller error and retrying will not help.
_RETRYABLE_STATUS = {418, 429}
_TAG_RE = re.compile(r"<[^>]+>")
_ASSET_ALIASES = {
    "BTC": ("bitcoin", "btc"),
    "ETH": ("ethereum", "ether", "eth"),
    "SOL": ("solana", "sol"),
    "BNB": ("bnb", "binance coin"),
    "XRP": ("xrp", "ripple"),
}
_QUOTE_ASSETS = ("USDT", "USDC", "BUSD", "USD")


def _iso_from_ms(value: int) -> str:
    return to_iso(datetime.fromtimestamp(value / 1000, tz=timezone.utc))


def _ms_from_iso(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone, for example 2026-10-15T00:00:00Z")
    return int(parsed.timestamp() * 1000)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return float(retry_after)
    return float(2 ** (attempt - 1))


def _get(url: str, *, timeout: int, params: dict[str, object] | None = None,
         headers: dict[str, str] | None = None, attempts: int = 3) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        response: requests.Response | None = None
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            status = response.status_code if response is not None else None
            if status is not None and 400 <= status < 500 and status not in _RETRYABLE_STATUS:
                body = response.text[:200] if response is not None else ""
                raise RuntimeError(f"request rejected with HTTP {status}: {url} {body}") from exc
            if attempt == attempts:
                break
            delay = _retry_delay(response, attempt)
            LOGGER.warning("Request failed (%d/%d), retrying in %.0fs: %s", attempt, attempts, delay, exc)
            time.sleep(delay)
    raise RuntimeError(f"request failed after {attempts} attempts: {url}") from last_error


def _json(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(f"invalid JSON from {response.url}") from exc


def _kline_record(row: list[Any], symbol: str, interval: str, collected_at: str) -> MarketRecord:
    return MarketRecord(
        data_type="market",
        source="binance_futures",
        symbol=symbol.upper(),
        interval=interval,
        open_time=_iso_from_ms(int(row[0])),
        close_time=_iso_from_ms(int(row[6])),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
        trade_count=int(row[8]),
        collected_at=collected_at,
    )


def _closed_klines(rows: Iterable[list[Any]], symbol: str, interval: str, now_ms: int,
                   keep: Callable[[int], bool] = lambda _: True) -> list[MarketRecord]:
    """Convert raw kline rows, dropping candles that are still open at now_ms.

    An open candle keeps changing until close_time, and the writer never overwrites a stored
    open_time, so saving it would freeze a partial value forever.
    """
    collected_at = utc_now()
    records: list[MarketRecord] = []
    for row in rows:
        try:
            open_ms, close_ms = int(row[0]), int(row[6])
            if close_ms >= now_ms or not keep(open_ms):
                continue
            records.append(_kline_record(row, symbol, interval, collected_at))
        except (IndexError, TypeError, ValueError) as exc:
            LOGGER.error("Skipping malformed %s kline %r: %s", symbol, row, exc)
    return records


class BinanceCollector:
    def __init__(self, base_url: str, timeout: int = 15, now_ms: Callable[[], int] = _now_ms) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.now_ms = now_ms

    def _klines(self, params: dict[str, object]) -> list[list[Any]]:
        rows = _json(_get(f"{self.base_url}/fapi/v1/klines", params=params, timeout=self.timeout))
        if not isinstance(rows, list):
            raise RuntimeError(f"unexpected Binance response: {str(rows)[:200]}")
        return rows

    def collect_klines(self, symbol: str, interval: str, limit: int = 100) -> list[MarketRecord]:
        rows = self._klines({"symbol": symbol, "interval": interval, "limit": limit})
        return _closed_klines(rows, symbol, interval, self.now_ms())

    def collect_klines_range(
        self, symbol: str, interval: str, start: str, end: str, limit: int = 1000
    ) -> list[MarketRecord]:
        """Collect all closed klines in [start, end), paging through Binance REST results."""
        start_ms = _ms_from_iso(start)
        end_ms = _ms_from_iso(end)
        if start_ms >= end_ms:
            raise ValueError("start must be earlier than end")

        records: list[MarketRecord] = []
        cursor_ms = start_ms
        while cursor_ms < end_ms:
            rows = self._klines({
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor_ms,
                "endTime": end_ms - 1,
                "limit": limit,
            })
            if not rows:
                break

            records.extend(_closed_klines(
                rows, symbol, interval, self.now_ms(), keep=lambda open_ms: start_ms <= open_ms < end_ms
            ))
            next_cursor = int(rows[-1][0]) + 1
            if next_cursor <= cursor_ms:
                raise RuntimeError("Binance pagination did not advance")
            cursor_ms = next_cursor
            LOGGER.info("Historical %s: collected %d records so far", symbol, len(records))
            if len(rows) < limit:
                break
        return records


def _parse_pub_date(value: str) -> str:
    published = parsedate_to_datetime(value)
    if published.tzinfo is None:
        # RFC 2822 "-0000" means UTC with unknown local offset; never fall back to the machine timezone.
        published = published.replace(tzinfo=timezone.utc)
    return to_iso(published)


def _clean_text(value: str) -> str:
    return " ".join(html.unescape(_TAG_RE.sub(" ", value)).split())


def tag_symbols(text: str, symbols: Iterable[str]) -> tuple[str, ...]:
    """Return the configured trading symbols whose base asset is mentioned in text."""
    words = set(re.findall(r"[a-z0-9]+", text.lower()))
    lowered = text.lower()
    tagged: list[str] = []
    for symbol in symbols:
        base = symbol.upper()
        for quote in _QUOTE_ASSETS:
            if base.endswith(quote) and len(base) > len(quote):
                base = base[: -len(quote)]
                break
        aliases = _ASSET_ALIASES.get(base, (base.lower(),))
        if any((alias in words) if " " not in alias else (alias in lowered) for alias in aliases):
            tagged.append(symbol.upper())
    return tuple(tagged)


class CoinDeskCollector:
    def __init__(self, rss_url: str, timeout: int = 15, symbols: Iterable[str] = ()) -> None:
        self.rss_url = rss_url
        self.timeout = timeout
        self.symbols = tuple(symbols)

    def collect(self) -> list[NewsRecord]:
        response = _get(self.rss_url, timeout=self.timeout, headers={"User-Agent": "data-pipeline/1.0"})
        return self.parse(response.content)

    def parse(self, content: bytes) -> list[NewsRecord]:
        try:
            root = ET.fromstring(content)
        except ET.ParseError as exc:
            raise RuntimeError(f"invalid RSS from {self.rss_url}: {exc}") from exc
        collected_at = utc_now()
        records: list[NewsRecord] = []
        for item in root.findall(".//item"):
            title = _clean_text(item.findtext("title") or "")
            url = (item.findtext("link") or "").strip()
            description = _clean_text(item.findtext("description") or "")
            published = (item.findtext("pubDate") or "").strip()
            if not title or not url or not published:
                LOGGER.warning("Skipping news item with missing title/url/pubDate: %s", url or title)
                continue
            try:
                published_at = _parse_pub_date(published)
            except (TypeError, ValueError) as exc:
                LOGGER.error("Skipping news item with invalid pubDate %r: %s", published, exc)
                continue
            records.append(NewsRecord(
                data_type="news", source="coindesk",
                article_id=hashlib.sha256(url.encode("utf-8")).hexdigest()[:24],
                title=title, summary=description, url=url,
                published_at=published_at, collected_at=collected_at,
                symbols=tag_symbols(f"{title} {description}", self.symbols),
            ))
        return records
