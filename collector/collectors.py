import hashlib
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from .models import MarketRecord, NewsRecord, utc_now

LOGGER = logging.getLogger(__name__)


def _iso_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _ms_from_iso(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone, for example 2026-10-15T00:00:00Z")
    return int(parsed.timestamp() * 1000)


def _get(url: str, *, timeout: int, params: dict[str, object] | None = None,
         headers: dict[str, str] | None = None, attempts: int = 3) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt == attempts:
                break
            delay = 2 ** (attempt - 1)
            LOGGER.warning("Request failed (%d/%d), retrying in %ds: %s", attempt, attempts, delay, exc)
            import time
            time.sleep(delay)
    raise RuntimeError(f"request failed after {attempts} attempts: {url}") from last_error


class BinanceCollector:
    def __init__(self, base_url: str, timeout: int = 15) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def collect_klines(self, symbol: str, interval: str, limit: int = 100) -> list[MarketRecord]:
        response = _get(
            f"{self.base_url}/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=self.timeout,
        )
        rows = response.json()
        return [
            MarketRecord(
                data_type="market",
                source="binance_futures",
                symbol=symbol.upper(),
                interval=interval,
                open_time=_iso_from_ms(row[0]),
                close_time=_iso_from_ms(row[6]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
                quote_volume=float(row[7]),
                trade_count=int(row[8]),
                collected_at=utc_now(),
            )
            for row in rows
        ]

    def collect_klines_range(
        self, symbol: str, interval: str, start: str, end: str, limit: int = 100
    ) -> list[MarketRecord]:
        """Collect all klines in [start, end), paging through Binance REST results."""
        start_ms = _ms_from_iso(start)
        end_ms = _ms_from_iso(end)
        if start_ms >= end_ms:
            raise ValueError("start must be earlier than end")

        records: list[MarketRecord] = []
        cursor_ms = start_ms
        while cursor_ms < end_ms:
            response = _get(
                f"{self.base_url}/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor_ms,
                    "endTime": end_ms - 1,
                    "limit": limit,
                },
                timeout=self.timeout,
            )
            rows = response.json()
            if not rows:
                break

            page = [
                MarketRecord(
                    data_type="market", source="binance_futures", symbol=symbol.upper(), interval=interval,
                    open_time=_iso_from_ms(row[0]), close_time=_iso_from_ms(row[6]),
                    open=float(row[1]), high=float(row[2]), low=float(row[3]), close=float(row[4]),
                    volume=float(row[5]), quote_volume=float(row[7]), trade_count=int(row[8]),
                    collected_at=utc_now(),
                )
                for row in rows
                if start_ms <= row[0] < end_ms
            ]
            records.extend(page)
            last_open_ms = int(rows[-1][0])
            next_cursor = last_open_ms + 1
            if next_cursor <= cursor_ms:
                raise RuntimeError("Binance pagination did not advance")
            cursor_ms = next_cursor
            LOGGER.info("Historical %s: collected %d records so far", symbol, len(records))
            if len(rows) < limit:
                break
        return records


class CoinDeskCollector:
    def __init__(self, rss_url: str, timeout: int = 15) -> None:
        self.rss_url = rss_url
        self.timeout = timeout

    def collect(self) -> list[NewsRecord]:
        response = _get(self.rss_url, timeout=self.timeout, headers={"User-Agent": "data-pipeline/1.0"})
        root = ET.fromstring(response.content)
        records: list[NewsRecord] = []
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            url = (item.findtext("link") or "").strip()
            description = (item.findtext("description") or "").strip()
            published = (item.findtext("pubDate") or "").strip()
            if not title or not url or not published:
                LOGGER.warning("Skipping news item with missing title/url/pubDate")
                continue
            published_at = parsedate_to_datetime(published).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            article_id = hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
            records.append(NewsRecord(
                data_type="news", source="coindesk", article_id=article_id,
                title=title, summary=description, url=url,
                published_at=published_at, collected_at=utc_now(),
            ))
        return records
