from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


def to_iso(value: datetime) -> str:
    """Format a datetime as ISO-8601 UTC with millisecond precision and a Z suffix."""
    if value.tzinfo is None:
        raise ValueError("datetime must include a timezone")
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_now() -> str:
    return to_iso(datetime.now(timezone.utc))


@dataclass(frozen=True)
class MarketRecord:
    data_type: str
    source: str
    symbol: str
    interval: str
    open_time: str
    close_time: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trade_count: int
    collected_at: str
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsRecord:
    data_type: str
    source: str
    article_id: str
    title: str
    summary: str
    url: str
    published_at: str
    collected_at: str
    symbols: tuple[str, ...] = ()
    sentiment: str | None = None
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["symbols"] = list(self.symbols)
        return result
