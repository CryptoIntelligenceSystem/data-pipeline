import math
from datetime import datetime
from typing import Any


class ValidationError(ValueError):
    """Raised when a normalized record is not safe to publish."""


def _required(record: dict[str, Any], fields: tuple[str, ...]) -> None:
    missing = [field for field in fields if record.get(field) in (None, "")]
    if missing:
        raise ValidationError(f"missing required fields: {', '.join(missing)}")


def _valid_timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValidationError(f"invalid {field}: {value}") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"{field} must include a timezone")
    return parsed


def _number(record: dict[str, Any], field: str) -> float:
    value = record.get(field)
    # bool is a subclass of int, but True/False is never a valid price or volume.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{field} must be numeric")
    if not math.isfinite(value):
        raise ValidationError(f"{field} must be finite")
    return float(value)


def validate_market(record: dict[str, Any]) -> None:
    _required(record, ("source", "symbol", "interval", "open_time", "close_time", "collected_at"))
    open_time = _valid_timestamp(record["open_time"], "open_time")
    close_time = _valid_timestamp(record["close_time"], "close_time")
    _valid_timestamp(record["collected_at"], "collected_at")
    if close_time <= open_time:
        raise ValidationError("close_time must be after open_time")
    open_, high, low, close, volume, quote_volume = (
        _number(record, field) for field in ("open", "high", "low", "close", "volume", "quote_volume")
    )
    if min(open_, high, low, close) <= 0:
        raise ValidationError("prices must be positive")
    if volume < 0 or quote_volume < 0:
        raise ValidationError("volume cannot be negative")
    trade_count = record.get("trade_count")
    if isinstance(trade_count, bool) or not isinstance(trade_count, int) or trade_count < 0:
        raise ValidationError("trade_count must be a non-negative integer")
    if high < low:
        raise ValidationError("high cannot be lower than low")
    if not low <= open_ <= high:
        raise ValidationError("open is outside low/high range")
    if not low <= close <= high:
        raise ValidationError("close is outside low/high range")


def validate_news(record: dict[str, Any]) -> None:
    _required(record, ("source", "article_id", "title", "url", "published_at", "collected_at"))
    _valid_timestamp(record["published_at"], "published_at")
    _valid_timestamp(record["collected_at"], "collected_at")
