from datetime import datetime
from typing import Any


class ValidationError(ValueError):
    """Raised when a normalized record is not safe to publish."""


def _required(record: dict[str, Any], fields: tuple[str, ...]) -> None:
    missing = [field for field in fields if record.get(field) in (None, "")]
    if missing:
        raise ValidationError(f"missing required fields: {', '.join(missing)}")


def _valid_timestamp(value: str, field: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"invalid {field}: {value}") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"{field} must include a timezone")


def validate_market(record: dict[str, Any]) -> None:
    _required(record, ("source", "symbol", "interval", "open_time", "close_time"))
    _valid_timestamp(record["open_time"], "open_time")
    _valid_timestamp(record["close_time"], "close_time")
    for field in ("open", "high", "low", "close", "volume", "quote_volume"):
        if not isinstance(record.get(field), (int, float)):
            raise ValidationError(f"{field} must be numeric")
    if record["volume"] < 0 or record["quote_volume"] < 0:
        raise ValidationError("volume cannot be negative")
    if record["high"] < record["low"]:
        raise ValidationError("high cannot be lower than low")
    if not record["low"] <= record["open"] <= record["high"]:
        raise ValidationError("open is outside low/high range")
    if not record["low"] <= record["close"] <= record["high"]:
        raise ValidationError("close is outside low/high range")


def validate_news(record: dict[str, Any]) -> None:
    _required(record, ("source", "article_id", "title", "url", "published_at"))
    _valid_timestamp(record["published_at"], "published_at")
    _valid_timestamp(record["collected_at"], "collected_at")
