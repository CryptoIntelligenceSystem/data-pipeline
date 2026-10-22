import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .validation import ValidationError, validate_market, validate_news

LOGGER = logging.getLogger(__name__)


def validate_and_deduplicate(records: Iterable[dict[str, Any]], data_type: str) -> list[dict[str, Any]]:
    valid: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in records:
        try:
            if data_type == "market":
                validate_market(record)
                key = f"{record['source']}:{record['symbol']}:{record['interval']}:{record['open_time']}"
            else:
                validate_news(record)
                key = f"{record['source']}:{record['article_id']}"
        except ValidationError as exc:
            LOGGER.error("Invalid %s record: %s", data_type, exc)
            continue
        if key in seen:
            LOGGER.warning("Duplicate %s record skipped: %s", data_type, key)
            continue
        seen.add(key)
        valid.append(record)
    return valid


def write_jsonl(records: Iterable[dict[str, Any]], path: str | Path) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def _record_key(record: dict[str, Any], data_type: str) -> str:
    if data_type == "market":
        return f"{record['source']}:{record['symbol']}:{record['interval']}:{record['open_time']}"
    return f"{record['source']}:{record['article_id']}"


def _record_date(record: dict[str, Any], data_type: str) -> str:
    field = "open_time" if data_type == "market" else "published_at"
    value = datetime.fromisoformat(record[field].replace("Z", "+00:00"))
    return value.strftime("%Y-%m-%d")


def write_partitioned_jsonl(
    records: Iterable[dict[str, Any]], base_dir: str | Path, data_type: str
) -> tuple[int, int]:
    """Append records to data_type/year/month/day.jsonl without duplicates.

    Returns (written_count, skipped_existing_count). Existing lines are preserved.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(_record_date(record, data_type), []).append(record)

    written = 0
    skipped = 0
    for day, day_records in grouped.items():
        year, month, day_number = day.split("-")
        destination = Path(base_dir) / data_type / year / month / f"{day_number}.jsonl"
        destination.parent.mkdir(parents=True, exist_ok=True)
        existing_keys: set[str] = set()
        if destination.exists():
            with destination.open("r", encoding="utf-8") as file:
                for line in file:
                    if not line.strip():
                        continue
                    try:
                        existing_keys.add(_record_key(json.loads(line), data_type))
                    except (json.JSONDecodeError, KeyError):
                        LOGGER.warning("Ignoring malformed line in %s", destination)

        with destination.open("a", encoding="utf-8") as file:
            for record in day_records:
                key = _record_key(record, data_type)
                if key in existing_keys:
                    skipped += 1
                    continue
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
                existing_keys.add(key)
                written += 1
    return written, skipped
