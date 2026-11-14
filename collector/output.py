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


def _pending_path(base_dir: str | Path, data_type: str) -> Path:
    """Return the durable outbox file next to the partitioned samples directory."""
    return Path(base_dir).parent / "outbox" / f"{data_type}.jsonl"


def read_pending_records(base_dir: str | Path, data_type: str) -> list[dict[str, Any]]:
    """Read records that were published unsuccessfully or before a previous crash."""
    source = _pending_path(base_dir, data_type)
    if not source.exists():
        return []

    records: list[dict[str, Any]] = []
    with source.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                LOGGER.warning("Ignoring malformed pending record in %s", source)
                continue
            if isinstance(value, dict):
                records.append(value)
            else:
                LOGGER.warning("Ignoring non-object pending record in %s", source)
    return records


def write_pending_records(records: Iterable[dict[str, Any]], base_dir: str | Path, data_type: str) -> None:
    """Atomically replace the outbox with the records still waiting for Kafka."""
    destination = _pending_path(base_dir, data_type)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    write_jsonl(records, temporary)
    temporary.replace(destination)


def clear_pending_records(base_dir: str | Path, data_type: str) -> None:
    _pending_path(base_dir, data_type).unlink(missing_ok=True)


def _record_key(record: dict[str, Any], data_type: str) -> str:
    if data_type == "market":
        return f"{record['source']}:{record['symbol']}:{record['interval']}:{record['open_time']}"
    return f"{record['source']}:{record['article_id']}"


def _record_date(record: dict[str, Any], data_type: str) -> str:
    field = "open_time" if data_type == "market" else "published_at"
    value = datetime.fromisoformat(record[field].replace("Z", "+00:00"))
    return value.strftime("%Y-%m-%d")


def _existing_keys(destination: Path, data_type: str) -> set[str]:
    existing_keys: set[str] = set()
    if not destination.exists():
        return existing_keys

    with destination.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            try:
                existing_keys.add(_record_key(json.loads(line), data_type))
            except (json.JSONDecodeError, KeyError):
                LOGGER.warning("Ignoring malformed line in %s", destination)
    return existing_keys


def find_new_records(
    records: Iterable[dict[str, Any]], base_dir: str | Path, data_type: str
) -> tuple[list[dict[str, Any]], int]:
    """Return records not already present in the partitioned local store.

    This is deliberately separate from writing. When Kafka publishing is enabled, the
    collector uses it to identify the outbox candidates, publishes them first, and only
    marks them as stored after Kafka acknowledges them.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(_record_date(record, data_type), []).append(record)

    new_records: list[dict[str, Any]] = []
    skipped = 0
    for day, day_records in grouped.items():
        year, month, day_number = day.split("-")
        destination = Path(base_dir) / data_type / year / month / f"{day_number}.jsonl"
        existing_keys = _existing_keys(destination, data_type)
        for record in day_records:
            key = _record_key(record, data_type)
            if key in existing_keys:
                skipped += 1
                continue
            existing_keys.add(key)
            new_records.append(record)
    return new_records, skipped


def write_partitioned_jsonl(
    records: Iterable[dict[str, Any]], base_dir: str | Path, data_type: str
) -> tuple[list[dict[str, Any]], int]:
    """Append records to data_type/year/month/day.jsonl without duplicates.

    Returns (written_records, skipped_existing_count). Existing lines are preserved, and only
    the returned records are new, so they are the ones safe to forward downstream.
    """
    new_records, skipped = find_new_records(records, base_dir, data_type)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in new_records:
        grouped.setdefault(_record_date(record, data_type), []).append(record)

    for day, day_records in grouped.items():
        year, month, day_number = day.split("-")
        destination = Path(base_dir) / data_type / year / month / f"{day_number}.jsonl"
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("a", encoding="utf-8") as file:
            for record in day_records:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
    return new_records, skipped
