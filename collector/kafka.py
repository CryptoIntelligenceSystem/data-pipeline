import json
import logging
from typing import Any, Iterable

LOGGER = logging.getLogger(__name__)


def _key(record: dict[str, Any]) -> bytes:
    return str(record.get("symbol") or record.get("article_id") or "").encode("utf-8")


def publish(records: Iterable[dict[str, Any]], topic: str, bootstrap_servers: str,
            timeout: float = 30) -> int:
    records = list(records)
    if not records:
        LOGGER.info("No new records to publish to %s", topic)
        return 0
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt before using --send-kafka") from exc
    try:
        producer = KafkaProducer(
            bootstrap_servers=[server.strip() for server in bootstrap_servers.split(",")],
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            acks="all",
        )
    except KafkaError as exc:
        raise RuntimeError(f"cannot connect to Kafka at {bootstrap_servers}: {exc}") from exc

    failed = 0
    try:
        futures = [producer.send(topic, key=_key(record), value=record) for record in records]
        producer.flush(timeout=timeout)
        for future in futures:
            try:
                future.get(timeout=timeout)
            except KafkaError as exc:
                failed += 1
                LOGGER.error("Kafka delivery to %s failed: %s", topic, exc)
    finally:
        producer.close()
    if failed:
        raise RuntimeError(f"{failed}/{len(records)} records were not delivered to {topic}")
    LOGGER.info("Published %d records to %s", len(records), topic)
    return len(records)
