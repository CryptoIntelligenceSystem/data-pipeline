import json
import logging
from typing import Any, Iterable

LOGGER = logging.getLogger(__name__)


def publish(records: Iterable[dict[str, Any]], topic: str, bootstrap_servers: str) -> int:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt before using --send-kafka") from exc
    producer = KafkaProducer(
        bootstrap_servers=[server.strip() for server in bootstrap_servers.split(",")],
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )
    count = 0
    for record in records:
        key = record.get("symbol", record.get("article_id", "")).encode()
        producer.send(topic, key=key, value=record)
        count += 1
    producer.flush()
    producer.close()
    LOGGER.info("Published %d records to %s", count, topic)
    return count
