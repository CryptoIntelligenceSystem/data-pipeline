import argparse
import logging
import time
from pathlib import Path
from datetime import datetime

from .collectors import BinanceCollector, CoinDeskCollector
from .config import Settings
from .kafka import publish
from .output import validate_and_deduplicate, write_partitioned_jsonl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect and normalize market/news data")
    parser.add_argument("--source", choices=("market", "news", "all"), default="all")
    parser.add_argument("--once", action="store_true",
                        help="Collect one batch and exit (default: poll every COLLECTOR_POLL_SECONDS)")
    parser.add_argument("--send-kafka", action="store_true", help="Publish new valid records to Kafka")
    parser.add_argument("--start", help="Historical start time in UTC ISO-8601, inclusive")
    parser.add_argument("--end", help="Historical end time in UTC ISO-8601, exclusive")
    return parser.parse_args()


def _validate_range(args: argparse.Namespace) -> None:
    if bool(args.start) != bool(args.end):
        raise SystemExit("--start and --end must be provided together")
    if args.start and args.end:
        try:
            start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
            end = datetime.fromisoformat(args.end.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SystemExit(f"invalid --start/--end: {exc}") from exc
        if start.tzinfo is None or end.tzinfo is None:
            raise SystemExit("--start and --end must include timezone, for example ...Z")
        if start >= end:
            raise SystemExit("--start must be earlier than --end")


def _in_range(value: str, start: str | None, end: str | None) -> bool:
    if not start or not end:
        return True
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(start.replace("Z", "+00:00")) <= timestamp < datetime.fromisoformat(end.replace("Z", "+00:00"))


def _store(records: list[dict], data_type: str, topic: str, args: argparse.Namespace, settings: Settings) -> None:
    valid = validate_and_deduplicate(records, data_type)
    written, skipped = write_partitioned_jsonl(valid, Path(settings.output_dir), data_type)
    logging.info("%s records written: %d; duplicates skipped: %d", data_type.capitalize(), len(written), skipped)
    if args.send_kafka:
        # Only records new to the local store are sent, so reruns do not replay old data into Kafka.
        try:
            publish(written, topic, settings.kafka_bootstrap_servers)
        except RuntimeError as exc:
            logging.error("Could not publish %s records to Kafka: %s", data_type, exc)


def collect_market(args: argparse.Namespace, settings: Settings) -> None:
    collector = BinanceCollector(settings.binance_base_url)
    records = []
    for symbol in settings.symbols:
        try:
            if args.start and args.end:
                batch = collector.collect_klines_range(
                    symbol, settings.interval, args.start, args.end, settings.history_limit
                )
            else:
                batch = collector.collect_klines(symbol, settings.interval, settings.limit)
            records.extend(record.to_dict() for record in batch)
        except (RuntimeError, ValueError) as exc:
            logging.error("Could not collect %s: %s", symbol, exc)
    _store(records, "market", settings.market_topic, args, settings)


def collect_news(args: argparse.Namespace, settings: Settings) -> None:
    if args.start and args.end:
        logging.warning("CoinDesk RSS only lists recent articles; --start/--end just filters that list")
    try:
        collector = CoinDeskCollector(settings.coindesk_rss_url, symbols=settings.symbols)
        records = [record.to_dict() for record in collector.collect()]
        records = [record for record in records if _in_range(record["published_at"], args.start, args.end)]
    except RuntimeError as exc:
        logging.error("Could not collect CoinDesk news: %s", exc)
        records = []
    _store(records, "news", settings.news_topic, args, settings)


def run_once(args: argparse.Namespace, settings: Settings) -> None:
    # Each source is isolated so a failure in one never prevents the other from running.
    if args.source in ("market", "all"):
        try:
            collect_market(args, settings)
        except Exception:
            logging.exception("Market collection failed")
    if args.source in ("news", "all"):
        try:
            collect_news(args, settings)
        except Exception:
            logging.exception("News collection failed")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    _validate_range(args)
    settings = Settings()
    if args.once or (args.start and args.end):
        run_once(args, settings)
        return
    logging.info("Polling every %ds; press Ctrl+C to stop", settings.poll_seconds)
    try:
        while True:
            started = time.monotonic()
            run_once(args, settings)
            time.sleep(max(0.0, settings.poll_seconds - (time.monotonic() - started)))
    except KeyboardInterrupt:
        logging.info("Stopped")


if __name__ == "__main__":
    main()
