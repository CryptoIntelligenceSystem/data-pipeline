import argparse
import logging
from pathlib import Path
from datetime import datetime

from .collectors import BinanceCollector, CoinDeskCollector
from .config import Settings
from .kafka import publish
from .output import validate_and_deduplicate, write_partitioned_jsonl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect and normalize market/news data")
    parser.add_argument("--source", choices=("market", "news", "all"), default="all")
    parser.add_argument("--once", action="store_true", help="Collect one batch and exit")
    parser.add_argument("--send-kafka", action="store_true", help="Publish valid records to Kafka")
    parser.add_argument("--start", help="Historical start time in UTC ISO-8601, inclusive")
    parser.add_argument("--end", help="Historical end time in UTC ISO-8601, exclusive")
    return parser.parse_args()


def _validate_range(args: argparse.Namespace) -> None:
    if bool(args.start) != bool(args.end):
        raise SystemExit("--start and --end must be provided together")
    if args.start and args.end:
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
        end = datetime.fromisoformat(args.end.replace("Z", "+00:00"))
        if start.tzinfo is None or end.tzinfo is None:
            raise SystemExit("--start and --end must include timezone, for example ...Z")
        if start >= end:
            raise SystemExit("--start must be earlier than --end")


def _in_range(value: str, start: str | None, end: str | None) -> bool:
    if not start or not end:
        return True
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(start.replace("Z", "+00:00")) <= timestamp < datetime.fromisoformat(end.replace("Z", "+00:00"))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    _validate_range(args)
    settings = Settings()
    output_dir = Path(settings.output_dir)
    if args.source in ("market", "all"):
        collector = BinanceCollector(settings.binance_base_url)
        records = []
        for symbol in settings.symbols:
            try:
                if args.start and args.end:
                    batch = collector.collect_klines_range(
                        symbol, settings.interval, args.start, args.end, settings.limit
                    )
                else:
                    batch = collector.collect_klines(symbol, settings.interval, settings.limit)
                records.extend(record.to_dict() for record in batch)
            except RuntimeError as exc:
                logging.error("Could not collect %s: %s", symbol, exc)
        valid = validate_and_deduplicate(records, "market")
        written, skipped = write_partitioned_jsonl(valid, output_dir, "market")
        if args.send_kafka:
            publish(valid, settings.market_topic, settings.kafka_bootstrap_servers)
        logging.info("Market records written: %d; duplicates skipped: %d", written, skipped)
    if args.source in ("news", "all"):
        try:
            records = [record.to_dict() for record in CoinDeskCollector(settings.coindesk_rss_url).collect()]
            records = [record for record in records if _in_range(record["published_at"], args.start, args.end)]
        except RuntimeError as exc:
            logging.error("Could not collect CoinDesk news: %s", exc)
            records = []
        valid = validate_and_deduplicate(records, "news")
        written, skipped = write_partitioned_jsonl(valid, output_dir, "news")
        if args.send_kafka:
            publish(valid, settings.news_topic, settings.kafka_bootstrap_servers)
        logging.info("News records written: %d; duplicates skipped: %d", written, skipped)


if __name__ == "__main__":
    main()
