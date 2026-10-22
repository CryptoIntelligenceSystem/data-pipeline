from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


def _csv(name: str, default: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in os.getenv(name, default).split(",") if item.strip())


@dataclass(frozen=True)
class Settings:
    binance_base_url: str = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com")
    symbols: tuple[str, ...] = _csv("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT")
    interval: str = os.getenv("BINANCE_INTERVAL", "1m")
    limit: int = int(os.getenv("BINANCE_LIMIT", "100"))
    coindesk_rss_url: str = os.getenv("COINDESK_RSS_URL", "https://www.coindesk.com/arc/outboundfeeds/rss/")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    market_topic: str = os.getenv("KAFKA_MARKET_TOPIC", "market_data")
    news_topic: str = os.getenv("KAFKA_NEWS_TOPIC", "news_data")
    output_dir: str = os.getenv("COLLECTOR_OUTPUT_DIR", "data/samples")
