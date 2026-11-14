"""Pure DataFrame transformations used by the jobs. No IO here, so everything is unit-testable."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .common import contracts
from .common.validate import is_valid, validate_market, validate_news

KAFKA_COLUMNS = ("topic", "partition", "offset", "timestamp", "key", "value")
MARKET_FIELDS = tuple(contracts.properties("market_kline"))
NEWS_FIELDS = tuple(contracts.properties("news_article"))


def _iso(column: Column) -> Column:
    return F.date_format(column, contracts.TIMESTAMP_OUTPUT_FORMAT)


def kafka_strings(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows with key/value decoded as UTF-8 strings."""
    return kafka_df.select(
        "topic", "partition", "offset", "timestamp",
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
    )


# ---------------------------------------------------------------- raw zone

def raw_records(kafka_df: DataFrame, event_time_field: str) -> DataFrame:
    """Rows of contracts/v1/raw_record plus year/month/day partition columns.

    The Kafka value is kept verbatim. Partitions come from the event time inside the message
    (open_time / published_at); messages without a usable event time land in year=0/month=0/day=0.
    """
    decoded = kafka_strings(kafka_df)
    event_raw = F.get_json_object("value", f"$.{event_time_field}")
    pattern = contracts.timestamp_pattern("market_kline")
    event_ts = F.when(event_raw.rlike(pattern), F.to_timestamp(event_raw, contracts.TIMESTAMP_FORMAT))
    return decoded.select(
        "topic",
        "partition",
        "offset",
        _iso(F.col("timestamp")).alias("kafka_timestamp"),
        "key",
        "value",
        _iso(F.current_timestamp()).alias("ingested_at"),
        F.coalesce(F.year(event_ts), F.lit(0)).alias("year"),
        F.coalesce(F.month(event_ts), F.lit(0)).alias("month"),
        F.coalesce(F.dayofmonth(event_ts), F.lit(0)).alias("day"),
    )


# ---------------------------------------------------------------- 1m candles

def split_market(kafka_rows: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Validate decoded Kafka rows (string key/value) against market_kline: (valid, rejected)."""
    checked = validate_market(kafka_rows)
    return checked.where(is_valid()), checked.where(~is_valid())


def dedupe_candles(candles: DataFrame) -> DataFrame:
    """One row per (symbol, interval, open_time): the most recently collected one."""
    order = [F.col("collected_at").desc()]
    if "offset" in candles.columns:
        order.append(F.col("offset").desc())
    window = Window.partitionBy("symbol", "interval", "open_time").orderBy(*order)
    return candles.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def market_rows(valid: DataFrame) -> DataFrame:
    """Columns of pipeline.market_data, stamped with processed_at."""
    return dedupe_candles(valid).select(*MARKET_FIELDS, F.current_timestamp().alias("processed_at"))


def reject_rows(rejected: DataFrame) -> DataFrame:
    """Columns of pipeline.market_data_rejects."""
    return rejected.select(
        "topic",
        F.col("partition").cast("int").alias("partition"),
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        "key",
        F.col("value").alias("raw_value"),
        F.array_join("errors", "; ").alias("errors"),
        F.current_timestamp().alias("rejected_at"),
    )


def split_news(kafka_rows: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Validate decoded Kafka news rows (string key/value): (valid, rejected)."""
    checked = validate_news(kafka_rows)
    return checked.where(is_valid()), checked.where(~is_valid())


def dedupe_news(articles: DataFrame) -> DataFrame:
    """Keep the newest copy of each news article by its natural key."""
    order = [F.col("collected_at").desc()]
    if "offset" in articles.columns:
        order.append(F.col("offset").desc())
    window = Window.partitionBy("source", "article_id").orderBy(*order)
    return articles.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def news_rows(valid: DataFrame) -> DataFrame:
    """Columns of pipeline.news_data, without Kafka-only metadata or validation columns."""
    return dedupe_news(valid).select(*NEWS_FIELDS)


def news_reject_rows(rejected: DataFrame) -> DataFrame:
    """Columns of pipeline.news_data_rejects."""
    return rejected.select(
        "topic",
        F.col("partition").cast("int").alias("partition"),
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        "key",
        F.col("value").alias("raw_value"),
        F.array_join("errors", "; ").alias("errors"),
        F.current_timestamp().alias("rejected_at"),
    )


# ---------------------------------------------------------------- batch: 5m candles

def aggregate_ohlcv_5m(candles_1m: DataFrame) -> DataFrame:
    """1m candles -> 5m candles: open = first open, high = max, low = min, close = last close,
    volume / quote_volume / trade_count = sums. Buckets are aligned to :00, :05, ... UTC.

    Only complete buckets (exactly 5 one-minute candles) are returned, so a 5m candle is never
    built from partial data. Input must already be deduplicated.
    """
    grouped = candles_1m.where(F.col("interval") == "1m").groupBy(
        "symbol", F.window("open_time", "5 minutes").alias("bucket")
    )
    return (
        grouped.agg(
            F.min_by("open", "open_time").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", "open_time").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("quote_volume").alias("quote_volume"),
            F.sum("trade_count").alias("trade_count"),
            F.count(F.lit(1)).alias("candle_count"),
        )
        .where(F.col("candle_count") == 5)
        .select(
            "symbol",
            F.lit("5m").alias("interval"),
            F.col("bucket.start").alias("open_time"),
            (F.col("bucket.end") - F.expr("INTERVAL 1 MILLISECOND")).alias("close_time"),
            "open", "high", "low", "close", "volume", "quote_volume",
            F.col("trade_count").cast("long").alias("trade_count"),
            F.col("candle_count").cast("int").alias("candle_count"),
        )
    )


# ---------------------------------------------------------------- batch: indicators

def moving_average(candles: DataFrame, periods: int = 20, column: str = "close") -> DataFrame:
    """Simple moving average of `column` over the last `periods` candles (current included),
    per symbol and interval. NULL until `periods` candles are available."""
    window = Window.partitionBy("symbol", "interval").orderBy("open_time").rowsBetween(-(periods - 1), 0)
    return candles.select(
        "symbol",
        "interval",
        F.lit(f"ma{periods}").alias("name"),
        "open_time",
        F.when(F.count(column).over(window) == periods, F.avg(column).over(window)).alias("value"),
    )


# ---------------------------------------------------------------- batch: news impact

def calculate_news_impact(articles: DataFrame, candles: DataFrame) -> DataFrame:
    """Measure price movement after each article, without claiming the article caused it.

    Articles have source/article_id/title/symbol/published_ms. Candles have
    symbol/close_ms/close. For the baseline, use the last closed 1m candle before
    publication (at most 60s old). For each 5/15/60 minute horizon, use the first
    closed candle at or after that time (less than 60s late). Missing prices are
    skipped rather than silently comparing with a stale candle.
    """
    event_key = ["source", "article_id", "symbol"]
    articles = articles.dropDuplicates(event_key).where(F.col("symbol").isNotNull())
    candles = candles.where(F.col("close") > 0)

    before = articles.alias("a").join(
        candles.alias("c"),
        (F.col("a.symbol") == F.col("c.symbol"))
        & (F.col("c.close_ms") < F.col("a.published_ms"))
        & (F.col("c.close_ms") >= F.col("a.published_ms") - 60000),
        "inner",
    ).select(
        *[F.col(f"a.{name}").alias(name) for name in [*event_key, "title", "published_ms"]],
        F.col("c.close_ms").alias("baseline_ms"),
        F.col("c.close").alias("baseline_close"),
    )
    baseline_rank = Window.partitionBy(*event_key).orderBy(F.col("baseline_ms").desc())
    baseline = before.withColumn("_rn", F.row_number().over(baseline_rank)).where("_rn = 1").drop("_rn")

    horizons = baseline.withColumn("horizon_minutes", F.explode(F.array(F.lit(5), F.lit(15), F.lit(60))))
    after = horizons.alias("a").join(
        candles.alias("c"),
        (F.col("a.symbol") == F.col("c.symbol"))
        & (F.col("c.close_ms") >= F.col("a.published_ms") + F.col("a.horizon_minutes") * 60000)
        & (F.col("c.close_ms") < F.col("a.published_ms") + (F.col("a.horizon_minutes") + 1) * 60000),
        "inner",
    ).select(
        *[F.col(f"a.{name}").alias(name) for name in [*event_key, "title", "published_ms", "baseline_ms", "baseline_close", "horizon_minutes"]],
        F.col("c.close_ms").alias("target_ms"),
        F.col("c.close").alias("target_close"),
    )
    target_rank = Window.partitionBy(*event_key, "horizon_minutes").orderBy(F.col("target_ms").asc())
    matched = after.withColumn("_rn", F.row_number().over(target_rank)).where("_rn = 1").drop("_rn")

    return matched.select(
        *event_key,
        "title",
        F.timestamp_millis("published_ms").alias("published_at"),
        "horizon_minutes",
        F.timestamp_millis("baseline_ms").alias("baseline_time"),
        "baseline_close",
        F.timestamp_millis("target_ms").alias("target_time"),
        "target_close",
        ((F.col("target_close") / F.col("baseline_close") - 1) * 100).alias("return_pct"),
        F.current_timestamp().alias("calculated_at"),
    )


# ---------------------------------------------------------------- volume anomaly

def detect_volume_anomalies(history: DataFrame, current: DataFrame, periods: int = 20,
                            k: float = 3.0) -> DataFrame:
    """Signals for candles in `current` whose volume > mean + k * stddev of the `periods`
    candles immediately before it.

    history/current: symbol, open_time, volume (1m candles). `history` supplies the candles that
    precede the current batch (read from ClickHouse); rows present in both are taken from current.
    The baseline must be exactly `periods` consecutive minutes; if a candle is missing in that
    span no signal is emitted, rather than comparing against an older, non-contiguous window.
    stddev is the sample standard deviation (stddev_samp).
    """
    cols = ["symbol", "open_time", "volume"]
    both = history.select(*cols, F.lit(False).alias("is_current")).unionByName(
        current.select(*cols, F.lit(True).alias("is_current"))
    )
    pick = Window.partitionBy("symbol", "open_time").orderBy(F.col("is_current").desc())
    candles = both.withColumn("_rn", F.row_number().over(pick)).where("_rn = 1").drop("_rn")

    before = Window.partitionBy("symbol").orderBy("open_time").rowsBetween(-periods, -1)
    span_seconds = F.unix_timestamp("open_time") - F.unix_timestamp(F.min("open_time").over(before))
    scored = (
        candles
        .withColumn("baseline_count", F.count("volume").over(before))
        .withColumn("baseline_mean", F.avg("volume").over(before))
        .withColumn("baseline_stddev", F.stddev_samp("volume").over(before))
        .withColumn("contiguous", span_seconds == periods * 60)
        .withColumn("threshold", F.col("baseline_mean") + F.lit(k) * F.col("baseline_stddev"))
    )
    details = F.to_json(F.struct(
        F.lit(periods).alias("periods"), F.lit(k).alias("k"), F.lit("stddev_samp").alias("stddev"),
    ))
    return scored.where(
        F.col("is_current")
        & (F.col("baseline_count") == periods)
        & F.col("contiguous")
        & (F.col("volume") > F.col("threshold"))
    ).select(
        "symbol",
        F.lit("volume_anomaly").alias("signal_type"),
        "open_time",
        F.col("volume").alias("value"),
        "baseline_mean",
        "baseline_stddev",
        "threshold",
        details.alias("details"),
        F.current_timestamp().alias("detected_at"),
    )
