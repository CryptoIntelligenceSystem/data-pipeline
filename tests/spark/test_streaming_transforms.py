import json

import pytest
from pyspark.sql import functions as F

from spark_jobs.common import contracts
from spark_jobs.transforms import detect_volume_anomalies, raw_records

from .conftest import candles_df, kafka_rows, market_message

jsonschema = pytest.importorskip("jsonschema")


def _volumes(spark, symbol_minute_volume):
    rows = [(s, m, 1, 1, 1, 1, v) for s, m, v in symbol_minute_volume]
    return candles_df(spark, rows).select("symbol", "open_time", "volume")


# History: 20 candles (minutes 0..19) alternating 10, 12.
# Hand check: mean = 11; sample stddev = sqrt(20 * 1^2 / 19) = 1.025978...;
# threshold = 11 + 3 * 1.025978 = 14.077935.
HISTORY = [("BTCUSDT", m, 10 if m % 2 == 0 else 12) for m in range(20)]


def test_volume_anomaly_hand_calculated(spark):
    current = _volumes(spark, [("BTCUSDT", 20, 15.0)])
    signals = detect_volume_anomalies(_volumes(spark, HISTORY), current, 20, 3.0).collect()
    assert len(signals) == 1
    signal = signals[0]
    assert signal["signal_type"] == "volume_anomaly" and signal["value"] == 15.0
    assert signal["baseline_mean"] == pytest.approx(11.0)
    assert signal["baseline_stddev"] == pytest.approx(1.0259783520851542)
    assert signal["threshold"] == pytest.approx(14.077935056255463)
    assert json.loads(signal["details"]) == {"periods": 20, "k": 3.0, "stddev": "stddev_samp"}


def test_volume_below_threshold_is_not_a_signal(spark):
    current = _volumes(spark, [("BTCUSDT", 20, 14.0)])
    assert detect_volume_anomalies(_volumes(spark, HISTORY), current).count() == 0


def test_needs_full_contiguous_baseline(spark):
    # Only 19 previous candles -> no signal.
    current = _volumes(spark, [("BTCUSDT", 20, 1000.0)])
    assert detect_volume_anomalies(_volumes(spark, HISTORY[1:]), current).count() == 0
    # 20 previous candles but one minute missing (gap at minute 5, extra one at -1) -> no signal.
    gappy = [row for row in HISTORY if row[1] != 5] + [("BTCUSDT", -1, 11)]
    assert detect_volume_anomalies(_volumes(spark, gappy), current).count() == 0


def test_batch_rows_extend_the_baseline_and_only_batch_rows_are_signalled(spark):
    # History has a spike at minute 10 that is not part of this batch: it must not be re-emitted.
    history = [("BTCUSDT", m, 500 if m == 10 else 11) for m in range(10, 30)]
    # Batch: minutes 30..31; minute 31's baseline = minutes 11..30, all from history+batch.
    current = _volumes(spark, [("BTCUSDT", 30, 11.0), ("BTCUSDT", 31, 100.0)])
    signals = detect_volume_anomalies(_volumes(spark, history), current).select(F.minute("open_time")).collect()
    assert [row[0] for row in signals] == [31]


def test_raw_records_follow_contract_and_partition_by_event_date(spark):
    kafka = kafka_rows(spark, [market_message(), "not json"]).withColumn("key", F.col("key").cast("binary")) \
        .withColumn("value", F.col("value").cast("binary"))
    rows = raw_records(kafka, "open_time").orderBy("offset").collect()

    validator = jsonschema.Draft202012Validator(contracts.load_schema("raw_record"))
    for row in rows:
        record = {k: row[k] for k in ("topic", "partition", "offset", "kafka_timestamp", "key", "value", "ingested_at")}
        assert list(validator.iter_errors(record)) == []

    good, bad = rows
    assert (good["year"], good["month"], good["day"]) == (2026, 10, 27)
    assert json.loads(good["value"]) == market_message()          # value kept verbatim
    assert good["kafka_timestamp"] == "2026-10-27T08:00:00.000Z"
    assert (bad["year"], bad["month"], bad["day"]) == (0, 0, 0)
    assert bad["value"] == "not json"
