import json

import pytest
from pyspark.sql import functions as F

from spark_jobs.common import contracts
from spark_jobs.transforms import split_market

from .conftest import kafka_rows, market_message


def _errors(spark, value) -> list[str]:
    valid, rejected = split_market(kafka_rows(spark, [value]))
    rows = rejected.select("errors").collect()
    assert valid.count() + len(rows) == 1
    return rows[0]["errors"] if rows else []


def test_spark_schema_mirrors_contract():
    schema = contracts.spark_schema("market_kline")
    assert schema.fieldNames() == list(contracts.load_schema("market_kline")["properties"])
    types = {f.name: f.dataType.simpleString() for f in schema.fields}
    assert types["open"] == "double" and types["trade_count"] == "bigint" and types["open_time"] == "string"


def test_contract_example_is_valid(spark):
    example = json.loads((contracts.CONTRACT_DIR / "examples" / "market_kline.valid.json").read_text())
    assert _errors(spark, example) == []


def test_contract_invalid_example_is_rejected_with_reasons(spark):
    example = json.loads((contracts.CONTRACT_DIR / "examples" / "market_kline.invalid.json").read_text())
    errors = _errors(spark, example)
    assert "missing:close" in errors
    assert "type:trade_count" in errors           # "2381" is a string
    assert "range:volume" in errors               # -1
    assert "pattern:open_time" in errors          # "2026-10-27 08:00:00"


@pytest.mark.parametrize(("overrides", "expected"), [
    ({"volume": "152.4"}, "type:volume"),
    ({"open": 0}, "range:open"),
    ({"quote_volume": -0.5}, "range:quote_volume"),
    ({"trade_count": 1.5}, "type:trade_count"),
    ({"symbol": "btc-usdt"}, "pattern:symbol"),
    ({"interval": "5m"}, "enum:interval"),
    ({"source": "kraken"}, "enum:source"),
    ({"data_type": "news"}, "const:data_type"),
    ({"open_time": "2026-10-27T08:00:00Z"}, "pattern:open_time"),
    ({"open_time": "2026-10-27T15:00:00.000+07:00"}, "pattern:open_time"),
    ({"high": 1.0}, "rule:high_lt_low"),
    ({"open": 999999.0}, "rule:open_outside_range"),
    ({"close": 1.0}, "rule:close_outside_range"),
    ({"close_time": "2026-10-27T07:00:00.000Z"}, "rule:close_time_not_after_open_time"),
    ({"exchange": "binance"}, "unexpected:exchange"),
])
def test_rule_violations_are_reported(spark, overrides, expected):
    assert expected in _errors(spark, market_message(**overrides))


def test_missing_field(spark):
    message = market_message()
    del message["volume"]
    assert "missing:volume" in _errors(spark, message)


def test_malformed_json_nan_and_infinity(spark):
    assert _errors(spark, '{"symbol": "BTCUSDT", ') == ["malformed_json"]
    message = json.dumps(market_message())
    # NaN is not valid JSON (Python's json.dumps can still emit it): rejected as malformed.
    assert _errors(spark, message.replace('"volume": 152.431', '"volume": NaN')) == ["malformed_json"]
    # 1e400 is valid JSON but overflows a double to Infinity.
    assert "not_finite:volume" in _errors(spark, message.replace('"volume": 152.431', '"volume": 1e400'))


def test_rejects_keep_raw_value_and_kafka_metadata(spark):
    from spark_jobs.transforms import reject_rows

    _, rejected = split_market(kafka_rows(spark, [market_message(volume=-1)]))
    row = reject_rows(rejected).first()
    assert row["topic"] == "market_data" and row["offset"] == 0
    assert '"volume": -1' in row["raw_value"]
    assert row["errors"] == "range:volume"


def test_timestamps_parse_as_utc_with_explicit_format(spark):
    # Hand check: 2026-10-27T08:00:00.123Z = 1793088000123 ms since epoch, whatever the machine timezone.
    valid, _ = split_market(kafka_rows(spark, [market_message(open_time="2026-10-27T08:00:00.123Z")]))
    row = valid.select(
        F.expr("unix_millis(open_time)").alias("ms"),
        F.date_format("open_time", contracts.TIMESTAMP_OUTPUT_FORMAT).alias("text"),
    ).first()
    assert row["ms"] == 1793088000123
    assert row["text"] == "2026-10-27T08:00:00.123Z"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
