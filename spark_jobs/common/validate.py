"""Contract validation of raw Kafka values, done entirely with Spark SQL expressions (no Python UDF).

Each record gets an ``errors`` array of short codes such as ``missing:close``, ``type:volume``,
``range:volume``, ``pattern:open_time``, ``rule:high_lt_low``. Records with errors go to the
dead-letter table instead of being dropped silently.
"""

import math

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from . import contracts

PARSED = "_parsed"


def _field_checks(name: str) -> list[Column]:
    value = F.col("value")
    checks: list[Column] = []
    for field, spec in contracts.properties(name).items():
        raw = F.get_json_object(value, f"$.{field}")
        parsed = F.col(f"{PARSED}.{field}")
        if field in contracts.required_fields(name):
            # Check key presence instead of raw == NULL so a required nullable field such as
            # news.sentiment may legally be present with an explicit JSON null value.
            present = F.array_contains(F.expr("json_object_keys(value)"), F.lit(field))
            checks.append(F.when(~present, F.lit(f"missing:{field}")))
        # Present in the JSON but from_json could not read it as the contract type.
        checks.append(F.when(raw.isNotNull() & parsed.isNull(), F.lit(f"type:{field}")))
        if "const" in spec:
            checks.append(F.when(parsed != F.lit(spec["const"]), F.lit(f"const:{field}")))
        if "enum" in spec:
            checks.append(F.when(~parsed.isin(*spec["enum"]), F.lit(f"enum:{field}")))
        if "pattern" in spec:
            checks.append(F.when(~parsed.rlike(spec["pattern"]), F.lit(f"pattern:{field}")))
        if spec.get("minLength"):
            checks.append(F.when(F.length(parsed) < spec["minLength"], F.lit(f"length:{field}")))
        if spec.get("type") == "array":
            item_spec = spec.get("items", {})
            if item_spec.get("pattern"):
                checks.append(F.when(
                    F.exists(parsed, lambda item: ~item.rlike(item_spec["pattern"])),
                    F.lit(f"pattern:{field}.items"),
                ))
            if spec.get("uniqueItems"):
                checks.append(F.when(
                    F.size(parsed) != F.size(F.array_distinct(parsed)),
                    F.lit(f"unique:{field}"),
                ))
        if spec.get("type") in ("number", "integer"):
            not_finite = F.isnan(parsed) | parsed.isin(math.inf, -math.inf)
            checks.append(F.when(not_finite, F.lit(f"not_finite:{field}")))
            if "minimum" in spec:
                checks.append(F.when(parsed < spec["minimum"], F.lit(f"range:{field}")))
            if "exclusiveMinimum" in spec:
                checks.append(F.when(parsed <= spec["exclusiveMinimum"], F.lit(f"range:{field}")))
    return checks


def _unexpected_fields(name: str) -> Column:
    if contracts.load_schema(name).get("additionalProperties", True):
        return F.lit(None)
    allowed = F.array(*[F.lit(field) for field in contracts.properties(name)])
    extra = F.array_except(F.expr("json_object_keys(value)"), allowed)
    return F.when(F.size(extra) > 0, F.concat(F.lit("unexpected:"), F.array_join(extra, ",")))


def _with_errors(df: DataFrame, name: str, rule_checks: list[Column]) -> DataFrame:
    parsed = df.withColumn(PARSED, F.from_json("value", contracts.spark_schema(name)))
    malformed = F.get_json_object(F.col("value"), "$").isNull()
    checks = _field_checks(name) + [_unexpected_fields(name)] + rule_checks
    errors = F.filter(F.array(*checks), lambda error: error.isNotNull())
    return parsed.withColumn(
        "errors",
        F.when(F.col("value").isNull() | malformed, F.array(F.lit("malformed_json"))).otherwise(errors),
    )


def _typed_fields(name: str) -> list[Column]:
    """Contract fields as columns; contract timestamps become TimestampType (UTC)."""
    timestamps = set(contracts.timestamp_fields(name))
    pattern = contracts.timestamp_pattern(name)
    columns = []
    for field in contracts.properties(name):
        column = F.col(f"{PARSED}.{field}")
        if field in timestamps:
            # Parse only canonical strings, with an explicit format, in the UTC session.
            column = F.when(column.rlike(pattern), F.to_timestamp(column, contracts.TIMESTAMP_FORMAT))
        columns.append(column.alias(field))
    return columns


def validate_market(df: DataFrame) -> DataFrame:
    """df needs a string column ``value`` (the Kafka value); other columns are kept.

    Adds the market_kline fields (typed) and ``errors``; a record is valid when errors is empty.
    """
    p = lambda field: F.col(f"{PARSED}.{field}")  # noqa: E731
    open_ts = F.to_timestamp(p("open_time"), contracts.TIMESTAMP_FORMAT)
    close_ts = F.to_timestamp(p("close_time"), contracts.TIMESTAMP_FORMAT)
    rules = [
        F.when(p("high") < p("low"), F.lit("rule:high_lt_low")),
        F.when((p("open") < p("low")) | (p("open") > p("high")), F.lit("rule:open_outside_range")),
        F.when((p("close") < p("low")) | (p("close") > p("high")), F.lit("rule:close_outside_range")),
        F.when(close_ts <= open_ts, F.lit("rule:close_time_not_after_open_time")),
    ]
    checked = _with_errors(df, "market_kline", rules)
    return checked.select(*[c for c in df.columns], *_typed_fields("market_kline"), "errors")


def validate_news(df: DataFrame) -> DataFrame:
    """Validate decoded Kafka rows against the news_article contract.

    News has no additional cross-field business rule in v1; the shared checks cover required
    fields, types, constants, patterns, array items and duplicate symbols.
    """
    checked = _with_errors(df, "news_article", [])
    return checked.select(*[c for c in df.columns], *_typed_fields("news_article"), "errors")


def is_valid() -> Column:
    return F.size("errors") == 0
