"""Spark view of the shared data contract (contracts/v1, vendored from the infrastructure repo).

Field names, types, patterns, enums and bounds all come from the JSON Schema files, so Spark
never redefines the message format.
"""

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from pyspark.sql import types as T

CONTRACT_DIR = Path(__file__).resolve().parents[2] / "contracts" / "v1"

# Explicit Spark pattern for the contract's utc_ms format (2026-10-27T08:00:00.000Z).
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
# Same format for writing timestamps back out as strings (session timezone is UTC).
TIMESTAMP_OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"


@lru_cache(maxsize=None)
def load_schema(name: str) -> dict[str, Any]:
    return json.loads((CONTRACT_DIR / f"{name}.schema.json").read_text(encoding="utf-8"))


def properties(name: str) -> dict[str, dict[str, Any]]:
    """Contract properties with local $ref resolved."""
    schema = load_schema(name)
    resolved = {}
    for field, spec in schema["properties"].items():
        if "$ref" in spec:
            target = spec["$ref"].removeprefix("#/$defs/")
            spec = {**schema["$defs"][target], **{k: v for k, v in spec.items() if k != "$ref"}}
        resolved[field] = spec
    return resolved


def timestamp_fields(name: str) -> list[str]:
    schema = load_schema(name)
    pattern = schema["$defs"]["utc_ms"]["pattern"]
    return [field for field, spec in properties(name).items() if spec.get("pattern") == pattern]


def timestamp_pattern(name: str) -> str:
    return load_schema(name)["$defs"]["utc_ms"]["pattern"]


def _spark_type(spec: dict[str, Any]) -> T.DataType:
    json_type = spec.get("type")
    if json_type is None:
        sample = spec["const"] if "const" in spec else spec["enum"][0]
        json_type = "string" if isinstance(sample, str) else "number"
    if isinstance(json_type, list):  # e.g. ["string", "null"]
        json_type = next(t for t in json_type if t != "null")
    if json_type == "array":
        return T.ArrayType(_spark_type(spec["items"]))
    return {
        "string": T.StringType(),
        "number": T.DoubleType(),
        "integer": T.LongType(),
        "boolean": T.BooleanType(),
    }[json_type]


def spark_schema(name: str) -> T.StructType:
    """StructType for from_json. Timestamps stay strings here; they are parsed after validation."""
    return T.StructType([T.StructField(field, _spark_type(spec), True) for field, spec in properties(name).items()])


def required_fields(name: str) -> list[str]:
    return list(load_schema(name)["required"])
