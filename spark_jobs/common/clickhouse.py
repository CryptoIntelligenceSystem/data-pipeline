"""ClickHouse access through JDBC and HTTP JSONEachRow."""

import base64
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import Settings


def _options(settings: Settings) -> dict[str, str]:
    return {
        "url": settings.clickhouse_jdbc_url,
        "driver": settings.clickhouse_jdbc_driver,
        "user": settings.clickhouse_user,
        "password": settings.clickhouse_password,
    }


def write(df: DataFrame, table: str, settings: Settings, batch_size: int = 5000) -> None:
    """Append rows. Tables are ReplacingMergeTree, so a replayed batch collapses into the same rows.

    Column names of df must exist in the table; columns not in df take their table default.
    """
    (
        df.write.format("jdbc")
        .options(**_options(settings))
        .option("dbtable", table)
        .option("batchsize", str(batch_size))
        # ClickHouse has no transactions; anything else makes Spark call setTransactionIsolation.
        .option("isolationLevel", "NONE")
        .mode("append")
        .save()
    )


def write_json_each_row(
    df: DataFrame,
    table: str,
    settings: Settings,
    batch_size: int = 500,
) -> None:
    """Write rows through ClickHouse HTTP JSONEachRow.

    Spark's generic JDBC writer cannot map ArrayType columns such as News.symbols.
    JSONEachRow preserves arrays and nullable values without changing the ClickHouse
    schema. The destination tables are ReplacingMergeTree tables, so executor retries
    remain safe when consumers query with FINAL.
    """
    if not re.fullmatch(r"[A-Za-z0-9_]+", table):
        raise ValueError(f"invalid ClickHouse table name: {table}")

    query = f"INSERT INTO {table} FORMAT JSONEachRow"
    endpoint = (
        f"{settings.clickhouse_http_url.rstrip('/')}?"
        f"{urlencode({'database': settings.clickhouse_database, 'query': query})}"
    )
    token = base64.b64encode(
        f"{settings.clickhouse_user}:{settings.clickhouse_password}".encode("utf-8")
    ).decode("ascii")

    def write_partition(rows) -> None:
        batch: list[str] = []

        def flush() -> None:
            if not batch:
                return
            request = Request(
                endpoint,
                data=("\n".join(batch) + "\n").encode("utf-8"),
                method="POST",
                headers={
                    "Authorization": f"Basic {token}",
                    "Content-Type": "application/json",
                },
            )
            try:
                with urlopen(request, timeout=60) as response:
                    if response.status >= 300:
                        body = response.read().decode("utf-8", errors="replace")
                        raise RuntimeError(
                            f"ClickHouse HTTP insert failed ({response.status}): {body}"
                        )
            except HTTPError as error:
                body = error.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"ClickHouse HTTP insert failed ({error.code}): {body}"
                ) from error
            batch.clear()

        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                flush()
        flush()

    json_df = _json_ready(df)
    json_df.toJSON().foreachPartition(write_partition)


def _json_ready(df: DataFrame) -> DataFrame:
    """Convert Spark timestamp columns to ClickHouse's JSON datetime format."""
    # ClickHouse DateTime64 accepts a space-separated datetime more reliably than
    # Spark's default ISO JSON representation (which contains ``T`` and ``Z``).
    # Keep arrays and nullable values untouched; only TimestampType columns need
    # this transport-level conversion.
    json_df = df
    for field in df.schema.fields:
        if isinstance(field.dataType, T.TimestampType):
            json_df = json_df.withColumn(
                field.name,
                F.date_format(F.col(field.name), "yyyy-MM-dd HH:mm:ss.SSS"),
            )

    return json_df


def read_query(spark: SparkSession, settings: Settings, query: str) -> DataFrame:
    """Run a SELECT. Prefer plain types in the query (toString, toFloat64, epoch ms) over
    ClickHouse-specific ones such as LowCardinality or DateTime64, which JDBC maps less reliably."""
    return spark.read.format("jdbc").options(**_options(settings)).option("query", query).load()
