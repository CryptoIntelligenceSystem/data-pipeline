"""ClickHouse access through the JDBC driver baked into the Spark image."""

from pyspark.sql import DataFrame, SparkSession

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


def read_query(spark: SparkSession, settings: Settings, query: str) -> DataFrame:
    """Run a SELECT. Prefer plain types in the query (toString, toFloat64, epoch ms) over
    ClickHouse-specific ones such as LowCardinality or DateTime64, which JDBC maps less reliably."""
    return spark.read.format("jdbc").options(**_options(settings)).option("query", query).load()
