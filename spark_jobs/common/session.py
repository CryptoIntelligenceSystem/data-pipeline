"""SparkSession construction with the pipeline-wide time and S3A settings."""

from pyspark.sql import SparkSession

from .config import Settings

# Applied to every session, including the local one used by unit tests.
BASE_CONF = {
    # All timestamps in the contract are UTC; never let the machine timezone leak in.
    "spark.sql.session.timeZone": "UTC",
    # Unparseable timestamps become NULL (and are rejected by validation) instead of raising
    # SparkUpgradeException under the default EXCEPTION policy.
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    # A field of the wrong type nulls only that field in from_json, so validation can name it.
    "spark.sql.json.enablePartialResults": "true",
}


def base_builder(app_name: str) -> SparkSession.Builder:
    builder = SparkSession.builder.appName(app_name)
    for key, value in BASE_CONF.items():
        builder = builder.config(key, value)
    return builder


def build_spark(app_name: str, settings: Settings) -> SparkSession:
    """Session for jobs submitted to the cluster; S3A points at MinIO from MINIO_* variables."""
    spark = (
        base_builder(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.minio_endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
