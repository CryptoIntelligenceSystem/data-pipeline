import importlib.util

# Spark tests need requirements-spark.txt (pyspark + JDK 17); collector tests do not.
collect_ignore = [] if importlib.util.find_spec("pyspark") else ["spark"]
