# Shortcuts for the Spark jobs (wrappers around scripts/spark.sh; on Windows use scripts/spark.ps1).
# Batch range: make ohlcv-5m START=2026-10-27 END=2026-10-28

START ?= $(shell python -c "import datetime as d; print(d.date.today() - d.timedelta(days=1))")
END ?= $(START)

.PHONY: test test-spark sync-contracts streaming stop-streaming ohlcv-5m indicators

test:
	python -m pytest -q

test-spark:  ## Spark unit tests inside the Spark image (JDK 17)
	scripts/spark.sh test

sync-contracts:
	python scripts/sync_contracts.py

streaming:  ## start the three streaming jobs
	scripts/spark.sh start raw_to_minio
	scripts/spark.sh start market_to_clickhouse
	scripts/spark.sh start volume_anomaly

stop-streaming:
	-scripts/spark.sh stop raw_to_minio
	-scripts/spark.sh stop market_to_clickhouse
	-scripts/spark.sh stop volume_anomaly

ohlcv-5m:
	scripts/spark.sh run batch_ohlcv_5m --start $(START) --end $(END)

indicators:
	scripts/spark.sh run batch_indicators --start $(START) --end $(END)
