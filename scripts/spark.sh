#!/usr/bin/env bash
# Run Spark jobs on the cluster from ../infrastructure (docker compose).
#
#   scripts/spark.sh start <job>            streaming job, detached; log in /tmp/<job>.log in spark-master
#   scripts/spark.sh run   <job> [args...]  batch job, foreground (e.g. --start 2026-10-27 --end 2026-10-28)
#   scripts/spark.sh stop  <job>
#   scripts/spark.sh logs  <job>
#   scripts/spark.sh test                   unit tests inside the Spark image (JDK 17, its own pyspark 3.5.9)
#
# Job tuning variables set in your shell are forwarded: SPARK_STARTING_OFFSETS,
# SPARK_TRIGGER_INTERVAL, SPARK_MAX_OFFSETS_PER_TRIGGER, SPARK_FAIL_ON_DATA_LOSS, ANOMALY_*,
# and per-job overrides such as MARKET_TO_CLICKHOUSE_STARTING_OFFSETS.
set -euo pipefail

INFRA_DIR="${INFRA_DIR:-$(cd "$(dirname "$0")/../../infrastructure" && pwd)}"
compose() { docker compose -f "${INFRA_DIR}/docker-compose.yml" "$@"; }

action="${1:?usage: spark.sh start|run|stop|logs|test [job] [args...]}"
shift

if [ "${action}" = "test" ]; then
  compose run --rm --no-deps -T -w /opt/pipeline --entrypoint sh spark-master -c \
    "export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/opt/pipeline:/opt/spark/python:\$(ls /opt/spark/python/lib/py4j-*.zip) && pip install --quiet --user jsonschema pytest -r requirements.txt >/dev/null && python3 -m pytest -q -p no:cacheprovider tests"
  exit
fi

job="${1:?job name required, e.g. market_to_clickhouse}"
shift
script="/opt/pipeline/spark_jobs/jobs/${job}.py"

env_args=()
for var in $(compgen -e | grep -E '^(SPARK_(STARTING_OFFSETS|TRIGGER_INTERVAL|MAX_OFFSETS_PER_TRIGGER|FAIL_ON_DATA_LOSS)|ANOMALY_|(RAW_TO_MINIO|MARKET_TO_CLICKHOUSE|NEWS_TO_CLICKHOUSE|VOLUME_ANOMALY)_)' || true); do
  env_args+=(-e "${var}=${!var}")
done

submit="/opt/spark/bin/spark-submit --master spark://spark-master:7077 --name ${job} ${script}"

case "${action}" in
  start)
    compose exec -d "${env_args[@]}" spark-master sh -c "${submit} $* > /tmp/${job}.log 2>&1"
    echo "${job} started. Logs: scripts/spark.sh logs ${job}   UI: http://localhost:8080"
    ;;
  run)
    compose exec -T "${env_args[@]}" spark-master sh -c "${submit} $*"
    ;;
  stop)
    compose exec -T spark-master pkill -f "spark_jobs/jobs/${job}.py" && echo "${job} stopped"
    ;;
  logs)
    compose exec spark-master tail -n 100 -f "/tmp/${job}.log"
    ;;
  *)
    echo "unknown action: ${action}" >&2
    exit 2
    ;;
esac
