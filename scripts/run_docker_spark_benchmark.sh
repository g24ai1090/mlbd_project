#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

INPUT_PATH="${1:-/workspace/data/real/yellow_tripdata_2024-01.parquet}"
OUTPUT_BASE_DIR="${2:-/workspace/results/docker_spark_run}"
EVENT_LOG_DIR="${3:-file:///workspace/results/docker_events}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

OUTPUT_BASE_DIR="${OUTPUT_BASE_DIR%/}"
OUTPUT_DIR="${OUTPUT_BASE_DIR}_${TIMESTAMP}"

mkdir -p results/docker_events

echo "Writing benchmark output to: ${OUTPUT_DIR}"

docker compose exec spark-master bash -lc "
  mkdir -p /workspace/results/docker_events && \
  INPUT_PATH='${INPUT_PATH}' && \
  OUTPUT_DIR='${OUTPUT_DIR}' && \
  EVENT_LOG_DIR='${EVENT_LOG_DIR}' && \
  PYTHONPATH=/workspace/src /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=\${EVENT_LOG_DIR} \
    --conf spark.sql.shuffle.partitions=8 \
    /workspace/spark_driver.py \
    spark-benchmark \
    --input-path \"\${INPUT_PATH}\" \
    --output-dir \"\${OUTPUT_DIR}\" \
    --event-log-dir \"\${EVENT_LOG_DIR}\" \
    --variants native external adaptive
"

echo "Benchmark results saved under: ${OUTPUT_DIR}"
echo "Summary JSON: ${OUTPUT_DIR}/benchmark_results.json"
