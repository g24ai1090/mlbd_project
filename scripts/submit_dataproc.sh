#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f config/dataproc.env ]]; then
  source config/dataproc.env
else
  source config/dataproc.env.example
fi

PACKAGE_ZIP="/tmp/bigdata_pipeline_src.zip"
rm -f "$PACKAGE_ZIP"
(
  cd src
  zip -rq "$PACKAGE_ZIP" bigdata_pipeline
)

gcloud dataproc batches submit pyspark spark_driver.py \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --deps-bucket "gs://$BUCKET/deps" \
  --py-files "$PACKAGE_ZIP" \
  --properties "spark.sql.adaptive.enabled=true,spark.eventLog.enabled=true,spark.eventLog.dir=$EVENT_LOG_DIR" \
  -- \
  spark-benchmark \
  --input-path "$DATASET_GCS_PREFIX/yellow_tripdata_2024-*.parquet" \
  --output-dir "gs://$BUCKET/results/final_run" \
  --event-log-dir "$EVENT_LOG_DIR" \
  --variants native external adaptive
