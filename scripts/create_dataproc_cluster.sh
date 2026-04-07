#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f config/dataproc.env ]]; then
  source config/dataproc.env
else
  source config/dataproc.env.example
fi

gcloud dataproc clusters create "$CLUSTER" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --subnet "$SUBNET" \
  --image-version "$IMAGE_VERSION" \
  --master-machine-type "$MASTER_MACHINE_TYPE" \
  --worker-machine-type "$WORKER_MACHINE_TYPE" \
  --num-workers "$NUM_WORKERS" \
  --bucket "$BUCKET" \
  --properties spark:spark.eventLog.enabled=true,spark:spark.eventLog.dir="$EVENT_LOG_DIR"
