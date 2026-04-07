#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHONPATH=src python3 -m bigdata_pipeline.main local-demo \
  --rows 150000 \
  --dataset-path data/raw/demo_taxi_trips.csv \
  --output-dir results/demo
