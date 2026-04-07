# Adaptive Distributed External Sorting and Analytics Pipeline

This repository turns the proposal into a demoable end-to-end project with:

- a local benchmark pipeline that generates synthetic NYC Taxi-like data and real result files
- a PySpark benchmark path for Spark Native vs External Merge vs Adaptive sorting
- a Docker-based distributed runtime for local Spark, monitoring, and dashboard delivery
- Dataproc/GCS helper scripts for full-dataset execution
- a Streamlit dashboard for presenting benchmarking results
- monitoring starter configs for Prometheus and Grafana

## Project Layout

- `src/bigdata_pipeline/`: pipeline package and CLI
- `config/`: benchmark and Dataproc settings
- `dashboard/`: Streamlit app
- `docs/`: architecture, experiment design, and presentation notes
- `monitoring/`: Prometheus and Grafana starter configs
- `results/demo/`: generated benchmark outputs for the local demo

## Final Project Status

The repository has been validated end to end with:

- successful real-data Spark benchmark runs on `yellow_tripdata_2024-*.parquet`
- successful real-data Spark benchmark runs on `yellow_tripdata_2025-*.parquet`
- Streamlit dashboard screenshots for both years
- Spark standalone cluster screenshots
- Grafana monitoring screenshots

On the local Docker cluster, separate annual runs were stable and are the recommended final evidence set.  
A combined `2024 + 2025` run was attempted, but it failed due shuffle metadata fetch instability on the constrained local cluster, so it should not be used as a final benchmark result.

## Quick Start

### 1. Local End-to-End Demo

This path works without Spark and is the fastest way to show a complete pipeline.

```bash
PYTHONPATH=src python3 -m bigdata_pipeline.main local-demo \
  --rows 150000 \
  --output-dir results/demo \
  --dataset-path data/raw/demo_taxi_trips.csv
```

It will:

1. generate synthetic taxi trip data
2. run three benchmark variants
3. export JSON and CSV results
4. create sorted outputs and analytics summaries

### 2. PySpark / Dataproc Benchmark

Install dependencies first:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Then run locally with Spark:

```bash
PYTHONPATH=src python3 -m bigdata_pipeline.main spark-benchmark \
  --input-path gs://YOUR_BUCKET/nyc_taxi/raw/yellow_tripdata_2024-01.parquet \
  --output-dir results/spark_run \
  --variants native adaptive \
  --event-log-dir file:///tmp/spark-events
```

Or use the Dataproc helper:

```bash
bash scripts/submit_dataproc.sh
```

### 3. Dashboard

```bash
streamlit run dashboard/app.py
```

### 4. Docker-Based Distributed Runtime

If you do not have cloud credits, you can run the project in a local Docker environment with:

- Spark master + workers
- Streamlit dashboard
- Prometheus
- Grafana

Start here:

```bash
docker compose up -d --build
```

Then follow:

```bash
docs/docker_deployment.md
```

### 5. Recommended Final Benchmark Commands

Start the stack first:

```bash
docker compose up -d --build
```

Then run separate annual benchmarks:

```bash
bash scripts/run_docker_spark_benchmark.sh \
  '/workspace/data/real/yellow_tripdata_2024-*.parquet' \
  /workspace/results/docker_spark_run_2024
```

```bash
bash scripts/run_docker_spark_benchmark.sh \
  '/workspace/data/real/yellow_tripdata_2025-*.parquet' \
  /workspace/results/docker_spark_run_2025
```

The helper script now creates a timestamped output directory on every run, so previous `benchmark_results.json` files are preserved.

Example:

- requested base output: `/workspace/results/docker_spark_run_2024`
- actual saved output: `/workspace/results/docker_spark_run_2024_YYYYMMDD_HHMMSS`

Load the generated `benchmark_results.json` from that timestamped folder in the Streamlit sidebar.

## Pipeline Stages

1. Ingestion
   Download monthly TLC parquet files to GCS.
2. Preprocessing
   Select required columns, filter invalid trips, and derive time fields.
3. Benchmarking
   Compare `native`, `external`, and `adaptive` sort strategies.
4. Analytics
   Produce hourly demand, revenue, distance, pickup hotspot, and dropoff hotspot summaries.
5. Monitoring
   Collect Spark and node metrics using Prometheus and Grafana.
6. Reporting
   Visualize results in Streamlit and export JSON/CSV for the report.

## Benchmark Variants

- `native`: standard in-memory sort for the local demo, Spark `orderBy` for the cluster path
- `external`: chunked external merge sort baseline
- `adaptive`: skew-aware partitioning and adaptive chunk sizing with merge tuning

## Full-Dataset Path

For the final project demo, use multiple monthly parquet files from the NYC TLC Trip Record Data portal and store them in GCS. The Dataproc job supports running the experiment matrix from `config/benchmark_profiles.json`.

For this repository's final local submission story, the practical validated path is:

- Docker Spark cluster
- separate annual TLC benchmarks for `2024` and `2025`
- Streamlit and Grafana evidence collected from those runs

## References

- NYC TLC Trip Record Data: https://home4.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Google Cloud Dataproc PySpark submission: https://docs.cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark
