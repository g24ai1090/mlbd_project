from __future__ import annotations

import argparse
import json
from pathlib import Path

from .benchmark import run_local_benchmark, run_spark_benchmark
from .dataset import generate_synthetic_taxi_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Adaptive distributed sorting pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    local_demo = subparsers.add_parser("local-demo", help="Run the local end-to-end benchmark demo")
    local_demo.add_argument("--rows", type=int, default=150000, help="Number of synthetic taxi rows to generate")
    local_demo.add_argument("--dataset-path", default="data/raw/demo_taxi_trips.csv")
    local_demo.add_argument("--output-dir", default="results/demo")

    spark = subparsers.add_parser("spark-benchmark", help="Run the PySpark benchmark path")
    spark.add_argument("--input-path", required=True, help="Input parquet path, local or GCS")
    spark.add_argument("--output-dir", default="results/spark_run")
    spark.add_argument("--event-log-dir", default=None)
    spark.add_argument("--variants", nargs="+", default=["native", "external", "adaptive"])

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "local-demo":
        dataset_path = generate_synthetic_taxi_dataset(args.dataset_path, args.rows)
        summary = run_local_benchmark(dataset_path, args.output_dir)
    elif args.command == "spark-benchmark":
        summary = run_spark_benchmark(args.input_path, args.output_dir, args.variants, args.event_log_dir)
    else:
        raise ValueError(f"Unsupported command: {args.command}")

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
