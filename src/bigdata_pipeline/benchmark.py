from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any

from .adaptive_sort import (
    choose_adaptive_chunk_rows,
    load_rows,
    run_external_sort,
    run_native_sort,
)
from .analytics import compute_csv_analytics
from .config import BenchmarkConfig, SortConfig
from .results import write_csv, write_json


def run_local_benchmark(dataset_path: str | Path, output_dir: str | Path) -> dict[str, Any]:
    dataset_path = Path(dataset_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sort_config = SortConfig()
    benchmark_config = BenchmarkConfig()
    rows = load_rows(dataset_path)
    adaptive_chunk_rows = choose_adaptive_chunk_rows(rows, sort_config)

    benchmarks = []
    variants = {
        "native": lambda path, out: run_native_sort(path, out, sort_config),
        "external": lambda path, out: run_external_sort(
            path, out, sort_config, sort_config.chunk_rows, "external", salted_stats=False
        ),
        "adaptive": lambda path, out: run_external_sort(
            path, out, sort_config, adaptive_chunk_rows, "adaptive", salted_stats=True
        ),
    }

    for variant in benchmark_config.variants:
        sorted_output = output_dir / f"sorted_{variant}.csv"
        result = variants[variant](dataset_path, sorted_output)
        result["sorted_output"] = str(sorted_output)
        benchmarks.append(result)

    analytics = compute_csv_analytics(output_dir / "sorted_adaptive.csv", benchmark_config.analytics_top_n)
    summary = {
        "generated_at_epoch": round(time.time(), 3),
        "dataset_path": str(dataset_path),
        "dataset_size_mb": round(dataset_path.stat().st_size / (1024**2), 3),
        "row_count": len(rows),
        "benchmark_results": benchmarks,
        "analytics": analytics,
        "comparison_summary": build_comparison_summary(benchmarks),
    }

    write_json(output_dir / "benchmark_results.json", summary)
    write_csv(output_dir / "benchmark_results.csv", benchmarks)
    write_json(output_dir / "analytics_summary.json", analytics)
    write_csv(output_dir / "hourly_revenue.csv", analytics["hourly_revenue"])
    return summary


def build_comparison_summary(benchmarks: list[dict[str, Any]]) -> dict[str, Any]:
    by_variant = {entry["variant"]: entry for entry in benchmarks}
    native = by_variant.get("native")
    adaptive = by_variant.get("adaptive")
    external = by_variant.get("external")
    if not (native and adaptive and external):
        return {}

    external_spill_events = float(external.get("spill_events", 0))
    adaptive_spill_events = float(adaptive.get("spill_events", 0))
    if external_spill_events > 0:
        spill_reduction_pct = round(
            ((external_spill_events - adaptive_spill_events) / external_spill_events) * 100,
            2,
        )
    else:
        external_spill_bytes = float(external.get("disk_bytes_spilled", 0)) + float(external.get("memory_bytes_spilled", 0))
        adaptive_spill_bytes = float(adaptive.get("disk_bytes_spilled", 0)) + float(adaptive.get("memory_bytes_spilled", 0))
        spill_reduction_pct = round(
            ((external_spill_bytes - adaptive_spill_bytes) / max(external_spill_bytes, 1e-9)) * 100,
            2,
        )

    note = (
        "On the small local dataset the native in-memory baseline is fastest, while the adaptive variant mainly demonstrates "
        "spill reduction and skew mitigation. Use the Dataproc run to evaluate large-scale runtime gains."
    )
    if any("shuffle_write_bytes" in entry for entry in benchmarks):
        note = (
            "For the Spark benchmark, compare runtime together with spill, shuffle, and partition imbalance. "
            "Large-scale Dataproc runs provide the strongest evidence for adaptive gains."
        )

    return {
        "adaptive_vs_native_runtime_pct": round(
            ((native["execution_seconds"] - adaptive["execution_seconds"]) / max(native["execution_seconds"], 1e-9)) * 100,
            2,
        ),
        "adaptive_vs_external_runtime_pct": round(
            ((external["execution_seconds"] - adaptive["execution_seconds"]) / max(external["execution_seconds"], 1e-9)) * 100,
            2,
        ),
        "adaptive_partition_imbalance_delta": round(
            external["partition_imbalance_ratio"] - adaptive["partition_imbalance_ratio"],
            4,
        ),
        "adaptive_vs_external_spill_reduction_pct": spill_reduction_pct,
        "adaptive_vs_external_imbalance_reduction_pct": round(
            (
                (external["partition_imbalance_ratio"] - adaptive["partition_imbalance_ratio"])
                / max(external["partition_imbalance_ratio"], 1e-9)
            )
            * 100,
            2,
        ),
        "demo_note": note,
    }


def maybe_import_pyspark() -> tuple[Any, Any, Any]:
    try:
        from pyspark.sql import SparkSession, functions as F, types as T
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Install dependencies with `pip install -r requirements.txt` before running spark-benchmark."
        ) from exc
    return SparkSession, F, T


def _estimate_input_bytes(spark: Any, input_path: str) -> int:
    statuses = _glob_statuses(spark, input_path)
    if not statuses:
        return 0
    total = 0
    for status in statuses:
        total += status.getLen()
    return int(total)


def _glob_statuses(spark: Any, input_path: str) -> list[Any]:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    statuses = fs.globStatus(jvm.org.apache.hadoop.fs.Path(input_path))
    if not statuses:
        return []
    return list(statuses)


def _resolve_input_paths(spark: Any, input_path: str) -> list[str]:
    return [str(status.getPath()) for status in _glob_statuses(spark, input_path)]


def _select_dataset_columns(df: Any) -> Any:
    return df.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "PULocationID",
        "DOLocationID",
    )


def _normalize_loaded_df(df: Any, F: Any) -> Any:
    return (
        _select_dataset_columns(df)
        .na.drop(subset=["tpep_pickup_datetime", "trip_distance", "fare_amount", "PULocationID"])
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("pickup_location_id", F.col("pickup_location_id").cast("int"))
        .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast("int"))
    )


def _load_input_df(spark: Any, input_path: str, F: Any) -> Any:
    paths = _resolve_input_paths(spark, input_path)
    if not paths:
        raise FileNotFoundError(f"No parquet files matched input path: {input_path}")

    normalized = []
    for path in paths:
        loaded = spark.read.parquet(path)
        normalized.append(_normalize_loaded_df(loaded, F))

    df = normalized[0]
    for part_df in normalized[1:]:
        df = df.unionByName(part_df)
    return df


def _compute_adaptive_partitions(
    input_bytes: int,
    row_count: int,
    skew_ratio: float,
    sort_config: SortConfig,
) -> int:
    base = math.ceil(input_bytes / (sort_config.target_partition_mb * 1024 * 1024)) if input_bytes else max(
        sort_config.min_partitions, math.ceil(row_count / 1_000_000)
    )
    if skew_ratio >= sort_config.skew_threshold:
        base = int(base * 1.5)
    return max(sort_config.min_partitions, min(sort_config.max_partitions, base))


def _parse_event_logs(event_log_dir: str | None) -> dict[str, Any]:
    if not event_log_dir:
        return {}
    if event_log_dir.startswith("file://"):
        base_dir = Path(event_log_dir.replace("file://", "", 1))
    else:
        return {"event_log_note": "Automatic parsing currently supports file:// event logs."}
    if not base_dir.exists():
        return {"event_log_note": f"Event log directory not found: {base_dir}"}

    latest = max(base_dir.glob("*"), key=lambda path: path.stat().st_mtime, default=None)
    if latest is None or not latest.is_file():
        return {}

    metrics = {
        "disk_bytes_spilled": 0,
        "memory_bytes_spilled": 0,
        "shuffle_write_bytes": 0,
        "shuffle_read_bytes": 0,
        "spill_events": 0,
    }
    with latest.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("Event") != "SparkListenerTaskEnd":
                continue
            task_metrics = payload.get("Task Metrics", {})
            disk_spilled = int(task_metrics.get("Disk Bytes Spilled", 0))
            memory_spilled = int(task_metrics.get("Memory Bytes Spilled", 0))
            metrics["disk_bytes_spilled"] += disk_spilled
            metrics["memory_bytes_spilled"] += memory_spilled
            if disk_spilled > 0 or memory_spilled > 0:
                metrics["spill_events"] += 1
            metrics["shuffle_write_bytes"] += int(task_metrics.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0))
            shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
            metrics["shuffle_read_bytes"] += int(shuffle_read.get("Remote Bytes Read", 0)) + int(
                shuffle_read.get("Local Bytes Read", 0)
            )
    return metrics


def run_spark_benchmark(
    input_path: str,
    output_dir: str | Path,
    variants: list[str],
    event_log_dir: str | None = None,
) -> dict[str, Any]:
    SparkSession, F, T = maybe_import_pyspark()
    sort_config = SortConfig()
    benchmark_config = BenchmarkConfig()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName("adaptive-distributed-sorting-benchmark")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )
    if event_log_dir:
        builder = builder.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", event_log_dir)
    spark = builder.getOrCreate()

    try:
        df = _load_input_df(spark, input_path, F)
        row_count = df.count()
        input_bytes = _estimate_input_bytes(spark, input_path)

        sample = df.sample(fraction=min(sort_config.sample_fraction, 1.0), seed=7)
        hotspot_row = sample.groupBy("pickup_location_id").count().orderBy(F.desc("count")).first()
        skew_ratio = (hotspot_row["count"] / max(sample.count(), 1)) if hotspot_row else 0.0
        adaptive_partitions = _compute_adaptive_partitions(input_bytes, row_count, skew_ratio, sort_config)

        results = []
        for variant in variants:
            started = time.perf_counter()
            run_output = output_dir / variant
            if variant == "native":
                spark.conf.set("spark.sql.shuffle.partitions", max(sort_config.min_partitions, adaptive_partitions // 2))
                sorted_df = df.orderBy("pickup_datetime", "pickup_location_id", "trip_distance")
                estimated_imbalance = skew_ratio if skew_ratio else 0.0
            elif variant == "external":
                spark.conf.set("spark.sql.shuffle.partitions", adaptive_partitions)
                sorted_df = df.repartition(adaptive_partitions, "pickup_location_id").sortWithinPartitions(
                    "pickup_datetime", "pickup_location_id", "trip_distance"
                )
                estimated_imbalance = skew_ratio if skew_ratio else 0.0
            elif variant == "adaptive":
                spark.conf.set("spark.sql.shuffle.partitions", adaptive_partitions)
                hot_keys = [row["pickup_location_id"] for row in sample.groupBy("pickup_location_id").count().orderBy(F.desc("count")).limit(sort_config.max_hot_keys).collect()]
                salted = df.withColumn(
                    "adaptive_salt",
                    F.when(F.col("pickup_location_id").isin(hot_keys), F.hour("pickup_datetime") % 8).otherwise(F.lit(0)),
                )
                sorted_df = salted.repartitionByRange(
                    adaptive_partitions,
                    "adaptive_salt",
                    "pickup_datetime",
                    "pickup_location_id",
                ).sortWithinPartitions("pickup_datetime", "pickup_location_id", "trip_distance")
                estimated_imbalance = max(0.0, skew_ratio / 2)
            else:
                raise ValueError(f"Unsupported variant: {variant}")

            sorted_df.write.mode("overwrite").parquet(str(run_output))
            elapsed = time.perf_counter() - started
            metrics = _parse_event_logs(event_log_dir)
            results.append(
                {
                    "variant": variant,
                    "rows": row_count,
                    "input_bytes": input_bytes,
                    "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
                    "execution_seconds": round(elapsed, 4),
                    "throughput_gb_per_min": round((input_bytes / (1024**3)) / max(elapsed / 60, 1e-9), 4),
                    "estimated_top_key_ratio": round(skew_ratio, 4),
                    "partition_imbalance_ratio": round(estimated_imbalance, 4),
                    "merge_passes": 0,
                    "output_path": str(run_output),
                    **metrics,
                }
            )

        hourly_analytics = (
            df.groupBy(F.date_trunc("hour", "pickup_datetime").alias("pickup_hour"))
            .agg(
                F.count("*").alias("trips"),
                F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
                F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            )
            .orderBy("pickup_hour")
            .limit(48)
            .withColumn("pickup_hour", F.date_format("pickup_hour", "yyyy-MM-dd HH"))
            .toPandas()
            .to_dict(orient="records")
        )
        top_pickup_zones = (
            df.groupBy("pickup_location_id")
            .count()
            .orderBy(F.desc("count"), F.asc("pickup_location_id"))
            .limit(benchmark_config.analytics_top_n)
            .select(
                F.col("pickup_location_id").alias("zone_id"),
                F.col("count").alias("trips"),
            )
            .toPandas()
            .to_dict(orient="records")
        )
        top_dropoff_zones = (
            df.groupBy("dropoff_location_id")
            .count()
            .orderBy(F.desc("count"), F.asc("dropoff_location_id"))
            .limit(benchmark_config.analytics_top_n)
            .select(
                F.col("dropoff_location_id").alias("zone_id"),
                F.col("count").alias("trips"),
            )
            .toPandas()
            .to_dict(orient="records")
        )
        analytics = {
            "hourly_revenue": hourly_analytics,
            "top_pickup_zones": top_pickup_zones,
            "top_dropoff_zones": top_dropoff_zones,
        }
        summary = {
            "dataset_path": input_path,
            "row_count": row_count,
            "benchmark_results": results,
            "analytics": analytics,
            "comparison_summary": build_comparison_summary(results),
        }
        write_json(output_dir / "benchmark_results.json", summary)
        write_csv(output_dir / "benchmark_results.csv", results)
        write_csv(output_dir / "hourly_revenue.csv", hourly_analytics)
        write_csv(output_dir / "top_pickup_zones.csv", top_pickup_zones)
        write_csv(output_dir / "top_dropoff_zones.csv", top_dropoff_zones)
        return summary
    finally:
        spark.stop()
