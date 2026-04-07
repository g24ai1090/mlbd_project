from __future__ import annotations

import csv
import heapq
import math
import shutil
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Callable, Iterable

from .config import SortConfig
from .dataset import FIELDNAMES


Row = dict[str, str]
SortKeyFn = Callable[[Row], tuple]


def load_rows(path: str | Path) -> list[Row]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def sort_key(row: Row) -> tuple:
    return (
        row["pickup_datetime"],
        int(row["pickup_location_id"]),
        float(row["trip_distance"]),
        row["trip_id"],
    )


def compute_skew_stats(rows: Iterable[Row], num_partitions: int, salted: bool = False) -> dict[str, float | int]:
    counts = Counter()
    pickup_counts = Counter()
    rows_list = list(rows)
    if not rows_list:
        return {
            "top_key_ratio": 0.0,
            "partition_imbalance_ratio": 0.0,
            "estimated_hot_keys": 0,
        }

    for row in rows_list:
        pickup = int(row["pickup_location_id"])
        pickup_counts[pickup] += 1
        partition_seed = (pickup, row["pickup_datetime"][:13]) if salted else pickup
        bucket = hash(partition_seed) % num_partitions
        counts[bucket] += 1

    max_partition = max(counts.values())
    mean_partition = sum(counts.values()) / num_partitions
    hottest = pickup_counts.most_common(1)[0][1]
    return {
        "top_key_ratio": round(hottest / len(rows_list), 4),
        "partition_imbalance_ratio": round(max_partition / mean_partition, 4) if mean_partition else 0.0,
        "estimated_hot_keys": sum(1 for _, count in pickup_counts.items() if count / len(rows_list) >= 0.08),
    }


def write_rows(path: str | Path, rows: Iterable[Row]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def _write_chunk(rows: list[Row], chunk_dir: Path, index: int, key_fn: SortKeyFn) -> Path:
    chunk_path = chunk_dir / f"chunk_{index:05d}.csv"
    write_rows(chunk_path, sorted(rows, key=key_fn))
    return chunk_path


def _merge_chunks(chunk_paths: list[Path], output_path: Path, key_fn: SortKeyFn) -> Path:
    readers = []
    heap = []
    try:
        for idx, chunk_path in enumerate(chunk_paths):
            handle = chunk_path.open("r", encoding="utf-8", newline="")
            reader = csv.DictReader(handle)
            readers.append((handle, reader))
            try:
                first = next(reader)
                heapq.heappush(heap, (key_fn(first), idx, first))
            except StopIteration:
                continue

        with output_path.open("w", encoding="utf-8", newline="") as out_handle:
            writer = csv.DictWriter(out_handle, fieldnames=FIELDNAMES)
            writer.writeheader()
            while heap:
                _, idx, row = heapq.heappop(heap)
                writer.writerow(row)
                handle, reader = readers[idx]
                try:
                    nxt = next(reader)
                except StopIteration:
                    continue
                heapq.heappush(heap, (key_fn(nxt), idx, nxt))
    finally:
        for handle, _ in readers:
            handle.close()
    return output_path


def _merge_pass_count(num_chunks: int, fan_in: int) -> int:
    if num_chunks <= 1:
        return 0
    return math.ceil(math.log(num_chunks, fan_in))


def run_native_sort(input_path: str | Path, output_path: str | Path, config: SortConfig) -> dict[str, float | int | str]:
    started = time.perf_counter()
    rows = load_rows(input_path)
    stats = compute_skew_stats(rows, config.num_simulated_partitions, salted=False)
    write_rows(output_path, sorted(rows, key=sort_key))
    elapsed = time.perf_counter() - started
    input_bytes = Path(input_path).stat().st_size
    return {
        "variant": "native",
        "execution_seconds": round(elapsed, 4),
        "rows": len(rows),
        "input_bytes": input_bytes,
        "spill_events": 0,
        "merge_passes": 0,
        "disk_write_bytes": Path(output_path).stat().st_size,
        "throughput_gb_per_min": round((input_bytes / (1024**3)) / max(elapsed / 60, 1e-9), 4),
        **stats,
    }


def run_external_sort(
    input_path: str | Path,
    output_path: str | Path,
    config: SortConfig,
    chunk_rows: int,
    variant_name: str,
    salted_stats: bool = False,
) -> dict[str, float | int | str]:
    started = time.perf_counter()
    rows = load_rows(input_path)
    stats = compute_skew_stats(rows, config.num_simulated_partitions, salted=salted_stats)
    chunk_dir = Path(tempfile.mkdtemp(prefix=f"{variant_name}_chunks_", dir=str(Path(output_path).parent)))
    chunk_paths: list[Path] = []
    disk_write_bytes = 0
    try:
        for index in range(0, len(rows), chunk_rows):
            chunk = rows[index : index + chunk_rows]
            chunk_path = _write_chunk(chunk, chunk_dir, index // chunk_rows, sort_key)
            chunk_paths.append(chunk_path)
            disk_write_bytes += chunk_path.stat().st_size
        _merge_chunks(chunk_paths, Path(output_path), sort_key)
        disk_write_bytes += Path(output_path).stat().st_size
    finally:
        shutil.rmtree(chunk_dir, ignore_errors=True)

    elapsed = time.perf_counter() - started
    input_bytes = Path(input_path).stat().st_size
    return {
        "variant": variant_name,
        "execution_seconds": round(elapsed, 4),
        "rows": len(rows),
        "input_bytes": input_bytes,
        "spill_events": len(chunk_paths),
        "merge_passes": _merge_pass_count(len(chunk_paths), config.merge_fan_in),
        "disk_write_bytes": disk_write_bytes,
        "throughput_gb_per_min": round((input_bytes / (1024**3)) / max(elapsed / 60, 1e-9), 4),
        **stats,
    }


def choose_adaptive_chunk_rows(rows: list[Row], config: SortConfig) -> int:
    skew = compute_skew_stats(rows, config.num_simulated_partitions, salted=False)
    if skew["top_key_ratio"] >= config.skew_threshold:
        return max(config.adaptive_chunk_rows, config.chunk_rows * 3)
    return config.adaptive_chunk_rows
