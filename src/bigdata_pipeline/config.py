from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(slots=True)
class SortConfig:
    target_partition_mb: int = 256
    min_partitions: int = 8
    max_partitions: int = 1024
    skew_threshold: float = 0.15
    sample_fraction: float = 0.02
    max_hot_keys: int = 10
    merge_fan_in: int = 16
    chunk_rows: int = 25000
    adaptive_chunk_rows: int = 100000
    num_simulated_partitions: int = 16


@dataclass(slots=True)
class BenchmarkConfig:
    variants: list[str] = field(default_factory=lambda: ["native", "external", "adaptive"])
    sort_columns: list[str] = field(
        default_factory=lambda: ["pickup_datetime", "pickup_location_id", "trip_distance"]
    )
    analytics_top_n: int = 10


def load_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)
