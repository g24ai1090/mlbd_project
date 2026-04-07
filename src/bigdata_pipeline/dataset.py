from __future__ import annotations

import csv
import math
import random
from datetime import datetime, timedelta
from pathlib import Path


FIELDNAMES = [
    "trip_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "pickup_location_id",
    "dropoff_location_id",
]

HOT_PICKUP_ZONES = [132, 138, 161, 162, 230, 236, 237]
OTHER_ZONES = [4, 12, 24, 43, 48, 68, 74, 79, 87, 90, 100, 107, 113, 140, 141, 142, 186, 194, 229]


def generate_synthetic_taxi_dataset(path: str | Path, rows: int, seed: int = 7) -> Path:
    """Create a skewed CSV dataset that resembles the TLC schema."""

    random.seed(seed)
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    start = datetime(2024, 1, 1, 0, 0, 0)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        for index in range(rows):
            pickup = start + timedelta(minutes=index % 60, hours=index // 6500, seconds=index % 59)
            duration_minutes = random.randint(4, 55)
            dropoff = pickup + timedelta(minutes=duration_minutes)
            passenger_count = random.choices([1, 2, 3, 4], weights=[58, 23, 12, 7], k=1)[0]
            pickup_zone = random.choices(
                [*HOT_PICKUP_ZONES, *OTHER_ZONES],
                weights=[18, 18, 12, 10, 10, 8, 8, *([1] * len(OTHER_ZONES))],
                k=1,
            )[0]
            dropoff_zone = random.choice(HOT_PICKUP_ZONES + OTHER_ZONES)
            trip_distance = round(max(0.5, random.lognormvariate(0.7, 0.55)), 2)
            surge = 1.35 if pickup.hour in {8, 9, 17, 18, 19} else 1.0
            fare_amount = round((2.5 + trip_distance * 2.2 + passenger_count * 0.3) * surge, 2)
            writer.writerow(
                {
                    "trip_id": f"trip_{index:09d}",
                    "pickup_datetime": pickup.isoformat(sep=" "),
                    "dropoff_datetime": dropoff.isoformat(sep=" "),
                    "passenger_count": passenger_count,
                    "trip_distance": f"{trip_distance:.2f}",
                    "fare_amount": f"{fare_amount:.2f}",
                    "pickup_location_id": pickup_zone,
                    "dropoff_location_id": dropoff_zone,
                }
            )
    return output_path


def infer_target_rows_for_size_gb(target_size_gb: float, average_row_bytes: int = 180) -> int:
    return math.ceil((target_size_gb * 1024 * 1024 * 1024) / average_row_bytes)
