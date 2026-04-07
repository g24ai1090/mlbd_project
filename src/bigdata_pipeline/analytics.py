from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


def compute_csv_analytics(path: str | Path, top_n: int = 10) -> dict[str, list[dict[str, str | float | int]]]:
    pickup_hotspots = Counter()
    dropoff_hotspots = Counter()
    hourly_revenue = defaultdict(float)
    hourly_trips = Counter()
    hourly_distance = defaultdict(float)

    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            pickup_hotspots[int(row["pickup_location_id"])] += 1
            dropoff_hotspots[int(row["dropoff_location_id"])] += 1
            hour_key = row["pickup_datetime"][:13]
            hourly_revenue[hour_key] += float(row["fare_amount"])
            hourly_distance[hour_key] += float(row["trip_distance"])
            hourly_trips[hour_key] += 1

    revenue_rows = [
        {
            "pickup_hour": hour,
            "trips": hourly_trips[hour],
            "total_revenue": round(hourly_revenue[hour], 2),
            "avg_trip_distance": round(hourly_distance[hour] / max(hourly_trips[hour], 1), 2),
        }
        for hour in sorted(hourly_revenue)
    ]
    return {
        "top_pickup_zones": [{"zone_id": zone, "trips": count} for zone, count in pickup_hotspots.most_common(top_n)],
        "top_dropoff_zones": [{"zone_id": zone, "trips": count} for zone, count in dropoff_hotspots.most_common(top_n)],
        "hourly_revenue": revenue_rows[: min(len(revenue_rows), 48)],
    }
