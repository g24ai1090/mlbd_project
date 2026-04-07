from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from bigdata_pipeline.benchmark import run_local_benchmark
from bigdata_pipeline.dataset import FIELDNAMES, generate_synthetic_taxi_dataset


class LocalPipelineTests(unittest.TestCase):
    def test_generate_synthetic_dataset_creates_expected_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_path = Path(tmp_dir) / "synthetic.csv"
            generate_synthetic_taxi_dataset(dataset_path, rows=25, seed=11)

            self.assertTrue(dataset_path.exists())
            lines = dataset_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(lines[0].split(","), FIELDNAMES)
            self.assertEqual(len(lines), 26)

    def test_local_benchmark_exports_expected_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            dataset_path = tmp_path / "demo.csv"
            output_dir = tmp_path / "results"

            generate_synthetic_taxi_dataset(dataset_path, rows=5000, seed=7)
            summary = run_local_benchmark(dataset_path, output_dir)

            self.assertEqual(summary["row_count"], 5000)
            self.assertEqual(len(summary["benchmark_results"]), 3)

            expected_files = [
                output_dir / "benchmark_results.json",
                output_dir / "benchmark_results.csv",
                output_dir / "analytics_summary.json",
                output_dir / "hourly_revenue.csv",
                output_dir / "sorted_native.csv",
                output_dir / "sorted_external.csv",
                output_dir / "sorted_adaptive.csv",
            ]
            for file_path in expected_files:
                self.assertTrue(file_path.exists(), msg=f"Missing artifact: {file_path}")

            payload = json.loads((output_dir / "benchmark_results.json").read_text(encoding="utf-8"))
            self.assertIn("comparison_summary", payload)
            self.assertIn("analytics", payload)

            variants = {entry["variant"]: entry for entry in payload["benchmark_results"]}
            self.assertEqual(set(variants.keys()), {"native", "external", "adaptive"})
            self.assertGreaterEqual(variants["external"]["spill_events"], variants["adaptive"]["spill_events"])


if __name__ == "__main__":
    unittest.main()
