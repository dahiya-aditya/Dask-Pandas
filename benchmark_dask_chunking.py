"""
Benchmark Dask chunk strategies for South Asia ERA5 analysis.

Runs south_asia_2024_dask_analysis with multiple chunk profiles,
collects runtime summaries, and writes:
- data/results/dask_chunk_benchmark.csv
- data/results/dask_chunk_benchmark.json
"""

from __future__ import annotations

import argparse
import json
import time

import pandas as pd

from core import config
from south_asia_2024_dask_analysis import run_dask_analysis


BENCHMARK_CSV = config.RESULTS_DIR / "dask_chunk_benchmark.csv"
BENCHMARK_JSON = config.RESULTS_DIR / "dask_chunk_benchmark.json"


def main() -> None:
    """Execute predefined chunk profiles and persist benchmark results."""
    parser = argparse.ArgumentParser(description="Benchmark Dask chunk strategies")
    parser.add_argument(
        "--scheduler",
        choices=["threads", "distributed"],
        default="threads",
        help="Scheduler mode for each benchmark run",
    )
    parser.add_argument("--workers", type=int, default=4, help="Workers for distributed mode")
    parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=1,
        help="Threads per worker for distributed mode",
    )
    args = parser.parse_args()

    profiles = [
        {"name": "time_heavy", "chunk_time": 240, "chunk_lat": 120, "chunk_lon": 140},
        {"name": "balanced", "chunk_time": 96, "chunk_lat": 80, "chunk_lon": 80},
        {"name": "tiny_chunks", "chunk_time": 24, "chunk_lat": 30, "chunk_lon": 30},
    ]

    rows: list[dict[str, object]] = []
    start_all = time.perf_counter()

    for profile in profiles:
        print("\n" + "=" * 70)
        print(f"Running profile: {profile['name']}")
        print("=" * 70)

        summary = run_dask_analysis(
            chunk_time=int(profile["chunk_time"]),
            chunk_lat=int(profile["chunk_lat"]),
            chunk_lon=int(profile["chunk_lon"]),
            scheduler=args.scheduler,
            workers=args.workers,
            threads_per_worker=args.threads_per_worker,
            save_figures=False,
        )

        rows.append(
            {
                "profile": profile["name"],
                "chunk_time": profile["chunk_time"],
                "chunk_lat": profile["chunk_lat"],
                "chunk_lon": profile["chunk_lon"],
                "elapsed_seconds": summary["elapsed_seconds"],
                "rows_daily": summary["rows_daily"],
                "annual_mean_t2m_c": summary["temperature_2024"]["annual_mean_c"],
            }
        )

    elapsed_all = time.perf_counter() - start_all

    report = pd.DataFrame(rows).sort_values("elapsed_seconds").reset_index(drop=True)
    report.to_csv(BENCHMARK_CSV, index=False)

    summary = {
        "profiles_tested": len(rows),
        "total_elapsed_seconds": round(float(elapsed_all), 3),
        "best_profile": str(report.iloc[0]["profile"]),
        "best_elapsed_seconds": float(report.iloc[0]["elapsed_seconds"]),
        "worst_profile": str(report.iloc[-1]["profile"]),
        "worst_elapsed_seconds": float(report.iloc[-1]["elapsed_seconds"]),
        "report_csv": str(BENCHMARK_CSV),
    }
    BENCHMARK_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\n" + "=" * 70)
    print("Dask chunk benchmark complete")
    print("=" * 70)
    print(f"Profiles: {summary['profiles_tested']}")
    print(f"Best: {summary['best_profile']} ({summary['best_elapsed_seconds']} s)")
    print(f"Worst: {summary['worst_profile']} ({summary['worst_elapsed_seconds']} s)")
    print(f"Report: {BENCHMARK_CSV}")
    print(f"Summary: {BENCHMARK_JSON}")


if __name__ == "__main__":
    main()
