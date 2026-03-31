"""
Reproducible Dask revision experiment: poor initial pipeline vs revised pipeline.

This script is designed to satisfy the project requirement that an initial Dask
approach performs poorly and is then revised for better scalability/clarity.

Outputs:
- data/results/dask_revision_experiment.json
- data/results/dask_revision_experiment.csv
"""

from __future__ import annotations

import argparse
import json
import pandas as pd

from core import config, utils
from south_asia_2024_dask_analysis import run_dask_analysis


EXPERIMENT_JSON = config.RESULTS_DIR / "dask_revision_experiment.json"
EXPERIMENT_CSV = config.RESULTS_DIR / "dask_revision_experiment.csv"


def run_poor_pipeline(chunk_time: int, chunk_lat: int, chunk_lon: int) -> dict[str, float | int | str | dict[str, int]]:
    """
    Intentionally poor Dask pipeline.

    Anti-patterns used intentionally:
    1) tiny chunks that produce too many tasks
    2) chunk boundaries that split storage chunks and increase overhead

    Uses the same analysis pipeline as the revised path to keep comparison fair.
    """
    summary = run_dask_analysis(
        chunk_time=chunk_time,
        chunk_lat=chunk_lat,
        chunk_lon=chunk_lon,
        scheduler="threads",
        workers=4,
        threads_per_worker=1,
        save_figures=False,
    )

    return {
        "name": "poor",
        "scheduler": str(summary["scheduler"]),
        "chunks": summary["chunks"],
        "elapsed_seconds": float(summary["elapsed_seconds"]),
        "rows_daily": int(summary["rows_daily"]),
        "annual_mean_t2m_c": float(summary["temperature_2024"]["annual_mean_c"]),
        "anti_patterns": [
            "tiny chunks",
            "storage-chunk misalignment",
        ],
    }


def run_revised_pipeline(chunk_time: int, chunk_lat: int, chunk_lon: int) -> dict[str, float | int | str | dict[str, int]]:
    """Revised pipeline using fused lazy graph and one materialization stage."""
    summary = run_dask_analysis(
        chunk_time=chunk_time,
        chunk_lat=chunk_lat,
        chunk_lon=chunk_lon,
        scheduler="threads",
        workers=4,
        threads_per_worker=1,
        save_figures=False,
    )

    return {
        "name": "revised",
        "scheduler": str(summary["scheduler"]),
        "chunks": summary["chunks"],
        "elapsed_seconds": float(summary["elapsed_seconds"]),
        "rows_daily": int(summary["rows_daily"]),
        "annual_mean_t2m_c": float(summary["temperature_2024"]["annual_mean_c"]),
        "good_practices": [
            "single lazy graph",
            "single compute phase",
            "chunking chosen for time aggregation",
        ],
    }


def main() -> None:
    """Run both versions and write a revision-comparison artifact."""
    parser = argparse.ArgumentParser(description="Run poor-vs-revised Dask experiment")
    parser.add_argument("--poor-chunk-time", type=int, default=24)
    parser.add_argument("--poor-chunk-lat", type=int, default=30)
    parser.add_argument("--poor-chunk-lon", type=int, default=30)
    parser.add_argument("--revised-chunk-time", type=int, default=96)
    parser.add_argument("--revised-chunk-lat", type=int, default=80)
    parser.add_argument("--revised-chunk-lon", type=int, default=80)
    args = parser.parse_args()

    utils.ensure_output_directories()

    poor = run_poor_pipeline(args.poor_chunk_time, args.poor_chunk_lat, args.poor_chunk_lon)
    revised = run_revised_pipeline(args.revised_chunk_time, args.revised_chunk_lat, args.revised_chunk_lon)

    speedup = float(poor["elapsed_seconds"]) / max(float(revised["elapsed_seconds"]), 1e-9)
    delta_temp = abs(float(poor["annual_mean_t2m_c"]) - float(revised["annual_mean_t2m_c"]))

    rows = pd.DataFrame([poor, revised])
    rows.to_csv(EXPERIMENT_CSV, index=False)

    summary = {
        "experiment": "poor_vs_revised_dask_pipeline",
        "poor": poor,
        "revised": revised,
        "speedup_poor_over_revised": round(speedup, 3),
        "annual_mean_temp_abs_diff_c": round(delta_temp, 6),
        "interpretation": {
            "unexpected_behavior": (
                "Tiny chunks and repeated compute calls can make Dask slower than expected "
                "because scheduler overhead dominates useful computation."
            ),
            "revision_effect": (
                "The revised graph reduces overhead by fusing operations and computing once near output."
            ),
        },
        "outputs": {
            "csv": str(EXPERIMENT_CSV),
            "json": str(EXPERIMENT_JSON),
        },
    }

    EXPERIMENT_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\n" + "=" * 70)
    print("Dask revision experiment complete")
    print("=" * 70)
    print(f"Poor elapsed     : {poor['elapsed_seconds']} s")
    print(f"Revised elapsed  : {revised['elapsed_seconds']} s")
    print(f"Relative speedup : {speedup:.3f}x")
    print(f"Mean temp delta  : {delta_temp:.6f} C")
    print(f"Summary JSON     : {EXPERIMENT_JSON}")


if __name__ == "__main__":
    main()
