"""
Run full project regeneration workflow.

This script can:
1) optionally delete data/results and data/figures
2) run housekeeping to verify/extract raw dataset structure
3) execute canonical and contributor analysis scripts
4) verify key regenerated artifacts exist
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def _run(cmd: list[str]) -> None:
    print("\n" + "=" * 80)
    print("Running:", " ".join(cmd))
    print("=" * 80)
    subprocess.run(cmd, cwd=ROOT, check=True)


def _clean_outputs() -> None:
    for rel in ["data/results", "data/figures"]:
        path = ROOT / rel
        if path.exists():
            shutil.rmtree(path)
            print(f"Removed: {path}")


def _commands(py: str, include_contrib: bool, include_saachi_benchmark: bool) -> list[list[str]]:
    cmds: list[list[str]] = [
        [py, "-m", "core.housekeeping"],
    ]

    # Saachi benchmark is expensive; include only when explicitly requested.
    if include_contrib and include_saachi_benchmark:
        cmds.append([py, "contrib_saachi/temp_agg.py"])

    cmds.extend(
        [
            [py, "central_india_464mb_analysis.py"],
            [py, "south_asia_2024_large_analysis.py", "--recompute"],
            [py, "south_asia_2024_dask_aligned.py", "--sample-days", "0", "--chunk-hours", "96"],
            [
                py,
                "dask_vs_pandas_benchmark.py",
                "--sample-days",
                "0",
                "--chunk-hours",
                "96",
                "--skip-central",
            ],
        ]
    )

    if include_contrib:
        cmds.extend(
            [
                [py, "contrib_adwita/temperature_cloudcover_dask_analysis.py"],
                [py, "contrib_adwita/toposphere_radiation_dask_analysis.py"],
                [py, "contrib_adwita/surface_radiation_dask_analysis.py"],
                [py, "contrib_brajesh/south_asia_2024_dask_analysis.py"],
                [py, "contrib_brajesh/benchmark_dask_chunking.py"],
                [py, "contrib_brajesh/validate_pandas_vs_dask.py"],
                [py, "contrib_brajesh/dask_revision_experiment.py"],
                [py, "contrib_brajesh/plot_anomaly_graphs.py"],
                [py, "contrib_brajesh/generate_case_study_report.py"],
            ]
        )

    return cmds


def _expected(include_contrib: bool, include_saachi_benchmark: bool) -> list[Path]:
    expected = [
        ROOT / "data/results/central_india/daily_metrics.csv",
        ROOT / "data/results/south_asia/pandas_daily_metrics.csv",
        ROOT / "data/results/south_asia/dask_daily_metrics.csv",
        ROOT / "data/results/benchmark/summary.json",
        ROOT / "data/figures/annual_temperature_cycle.pdf",
        ROOT / "data/figures/dask_aligned_daily_t2m.pdf",
    ]

    if include_contrib:
        expected.extend(
            [
                ROOT / "data/figures/contrib_adwita/t2m_global_vs_southasia.pdf",
                ROOT / "data/figures/contrib_adwita/tcc_global_vs_southasia.pdf",
                ROOT / "data/figures/contrib_adwita/ttr_global_vs_southasia.pdf",
                ROOT / "data/figures/contrib_adwita/tsr_global_vs_southasia.pdf",
                ROOT / "data/figures/contrib_adwita/tisr_global_vs_southasia.pdf",
                ROOT / "data/figures/contrib_adwita/str_global_vs_southasia.pdf",
                ROOT / "data/figures/contrib_adwita/ssr_global_vs_southasia.pdf",
                ROOT / "data/results/contrib_brajesh/dask_large_summary.json",
                ROOT / "data/figures/contrib_brajesh/dask_annual_temperature_cycle.pdf",
                ROOT / "data/results/contrib_brajesh/dask_case_study_report.md",
            ]
        )

    if include_contrib and include_saachi_benchmark:
        expected.append(ROOT / "data/figures/contrib_saachi/chunking_benchmark.pdf")

    return expected


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full analysis regeneration workflow")
    parser.add_argument("--clean", action="store_true", help="Delete data/results and data/figures before running")
    parser.add_argument("--canonical-only", action="store_true", help="Run only canonical root workflow")
    parser.add_argument(
        "--include-saachi-benchmark",
        action="store_true",
        help="Include contrib_saachi/temp_agg.py (slow benchmark) in the run",
    )
    args = parser.parse_args()

    include_contrib = not args.canonical_only
    include_saachi_benchmark = bool(args.include_saachi_benchmark)
    py = sys.executable

    if args.clean:
        _clean_outputs()

    for cmd in _commands(
        py,
        include_contrib=include_contrib,
        include_saachi_benchmark=include_saachi_benchmark,
    ):
        _run(cmd)

    missing = [
        p
        for p in _expected(
            include_contrib=include_contrib,
            include_saachi_benchmark=include_saachi_benchmark,
        )
        if not p.exists()
    ]
    print("\n" + "=" * 80)
    print("Verification")
    print("=" * 80)
    if missing:
        print("Missing expected outputs:")
        for p in missing:
            print(" -", p)
        return 1

    print("All expected outputs are present.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
