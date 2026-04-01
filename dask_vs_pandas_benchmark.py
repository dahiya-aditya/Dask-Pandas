"""
Assignment benchmark orchestrator.

Runs existing analysis scripts plus the aligned Dask script, then writes:
1) runtime benchmark table
2) Dask vs Pandas consistency table
3) assignment summary JSON

This keeps benchmarking grounded in your real project scripts.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import psutil

from core import config, utils


ROOT = Path(__file__).resolve().parent
RESULTS_DIR = config.RESULTS_DIR
BENCHMARK_RESULTS_DIR = RESULTS_DIR / "benchmark"
SOUTH_ASIA_RESULTS_DIR = RESULTS_DIR / "south_asia"

RUNTIME_CSV = BENCHMARK_RESULTS_DIR / "runtime_table.csv"
CONSISTENCY_CSV = BENCHMARK_RESULTS_DIR / "consistency_daily.csv"
ANOMALY_CONSISTENCY_CSV = BENCHMARK_RESULTS_DIR / "consistency_anomaly.csv"
CHUNK_SWEEP_CSV = BENCHMARK_RESULTS_DIR / "chunk_sweep_table.csv"
SUMMARY_JSON = BENCHMARK_RESULTS_DIR / "summary.json"

PANDAS_DAILY = SOUTH_ASIA_RESULTS_DIR / "pandas_daily_metrics.csv"
DASK_DAILY = SOUTH_ASIA_RESULTS_DIR / "dask_daily_metrics.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run assignment benchmark across existing scripts.")
    parser.add_argument(
        "--sample-days",
        type=int,
        default=31,
        help="Sample window for Dask aligned script; 0 uses full span.",
    )
    parser.add_argument(
        "--chunk-hours",
        type=str,
        default="96",
        help="Chunk size for Dask aligned script.",
    )
    parser.add_argument(
        "--chunk-sweep",
        type=str,
        default="",
        help="Comma-separated chunk settings, e.g. '24,48,96,192,auto'.",
    )
    parser.add_argument(
        "--skip-central",
        action="store_true",
        help="Skip central_india_464mb_analysis.py timing run.",
    )
    parser.add_argument(
        "--no-recompute-pandas",
        action="store_true",
        help="Run south_asia_2024_large_analysis.py without --recompute.",
    )
    return parser.parse_args()


def run_script(label: str, cmd: list[str]) -> dict[str, object]:
    print(f"\nRunning: {label}")
    print(" ".join(cmd))

    start = time.perf_counter()
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    peak_rss = 0
    ps_proc = psutil.Process(proc.pid)
    while proc.poll() is None:
        try:
            peak_rss = max(peak_rss, int(ps_proc.memory_info().rss))
        except psutil.Error:
            pass
        time.sleep(0.2)

    stdout, stderr = proc.communicate()
    elapsed = time.perf_counter() - start

    if proc.returncode == 0:
        print(f"  ok in {elapsed:.2f}s")
    else:
        print(f"  failed in {elapsed:.2f}s (code={proc.returncode})")

    stderr_tail = "\n".join(stderr.strip().splitlines()[-8:]) if stderr else ""

    return {
        "script": label,
        "command": " ".join(cmd),
        "return_code": int(proc.returncode),
        "runtime_seconds": round(elapsed, 3),
        "peak_rss_mb": round(peak_rss / (1024 * 1024), 2),
        "stderr_tail": stderr_tail,
    }


def load_daily(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Expected output not found: {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Output is empty: {path}")

    time_col = df.columns[0]
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.rename(columns={time_col: "time"})
    return df


def anomalies_from_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Compute monthly anomalies from a daily metrics table."""
    out = df.copy()
    out["time"] = pd.to_datetime(out["time"])
    idx = out["time"].dt.month

    value_cols = [c for c in out.columns if c != "time"]
    anom = out[["time"]].copy()
    for col in value_cols:
        anom[f"{col}_anom"] = out[col] - out[col].groupby(idx).transform("mean")

    return anom


def build_consistency_table(pandas_df: pd.DataFrame, dask_df: pd.DataFrame, candidate_cols: list[str]) -> pd.DataFrame:
    compare_cols = [c for c in candidate_cols if c in pandas_df.columns and c in dask_df.columns]

    if not compare_cols:
        raise ValueError("No overlapping columns found for Dask vs Pandas consistency.")

    merged = pandas_df[["time"] + compare_cols].merge(
        dask_df[["time"] + compare_cols],
        on="time",
        how="inner",
        suffixes=("_pandas", "_dask"),
    )

    rows: list[dict[str, object]] = []
    for col in compare_cols:
        diff = (merged[f"{col}_pandas"] - merged[f"{col}_dask"]).abs()
        rows.append(
            {
                "metric": col,
                "rows_compared": int(len(diff)),
                "max_abs_diff": float(diff.max()),
                "mean_abs_diff": float(diff.mean()),
            }
        )

    return pd.DataFrame(rows)


def write_assignment_summary(runtime_df: pd.DataFrame, consistency_df: pd.DataFrame, sample_days: int, chunk_hours: int) -> None:
    all_ok = bool((runtime_df["return_code"] == 0).all())

    summary = {
        "run_configuration": {
            "sample_days": sample_days,
            "chunk_hours": chunk_hours,
        },
        "scripts_ran": runtime_df.to_dict(orient="records"),
        "all_scripts_success": all_ok,
        "consistency": consistency_df.to_dict(orient="records"),
        "assignment_status": {
            "large_dataset_dask_pipeline": "done" if all_ok else "partial",
            "existing_scripts_benchmarked": "done" if all_ok else "partial",
            "dask_vs_pandas_consistency_table": "done",
            "runtime_table_generated": "done",
        },
        "outputs": {
            "runtime_csv": str(RUNTIME_CSV),
            "consistency_csv": str(CONSISTENCY_CSV),
            "anomaly_consistency_csv": str(ANOMALY_CONSISTENCY_CSV),
            "chunk_sweep_csv": str(CHUNK_SWEEP_CSV),
            "summary_json": str(SUMMARY_JSON),
        },
    }

    SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def parse_chunk_sweep(raw: str) -> list[str]:
    if not raw.strip():
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def run_chunk_sweep(py: str, args: argparse.Namespace, pandas_daily: pd.DataFrame, pandas_anom: pd.DataFrame) -> pd.DataFrame:
    sweep_rows: list[dict[str, object]] = []
    daily_cols = ["t2m_c", "tcc", "ssr", "str_nlw", "net_rad"]
    anom_cols = ["t2m_c_anom", "net_rad_anom"]

    for chunk_spec in parse_chunk_sweep(args.chunk_sweep):
        dask_cmd = [
            py,
            "south_asia_2024_dask_aligned.py",
            "--sample-days",
            str(args.sample_days),
            "--chunk-hours",
            chunk_spec,
        ]
        row = run_script(f"south_asia_2024_dask_aligned.py [chunk={chunk_spec}]", dask_cmd)

        if int(row["return_code"]) != 0:
            row["daily_t2m_mean_abs_diff"] = None
            row["anom_t2m_mean_abs_diff"] = None
            sweep_rows.append(row)
            continue

        dask_daily = load_daily(DASK_DAILY)
        dask_anom = anomalies_from_daily(dask_daily)

        daily_cmp = build_consistency_table(pandas_daily, dask_daily, daily_cols)
        anom_cmp = build_consistency_table(pandas_anom, dask_anom, anom_cols)

        t2m_daily = daily_cmp[daily_cmp["metric"] == "t2m_c"]["mean_abs_diff"].iloc[0]
        t2m_anom = anom_cmp[anom_cmp["metric"] == "t2m_c_anom"]["mean_abs_diff"].iloc[0]

        row["chunk_spec"] = chunk_spec
        row["daily_t2m_mean_abs_diff"] = float(t2m_daily)
        row["anom_t2m_mean_abs_diff"] = float(t2m_anom)
        sweep_rows.append(row)

    sweep_df = pd.DataFrame(sweep_rows)
    sweep_df.to_csv(CHUNK_SWEEP_CSV, index=False)
    return sweep_df


def main() -> None:
    args = parse_args()
    utils.ensure_output_directories()
    BENCHMARK_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    commands: list[tuple[str, list[str]]] = []
    py = sys.executable

    if not args.skip_central:
        commands.append(("central_india_464mb_analysis.py", [py, "central_india_464mb_analysis.py"]))

    south_pandas_cmd = [py, "south_asia_2024_large_analysis.py"]
    if not args.no_recompute_pandas:
        south_pandas_cmd.append("--recompute")
    commands.append(("south_asia_2024_large_analysis.py", south_pandas_cmd))

    dask_cmd = [
        py,
        "south_asia_2024_dask_aligned.py",
        "--sample-days",
        str(args.sample_days),
        "--chunk-hours",
        str(args.chunk_hours),
    ]
    commands.append(("south_asia_2024_dask_aligned.py", dask_cmd))

    runtime_rows: list[dict[str, object]] = []
    for label, cmd in commands:
        runtime_rows.append(run_script(label, cmd))

    runtime_df = pd.DataFrame(runtime_rows)
    runtime_df.to_csv(RUNTIME_CSV, index=False)

    pandas_df = load_daily(PANDAS_DAILY)
    dask_df = load_daily(DASK_DAILY)
    consistency_df = build_consistency_table(
        pandas_df,
        dask_df,
        candidate_cols=["t2m_c", "tcc", "ssr", "str_nlw", "net_rad"],
    )
    consistency_df.to_csv(CONSISTENCY_CSV, index=False)

    pandas_anom_df = anomalies_from_daily(pandas_df)
    dask_anom_df = anomalies_from_daily(dask_df)
    anomaly_consistency_df = build_consistency_table(
        pandas_anom_df,
        dask_anom_df,
        candidate_cols=["t2m_c_anom", "tcc_anom", "ssr_anom", "str_nlw_anom", "net_rad_anom"],
    )
    anomaly_consistency_df.to_csv(ANOMALY_CONSISTENCY_CSV, index=False)

    if parse_chunk_sweep(args.chunk_sweep):
        run_chunk_sweep(py, args, pandas_df, pandas_anom_df)

    write_assignment_summary(
        runtime_df=runtime_df,
        consistency_df=consistency_df,
        sample_days=args.sample_days,
        chunk_hours=args.chunk_hours,
    )

    print("\nSaved benchmark artifacts:")
    print(f"  - {RUNTIME_CSV}")
    print(f"  - {CONSISTENCY_CSV}")
    print(f"  - {ANOMALY_CONSISTENCY_CSV}")
    if parse_chunk_sweep(args.chunk_sweep):
        print(f"  - {CHUNK_SWEEP_CSV}")
    print(f"  - {SUMMARY_JSON}")


if __name__ == "__main__":
    main()
