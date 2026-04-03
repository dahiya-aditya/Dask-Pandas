"""
Validate Pandas and Dask outputs for South Asia 2024 analysis.

Compares numeric columns across matching output tables and writes:
- data/results/pandas_vs_dask_validation.csv
- data/results/pandas_vs_dask_validation_summary.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from _bootstrap import ensure_repo_root

ensure_repo_root()

import numpy as np
import pandas as pd

from core import config


BRAJESH_RESULTS_DIR = config.RESULTS_CONTRIBUTOR_DIRS["brajesh"]

PANDAS_DAILY = config.RESULTS_SOUTH_ASIA_DIR / "pandas_daily_metrics.csv"
PANDAS_MONTHLY = config.RESULTS_SOUTH_ASIA_DIR / "pandas_monthly_climatology.csv"
PANDAS_ANOM = config.RESULTS_SOUTH_ASIA_DIR / "pandas_temperature_anomalies.csv"
PANDAS_HEAT = config.RESULTS_SOUTH_ASIA_DIR / "pandas_heat_extreme_daily.csv"

DASK_DAILY = BRAJESH_RESULTS_DIR / "dask_daily_regional_means.csv"
DASK_MONTHLY = BRAJESH_RESULTS_DIR / "dask_large_monthly_climatology.csv"
DASK_ANOM = BRAJESH_RESULTS_DIR / "dask_large_temperature_anomalies.csv"
DASK_HEAT = BRAJESH_RESULTS_DIR / "dask_heat_extreme_daily.csv"

VALIDATION_CSV = BRAJESH_RESULTS_DIR / "pandas_vs_dask_validation.csv"
VALIDATION_JSON = BRAJESH_RESULTS_DIR / "pandas_vs_dask_validation_summary.json"


def _read_table(path: Path, *, parse_first_as_datetime: bool) -> pd.DataFrame:
    """Read CSV and use first column as index."""
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    table = pd.read_csv(path)
    if table.empty:
        raise ValueError(f"Empty file: {path}")

    index_col = table.columns[0]
    if parse_first_as_datetime:
        table[index_col] = pd.to_datetime(table[index_col])
    table = table.set_index(index_col)
    return table.sort_index()


def _compute_metrics(ref: pd.Series, cand: pd.Series, atol: float, rtol: float) -> dict[str, float | bool]:
    """Compute difference metrics and tolerance pass/fail for aligned numeric series."""
    diff = (cand - ref).to_numpy(dtype=np.float64)
    ref_vals = ref.to_numpy(dtype=np.float64)

    abs_diff = np.abs(diff)
    mae = float(np.nanmean(abs_diff))
    max_abs = float(np.nanmax(abs_diff))
    rmse = float(np.sqrt(np.nanmean(diff**2)))

    denom = np.maximum(np.abs(ref_vals), 1e-12)
    rel = abs_diff / denom
    max_rel = float(np.nanmax(rel))

    within = np.isclose(cand.to_numpy(dtype=np.float64), ref_vals, rtol=rtol, atol=atol, equal_nan=True)
    pass_rate = float(np.mean(within))
    passed = bool(np.all(within))

    return {
        "mae": mae,
        "max_abs": max_abs,
        "rmse": rmse,
        "max_rel": max_rel,
        "pass_rate": pass_rate,
        "passed": passed,
    }


def _compare_tables(
    *,
    label: str,
    pandas_path: Path,
    dask_path: Path,
    parse_datetime_index: bool,
    atol: float,
    rtol: float,
) -> list[dict[str, object]]:
    """Compare one Pandas table and one Dask table over common numeric columns."""
    p = _read_table(pandas_path, parse_first_as_datetime=parse_datetime_index)
    d = _read_table(dask_path, parse_first_as_datetime=parse_datetime_index)

    common_index = p.index.intersection(d.index)
    if len(common_index) == 0:
        raise ValueError(f"No overlapping index entries for {label}")

    p = p.loc[common_index]
    d = d.loc[common_index]

    common_cols = [c for c in p.columns if c in d.columns]
    if not common_cols:
        raise ValueError(f"No overlapping columns for {label}")

    rows: list[dict[str, object]] = []
    for col in common_cols:
        if not (pd.api.types.is_numeric_dtype(p[col]) and pd.api.types.is_numeric_dtype(d[col])):
            continue
        m = _compute_metrics(p[col], d[col], atol=atol, rtol=rtol)
        rows.append(
            {
                "table": label,
                "column": col,
                "rows_compared": int(len(common_index)),
                "mae": m["mae"],
                "max_abs": m["max_abs"],
                "rmse": m["rmse"],
                "max_rel": m["max_rel"],
                "pass_rate": m["pass_rate"],
                "passed": m["passed"],
            }
        )

    return rows


def main() -> None:
    """Run all validations and persist detailed report."""
    BRAJESH_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    parser = argparse.ArgumentParser(description="Validate Pandas and Dask result consistency")
    parser.add_argument("--atol", type=float, default=1e-3, help="Absolute tolerance")
    parser.add_argument("--rtol", type=float, default=1e-3, help="Relative tolerance")
    args = parser.parse_args()

    atol = float(args.atol)
    rtol = float(args.rtol)

    checks = [
        ("daily_regional_means", PANDAS_DAILY, DASK_DAILY, True),
        ("monthly_climatology", PANDAS_MONTHLY, DASK_MONTHLY, False),
        ("temperature_anomalies", PANDAS_ANOM, DASK_ANOM, True),
        ("heat_extremes", PANDAS_HEAT, DASK_HEAT, True),
    ]

    all_rows: list[dict[str, object]] = []
    for label, p_path, d_path, parse_dt in checks:
        try:
            all_rows.extend(
                _compare_tables(
                    label=label,
                    pandas_path=p_path,
                    dask_path=d_path,
                    parse_datetime_index=parse_dt,
                    atol=atol,
                    rtol=rtol,
                )
            )
        except FileNotFoundError as exc:
            print(f"Skipping {label}: {exc}")
            continue

    if not all_rows:
        raise FileNotFoundError(
            "No comparable Pandas/Dask output files found. "
            "Run south_asia_2024_dask_analysis.py first."
        )

    report = pd.DataFrame(all_rows)
    report = report.sort_values(["table", "column"]).reset_index(drop=True)
    report.to_csv(VALIDATION_CSV, index=False)

    summary = {
        "tolerances": {"atol": atol, "rtol": rtol},
        "checks": int(report.shape[0]),
        "tables": sorted(report["table"].unique().tolist()),
        "all_passed": bool(report["passed"].all()),
        "min_pass_rate": float(report["pass_rate"].min()),
        "max_abs_overall": float(report["max_abs"].max()),
        "max_rel_overall": float(report["max_rel"].max()),
        "report_csv": str(VALIDATION_CSV),
    }
    VALIDATION_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\n" + "=" * 70)
    print("Pandas vs Dask validation complete")
    print("=" * 70)
    print(f"Checks run: {summary['checks']}")
    print(f"All passed: {summary['all_passed']}")
    print(f"Min pass rate: {summary['min_pass_rate']:.4f}")
    print(f"Max absolute diff: {summary['max_abs_overall']:.6f}")
    print(f"Max relative diff: {summary['max_rel_overall']:.6f}")
    print(f"Report: {VALIDATION_CSV}")
    print(f"Summary: {VALIDATION_JSON}")


if __name__ == "__main__":
    main()
