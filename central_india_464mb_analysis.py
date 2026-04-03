"""
ERA5 464 MB seasonal analysis - Central India subset.

Computes regional aggregates, temporal statistics, and energy metrics for
the Era5 consistent slice archive (464 MB). Exports results as CSV/JSON
and generates descriptive figures.

Before running: Ensure extracted NetCDF files exist via:
    python -m core.housekeeping
"""

from __future__ import annotations

import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import netCDF4 as nc
import numpy as np
import pandas as pd

from core import config, utils
from core.config import CONSISTENT_SLICE, REGIONS


CENTRAL_RESULTS_DIR = config.RESULTS_DIR / "central_india"
CENTRAL_DAILY_CSV = CENTRAL_RESULTS_DIR / "daily_metrics.csv"
CENTRAL_SUMMARY_JSON = CENTRAL_RESULTS_DIR / "summary.json"


def load_consistent_region_dataframe(
    region: dict[str, float | int],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Load Central India variables using netCDF4 and return a flattened DataFrame.

    Parameters
    ----------
    region : dict[str, float | int]
        Region bounds with keys: north, south, west, east.

    Returns
    -------
    tuple[pd.DataFrame, dict[str, int]]
        Flattened DataFrame and grid metadata.
    """
    data_dir = CONSISTENT_SLICE["data_dir"]
    instant_path = data_dir / CONSISTENT_SLICE["instant_file"]
    accum_path = data_dir / CONSISTENT_SLICE["accum_file"]

    if not instant_path.exists() or not accum_path.exists():
        raise FileNotFoundError(
            f"Missing extracted NetCDF files in {data_dir}\n"
            f"Run: python -m core.housekeeping"
        )

    def read_subset_with_retry(var: nc.Variable, label: str, lat_sl: slice, lon_sl: slice) -> np.ndarray:
        """Read a variable subset and retry once if interrupted during first read."""
        for attempt in range(2):
            try:
                arr = var[:, lat_sl, lon_sl]
                if np.ma.isMaskedArray(arr):
                    arr = arr.filled(np.nan)
                return np.asarray(arr, dtype=np.float32)
            except KeyboardInterrupt:
                if attempt == 0:
                    print(f"  {label} read interrupted; retrying once ...")
                    continue
                raise

        raise RuntimeError(f"Failed to read {label} after retry")

    with nc.Dataset(instant_path) as ds_i:
        lat_full = ds_i.variables["latitude"][:]
        lon_full = ds_i.variables["longitude"][:]
        time_raw = ds_i.variables["valid_time"][:]

        lat_idx = np.where((lat_full >= region["south"]) & (lat_full <= region["north"]))[0]
        lon_idx = np.where((lon_full >= region["west"]) & (lon_full <= region["east"]))[0]
        if len(lat_idx) == 0 or len(lon_idx) == 0:
            raise ValueError(f"Region has no valid grid cells: {region}")

        lat_sl = slice(int(lat_idx.min()), int(lat_idx.max()) + 1)
        lon_sl = slice(int(lon_idx.min()), int(lon_idx.max()) + 1)

        lats = np.array(lat_full[lat_sl], dtype=np.float32)
        lons = np.array(lon_full[lon_sl], dtype=np.float32)
        t2m = read_subset_with_retry(ds_i.variables["t2m"], "t2m", lat_sl, lon_sl)
        sp = read_subset_with_retry(ds_i.variables["sp"], "sp", lat_sl, lon_sl)

    with nc.Dataset(accum_path) as ds_a:
        tp = read_subset_with_retry(ds_a.variables["tp"], "tp", lat_sl, lon_sl)

    times = pd.to_datetime(np.array(time_raw), unit="s", utc=True).tz_localize(None)

    nt, ny, nx = t2m.shape
    cells = nt * ny * nx
    time_flat = np.repeat(times.values, ny * nx)
    lat_flat = np.tile(np.repeat(lats, nx), nt)
    lon_flat = np.tile(lons, nt * ny)

    df = pd.DataFrame(
        {
            "valid_time": time_flat,
            "latitude": lat_flat,
            "longitude": lon_flat,
            "t2m": t2m.reshape(cells),
            "sp": sp.reshape(cells),
            "tp": tp.reshape(cells),
        }
    )

    grid_shape = {
        "time_steps": int(nt),
        "latitude": int(ny),
        "longitude": int(nx),
    }
    return df, grid_shape


def plot_analysis_figures(
    monthly: pd.DataFrame,
    seasonal: pd.DataFrame,
    daily: pd.DataFrame,
) -> dict[str, str]:
    """
    Generate three analysis figures: seasonal cycle, monsoon comparison, first-year timeline.
    
    Parameters
    ----------
    monthly : pd.DataFrame
        Monthly aggregated statistics
    seasonal : pd.DataFrame
        Seasonal comparison (monsoon vs other)
    daily : pd.DataFrame
        Daily statistics
        
    Returns
    -------
    dict[str, str]
        Mapping of figure name to file path
    """
    figure_paths: dict[str, str] = {}

    # Figure 1: Monthly seasonal cycle
    fig, axes = plt.subplots(3, 1, figsize=(11, 8), sharex=True)
    axes[0].plot(monthly["month"], monthly["t2m_c_mean"], marker="o", color="#c44e52", linewidth=2)
    axes[0].set_ylabel("Temperature (°C)")
    axes[0].set_title("Central India sampled-day seasonal cycle")

    axes[1].plot(monthly["month"], monthly["sp_hpa_mean"], marker="s", color="#4c72b0", linewidth=1.7)
    axes[1].set_ylabel("Pressure (hPa)")

    axes[2].bar(monthly["month"], monthly["tp_mm_total"], color="#55a868", alpha=0.85)
    axes[2].set_ylabel("Precipitation (mm/day)")
    axes[2].set_xlabel("Month")
    axes[2].set_xticks(range(1, 13))
    axes[2].set_xticklabels(config.MONTH_ABBR)
    figure_paths["monthly_cycle"] = str(utils.save_figure(fig, "monthly_cycle"))

    # Figure 2: Monsoon vs other season comparison
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    axes[0].bar(seasonal["season"], seasonal["t2m_c_mean"], color=["#55a868", "#c44e52"])
    axes[0].set_title("Temperature by season group")
    axes[0].set_ylabel("Temperature (°C)")
    axes[0].tick_params(axis="x", rotation=15)

    axes[1].bar(seasonal["season"], seasonal["tp_mm_total_mean"], color=["#55a868", "#4c72b0"])
    axes[1].set_title("Rainfall by season group")
    axes[1].set_ylabel("Mean daily precipitation (mm)")
    axes[1].tick_params(axis="x", rotation=15)
    figure_paths["monsoon_comparison"] = str(utils.save_figure(fig, "monsoon_comparison"))

    # Figure 3: First year timeseries
    year_slice = daily[daily["year"] == daily["year"].min()].copy()
    x = np.arange(len(year_slice), dtype=np.int32)
    month_key = year_slice["date"].dt.to_period("M")
    month_change = month_key.ne(month_key.shift(1))
    tick_idx = np.where(month_change.to_numpy())[0]
    tick_labels = year_slice["date"].dt.strftime("%b %Y").iloc[tick_idx]

    fig, axes = plt.subplots(2, 1, figsize=(10.5, 6.2))
    axes[0].plot(x, year_slice["t2m_c_mean"], color="#dd8452", linewidth=2)
    axes[0].set_ylabel("Temperature (°C)")
    axes[0].set_title(f"Central India sampled days in {int(year_slice['year'].min())}")
    axes[0].set_xlim(0, len(year_slice) - 1)
    axes[0].set_xticks(tick_idx)
    axes[0].set_xticklabels([])

    axes[1].bar(x, year_slice["tp_mm_total"], color="#4c72b0", alpha=0.85, width=0.8)
    axes[1].set_xlim(0, len(year_slice) - 1)
    axes[1].set_xticks(tick_idx)
    axes[1].set_xticklabels(tick_labels, rotation=30, ha="right", fontsize=9)
    axes[1].set_xlabel("Date")
    axes[1].set_ylabel("Precipitation (mm/day)")
    fig.subplots_adjust(left=0.1, right=0.95, top=0.92, bottom=0.15, hspace=0.3)
    figure_paths["first_year_timeseries"] = str(utils.save_figure(fig, "first_year_timeseries"))

    return figure_paths


def run_analysis() -> dict:
    """
    Execute full analysis: load data, compute aggregates, export results.
    
    Returns
    -------
    dict
        Summary of analysis metadata and results
    """
    print("\n" + "=" * 70)
    print(f"Analyzing {CONSISTENT_SLICE['name']}")
    print("=" * 70)

    # Load dataset
    print("\nLoading dataset ...")

    # Extract Central India region
    region = REGIONS["central_india"]
    print(f"Extracting region: {region['name']} ({region['south']}-{region['north']} N, {region['west']}-{region['east']} E)")

    # Load and flatten region data into a DataFrame
    df, grid_shape = load_consistent_region_dataframe(region)

    # Time and unit conversions
    df["valid_time"] = pd.to_datetime(df["valid_time"])
    df["date"] = df["valid_time"].dt.floor("D")
    df["month"] = df["valid_time"].dt.month
    df["year"] = df["valid_time"].dt.year
    df["t2m_c"] = df["t2m"] - 273.15
    df["sp_hpa"] = df["sp"] / 100.0
    df["tp_mm"] = df["tp"] * 1000.0

    # Time-mean aggregation
    regional_time = (
        df.groupby("valid_time", as_index=False)
        .agg(
            t2m_c_mean=("t2m_c", "mean"),
            sp_hpa_mean=("sp_hpa", "mean"),
            tp_mm_mean=("tp_mm", "mean"),
        )
        .sort_values("valid_time")
    )
    regional_time["date"] = regional_time["valid_time"].dt.floor("D")
    regional_time["month"] = regional_time["valid_time"].dt.month
    regional_time["year"] = regional_time["valid_time"].dt.year

    # Daily aggregation
    daily = (
        regional_time.groupby("date", as_index=False)
        .agg(
            t2m_c_mean=("t2m_c_mean", "mean"),
            sp_hpa_mean=("sp_hpa_mean", "mean"),
            tp_mm_total=("tp_mm_mean", "sum"),
        )
        .sort_values("date")
    )
    daily["month"] = daily["date"].dt.month
    daily["year"] = daily["date"].dt.year

    # Monthly climatology
    monthly = (
        daily.groupby("month", as_index=False)
        .agg(
            t2m_c_mean=("t2m_c_mean", "mean"),
            sp_hpa_mean=("sp_hpa_mean", "mean"),
            tp_mm_total=("tp_mm_total", "mean"),
        )
        .sort_values("month")
    )

    # Monsoon vs other season comparison
    monsoon_mask = daily["month"].isin([6, 7, 8, 9])
    seasonal = pd.DataFrame(
        {
            "season": ["Monsoon sampled days", "Other sampled days"],
            "day_count": [int(monsoon_mask.sum()), int((~monsoon_mask).sum())],
            "t2m_c_mean": [
                float(daily.loc[monsoon_mask, "t2m_c_mean"].mean()),
                float(daily.loc[~monsoon_mask, "t2m_c_mean"].mean()),
            ],
            "sp_hpa_mean": [
                float(daily.loc[monsoon_mask, "sp_hpa_mean"].mean()),
                float(daily.loc[~monsoon_mask, "sp_hpa_mean"].mean()),
            ],
            "tp_mm_total_mean": [
                float(daily.loc[monsoon_mask, "tp_mm_total"].mean()),
                float(daily.loc[~monsoon_mask, "tp_mm_total"].mean()),
            ],
        }
    )

    # Build summary metadata
    summary = {
        "dataset": CONSISTENT_SLICE["name"],
        "region": region,
        "rows_processed": int(df.shape[0]),
        "memory_mb": float(df.memory_usage(deep=True).sum() / (1024**2)),
        "time_steps": grid_shape["time_steps"],
        "grid_shape": {
            "latitude": grid_shape["latitude"],
            "longitude": grid_shape["longitude"],
        },
        "temperature_summary": {
            "mean_t2m_c": float(daily["t2m_c_mean"].mean()),
            "max_t2m_c": float(daily["t2m_c_mean"].max()),
            "min_t2m_c": float(daily["t2m_c_mean"].min()),
        },
        "pressure_summary": {
            "mean_sp_hpa": float(daily["sp_hpa_mean"].mean()),
        },
        "precipitation_summary": {
            "mean_daily_tp_mm": float(daily["tp_mm_total"].mean()),
            "total_tp_mm": float(daily["tp_mm_total"].sum()),
        },
        "correlations": {
            "temp_pressure": float(daily["t2m_c_mean"].corr(daily["sp_hpa_mean"])),
        },
    }

    # Save consolidated results
    print("\nSaving results ...")
    CENTRAL_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    daily.to_csv(CENTRAL_DAILY_CSV, index=False)

    summary["figure_paths"] = plot_analysis_figures(monthly, seasonal, daily)
    CENTRAL_SUMMARY_JSON.write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )

    return summary


def main() -> None:
    """Run complete analysis pipeline."""
    utils.ensure_output_directories()
    CENTRAL_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    summary = run_analysis()

    print("\n" + "=" * 70)
    print("Output files saved:")
    print("=" * 70)
    print(f"  CSV Results:")
    print(f"    - {CENTRAL_DAILY_CSV}")
    print(f"  Figures:")
    print(f"    - {config.FIGURES_DIR}/monthly_cycle.pdf")
    print(f"    - {config.FIGURES_DIR}/monsoon_comparison.pdf")
    print(f"    - {config.FIGURES_DIR}/first_year_timeseries.pdf")
    print(f"  Summary:")
    print(f"    - {CENTRAL_SUMMARY_JSON}")
    print()
    print(f"Data summary:")
    print(f"  Rows processed: {summary['rows_processed']:,}")
    print(f"  DataFrame memory: {summary['memory_mb']:.1f} MB")
    print(f"  Mean temperature: {summary['temperature_summary']['mean_t2m_c']:.1f}°C")
    print()


if __name__ == "__main__":
    main()

