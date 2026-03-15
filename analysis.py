from __future__ import annotations
"""Minimal 464 MB ERA5 Pandas analysis for Central India.

This script extracts the provided archive payload, computes regional aggregates,
exports summary CSV/JSON files, and saves three figures.
"""

import json
from pathlib import Path
import zipfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr

RAW_DIR = Path("data/raw")
RESULTS_DIR = Path("data/results")
FIGURES_DIR = Path("data/figures")

CONSISTENT_PAYLOAD = RAW_DIR / "era5_consistent_slice_download.nc"
CONSISTENT_EXTRACT_DIR = RAW_DIR / "era5_consistent_slice_extracted"
CONSISTENT_INSTANT_FILE = CONSISTENT_EXTRACT_DIR / "data_stream-oper_stepType-instant.nc"
CONSISTENT_ACCUM_FILE = CONSISTENT_EXTRACT_DIR / "data_stream-oper_stepType-accum.nc"

REGION = {
    "region_name": "Central India",
    "north": 30,
    "south": 20,
    "west": 75,
    "east": 85,
}


def ensure_dirs() -> None:
    """Create output and extraction directories if they do not exist."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    CONSISTENT_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)


def extract_zip_payload(archive_path: Path, extract_dir: Path) -> list[Path]:
    """Extract NetCDF members from the ERA5 zip payload into `extract_dir`."""
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive not found: {archive_path}")

    with zipfile.ZipFile(archive_path) as zf:
        nc_members = [name for name in zf.namelist() if name.lower().endswith(".nc")]
        if not nc_members:
            raise RuntimeError(f"Archive contains no NetCDF files: {archive_path}")

        extracted_paths: list[Path] = []
        for member in nc_members:
            output_path = extract_dir / Path(member).name
            if not output_path.exists():
                with zf.open(member) as src, output_path.open("wb") as dst:
                    dst.write(src.read())
            extracted_paths.append(output_path)
    return extracted_paths


def _open_dataset(path: Path) -> xr.Dataset:
    """Open one NetCDF file using netcdf4 backend."""
    return xr.open_dataset(path, engine="netcdf4")


def load_consistent_dataset() -> xr.Dataset:
    """Load merged instant+accum datasets for the 464 MB payload."""
    extract_zip_payload(CONSISTENT_PAYLOAD, CONSISTENT_EXTRACT_DIR)
    instant = _open_dataset(CONSISTENT_INSTANT_FILE)
    accum = _open_dataset(CONSISTENT_ACCUM_FILE)
    return xr.merge([instant, accum], compat="override", join="outer")


def save_figure(fig: plt.Figure, filename: str) -> str:
    """Save figure as both high-res PNG and vector PDF."""
    output_path = FIGURES_DIR / filename
    fig.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    fig.savefig(output_path.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)
    return str(output_path)


def plot_consistent_analysis(monthly: pd.DataFrame, seasonal: pd.DataFrame, daily: pd.DataFrame) -> dict[str, str]:
    """Create three compact figures for monthly, seasonal, and first-year views."""
    figure_paths: dict[str, str] = {}

    fig, axes = plt.subplots(3, 1, figsize=(11, 8), sharex=True)
    axes[0].plot(monthly["month"], monthly["t2m_c_mean"], marker="o", color="#c44e52", linewidth=2)
    axes[0].set_ylabel("Temperature (C)")
    axes[0].set_title("Central India sampled-day seasonal cycle")

    axes[1].plot(monthly["month"], monthly["sp_hpa_mean"], marker="s", color="#4c72b0", linewidth=1.7)
    axes[1].set_ylabel("Pressure (hPa)")

    axes[2].bar(monthly["month"], monthly["tp_mm_total"], color="#55a868", alpha=0.85)
    axes[2].set_ylabel("Precipitation (mm/day)")
    axes[2].set_xlabel("Month")
    axes[2].set_xticks(range(1, 13))
    axes[2].set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    figure_paths["monthly_cycle"] = save_figure(fig, "monthly_cycle.png")

    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    axes[0].bar(seasonal["season"], seasonal["t2m_c_mean"], color=["#55a868", "#c44e52"])
    axes[0].set_title("Temperature by season group")
    axes[0].set_ylabel("Temperature (C)")
    axes[0].tick_params(axis="x", rotation=15)

    axes[1].bar(seasonal["season"], seasonal["tp_mm_total_mean"], color=["#55a868", "#4c72b0"])
    axes[1].set_title("Rainfall by season group")
    axes[1].set_ylabel("Mean daily precipitation (mm)")
    axes[1].tick_params(axis="x", rotation=15)
    figure_paths["monsoon_comparison"] = save_figure(fig, "monsoon_comparison.png")

    year_slice = daily[daily["year"] == daily["year"].min()].copy()
    fig, axes = plt.subplots(2, 1, figsize=(10.5, 6.2), sharex=True)
    axes[0].plot(year_slice["date"], year_slice["t2m_c_mean"], color="#dd8452", linewidth=2)
    axes[0].set_ylabel("Temperature (C)")
    axes[0].set_title(f"Central India sampled days in {int(year_slice['year'].min())}")

    axes[1].bar(year_slice["date"], year_slice["tp_mm_total"], color="#4c72b0", alpha=0.85, width=2.2)
    axes[1].set_xlabel("Date")
    axes[1].set_ylabel("Precipitation (mm/day)")
    figure_paths["first_year_timeseries"] = save_figure(fig, "first_year_timeseries.png")

    return figure_paths


def run_consistent_subset_analysis() -> dict:
    """Run the full 464 MB analysis and write CSV/JSON/figure outputs."""
    ds = load_consistent_dataset()

    subset = ds.sel(
        latitude=slice(REGION["north"], REGION["south"]),
        longitude=slice(REGION["west"], REGION["east"]),
    )
    df = subset[["t2m", "sp", "tp"]].to_dataframe().reset_index()

    df["valid_time"] = pd.to_datetime(df["valid_time"])
    df["date"] = df["valid_time"].dt.floor("D")
    df["month"] = df["valid_time"].dt.month
    df["year"] = df["valid_time"].dt.year
    df["t2m_c"] = df["t2m"] - 273.15
    df["sp_hpa"] = df["sp"] / 100.0
    df["tp_mm"] = df["tp"] * 1000.0

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

    monthly = (
        daily.groupby("month", as_index=False)
        .agg(
            t2m_c_mean=("t2m_c_mean", "mean"),
            sp_hpa_mean=("sp_hpa_mean", "mean"),
            tp_mm_total=("tp_mm_total", "mean"),
        )
        .sort_values("month")
    )

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

    summary = {
        "analysis": "Central India seasonal heating and rainfall from the 464 MB archive",
        "source_archive": str(CONSISTENT_PAYLOAD),
        "subset_region": REGION,
        "rows": int(df.shape[0]),
        "memory_mb": float(df.memory_usage(deep=True).sum() / (1024**2)),
        "time_steps": int(subset.sizes["valid_time"]),
        "grid_shape": {
            "latitude": int(subset.sizes["latitude"]),
            "longitude": int(subset.sizes["longitude"]),
        },
        "mean_t2m_c": float(daily["t2m_c_mean"].mean()),
        "mean_sp_hpa": float(daily["sp_hpa_mean"].mean()),
        "mean_daily_tp_mm": float(daily["tp_mm_total"].mean()),
        "temp_pressure_correlation": float(daily["t2m_c_mean"].corr(daily["sp_hpa_mean"])),
    }

    regional_time.to_csv(RESULTS_DIR / "regional_time_means.csv", index=False)
    daily.to_csv(RESULTS_DIR / "daily_means.csv", index=False)
    monthly.to_csv(RESULTS_DIR / "monthly_climatology.csv", index=False)
    seasonal.to_csv(RESULTS_DIR / "seasonal_comparison.csv", index=False)

    summary["figure_paths"] = plot_consistent_analysis(monthly, seasonal, daily)
    (RESULTS_DIR / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    subset.close()
    ds.close()
    return summary


def main() -> None:
    ensure_dirs()
    summary = run_consistent_subset_analysis()

    print("Saved Pandas outputs for the 464 MB archive subset.")
    print(f"- {RESULTS_DIR / 'regional_time_means.csv'}")
    print(f"- {RESULTS_DIR / 'daily_means.csv'}")
    print(f"- {RESULTS_DIR / 'monthly_climatology.csv'}")
    print(f"- {RESULTS_DIR / 'seasonal_comparison.csv'}")
    print(f"- {RESULTS_DIR / 'summary.json'}")
    print(f"- {FIGURES_DIR / 'monthly_cycle.png'}")
    print(f"- {FIGURES_DIR / 'monsoon_comparison.png'}")
    print(f"- {FIGURES_DIR / 'first_year_timeseries.png'}")
    print()
    print(f"Rows processed: {summary['rows']}")
    print(f"DataFrame memory (MB): {summary['memory_mb']:.1f}")


if __name__ == "__main__":
    main()
