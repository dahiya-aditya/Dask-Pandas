"""
Minimal Dask-based South Asia analysis aligned with project conventions.

This file is a minimally adapted version of analysis_using_dask.py:
- uses core.config and core.utils paths
- writes outputs to data/results
- keeps analysis scope simple and teammate-friendly
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from core import config, utils
from core.config import LARGE_SOUTH_ASIA, REGIONS


SOUTH_ASIA_RESULTS_DIR = config.RESULTS_DIR / "south_asia"
OUTPUT_DAILY_CSV = SOUTH_ASIA_RESULTS_DIR / "dask_daily_metrics.csv"
OUTPUT_SUMMARY_JSON = SOUTH_ASIA_RESULTS_DIR / "dask_summary.json"


def figure_daily_t2m(daily_df: pd.DataFrame) -> plt.Figure:
    """Simple daily mean temperature figure for aligned Dask output."""
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(pd.to_datetime(daily_df["time"]), daily_df["t2m_c"], lw=1.2, color="#1f77b4")
    ax.set_title("South Asia Daily Mean Temperature (Dask aligned)")
    ax.set_xlabel("Date")
    ax.set_ylabel("t2m (C)")
    ax.grid(alpha=0.3)
    return fig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal aligned Dask analysis for South Asia ERA5.")
    parser.add_argument(
        "--sample-days",
        type=int,
        default=0,
        help="Optional number of days from dataset start to process; 0 uses full span.",
    )
    parser.add_argument(
        "--chunk-hours",
        type=str,
        default="96",
        help="Post-load rechunk size along valid_time, or 'auto' to keep loader chunks.",
    )
    return parser.parse_args()


def build_dataset_paths() -> list[str]:
    data_dir = LARGE_SOUTH_ASIA["data_dir"]
    instant_nc = data_dir / LARGE_SOUTH_ASIA["instant_file"]
    accum_nc = data_dir / LARGE_SOUTH_ASIA["accum_file"]

    if not instant_nc.exists() or not accum_nc.exists():
        raise FileNotFoundError(
            f"Missing NetCDF files in {data_dir}.\nRun: python -m core.housekeeping"
        )

    return [str(instant_nc), str(accum_nc)]


def region_subset(ds: xr.Dataset) -> xr.Dataset:
    region = REGIONS["south_asia"]
    south, north = float(region["south"]), float(region["north"])
    west, east = float(region["west"]), float(region["east"])

    lat0 = float(ds.latitude.isel(latitude=0).values)
    latn = float(ds.latitude.isel(latitude=-1).values)
    if lat0 > latn:
        lat_slice = slice(north, south)
    else:
        lat_slice = slice(south, north)

    lon0 = float(ds.longitude.isel(longitude=0).values)
    lonn = float(ds.longitude.isel(longitude=-1).values)
    if lon0 > lonn:
        lon_slice = slice(east, west)
    else:
        lon_slice = slice(west, east)

    return ds.sel(latitude=lat_slice, longitude=lon_slice)


def _open_chunked_dataset(path: Path, chunks: dict[str, int]) -> xr.Dataset:
    """Open with native on-disk chunks, then rechunk for compute."""
    ds = xr.open_dataset(path, engine="netcdf4", chunks={})
    return ds.chunk(chunks)


def apply_time_window(ds: xr.Dataset, sample_days: int) -> xr.Dataset:
    if sample_days <= 0:
        return ds

    t0 = ds["valid_time"].min()
    t1 = t0 + pd.to_timedelta(sample_days, unit="D")
    return ds.sel(valid_time=slice(t0, t1))


def run(sample_days: int, chunk_spec: str) -> tuple[pd.DataFrame, dict[str, float | int | str]]:
    files = build_dataset_paths()
    instant_nc = Path(files[0])
    accum_nc = Path(files[1])

    print("Opening dataset with Dask...")
    start = time.perf_counter()

    if chunk_spec.lower() == "auto":
        chunks = {"valid_time": 96, "latitude": 80, "longitude": 80}
    else:
        chunk_hours = int(chunk_spec)
        if chunk_hours <= 0:
            raise ValueError("chunk-hours must be > 0, or use 'auto'.")
        chunks = {"valid_time": chunk_hours, "latitude": 80, "longitude": 80}

    ds_i = _open_chunked_dataset(instant_nc, chunks=chunks)
    ds_a = _open_chunked_dataset(accum_nc, chunks=chunks)

    ds = xr.merge(
        [
            ds_i[["t2m", "tcc"]],
            ds_a[["ssr", "str", "tisr", "tsr", "ttr"]],
        ],
        compat="override",
        combine_attrs="override",
    )

    ds = region_subset(ds)
    ds = apply_time_window(ds, sample_days)

    # Keep metrics aligned with the existing large Pandas analysis outputs.
    # Existing Pandas code uses latitude-weighted regional means.
    lat_weights = np.cos(np.deg2rad(ds["latitude"]))
    lat_weights = lat_weights / lat_weights.sum()

    def wmean(field: xr.DataArray) -> xr.DataArray:
        return field.weighted(lat_weights).mean(dim=["latitude", "longitude"])

    t2m_c = ds["t2m"] - 273.15
    t2m_regional = wmean(t2m_c)
    tcc = wmean(ds["tcc"])

    secs_per_step = 6 * 3600
    ssr = wmean(ds["ssr"] / secs_per_step)
    str_nlw = wmean(ds["str"] / secs_per_step)
    tisr = wmean(ds["tisr"] / secs_per_step)
    tsr = wmean(ds["tsr"] / secs_per_step)
    ttr = wmean(ds["ttr"] / secs_per_step)

    six_hourly = xr.Dataset(
        {
            "t2m_c": t2m_regional,
            "tcc": tcc,
            "ssr": ssr,
            "str_nlw": str_nlw,
            "tisr": tisr,
            "tsr": tsr,
            "ttr": ttr,
        }
    )

    daily_mean = six_hourly.resample(valid_time="1D").mean()
    daily_tmax = t2m_regional.resample(valid_time="1D").max().rename("t2m_c_max")
    daily_tmin = t2m_regional.resample(valid_time="1D").min().rename("t2m_c_min")

    daily = xr.merge([daily_mean, daily_tmax, daily_tmin]).to_dataframe()
    daily["net_rad"] = daily["ssr"] + daily["str_nlw"]
    daily["reflected_solar"] = daily["tisr"] - daily["tsr"]
    daily.index.name = "time"

    daily_out = daily.reset_index()

    elapsed = time.perf_counter() - start
    summary = {
        "dataset": LARGE_SOUTH_ASIA["name"],
        "region": REGIONS["south_asia"],
        "rows": int(len(daily_out)),
        "sample_days": int(sample_days),
        "chunk_spec": chunk_spec,
        "runtime_seconds": round(elapsed, 3),
        "mean_t2m_c": float(daily_out["t2m_c"].mean()),
        "max_t2m_c": float(daily_out["t2m_c_max"].max()),
        "min_t2m_c": float(daily_out["t2m_c_min"].min()),
    }

    return daily_out, summary


def main() -> None:
    args = parse_args()
    utils.ensure_output_directories()

    SOUTH_ASIA_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    daily, summary = run(sample_days=args.sample_days, chunk_spec=args.chunk_hours)

    daily.round(4).to_csv(OUTPUT_DAILY_CSV, index=False)
    OUTPUT_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    fig = figure_daily_t2m(daily)
    utils.save_figure(fig, "dask_aligned_daily_t2m", formats=["pdf"])

    print("\nSaved outputs:")
    print(f"  - {OUTPUT_DAILY_CSV}")
    print(f"  - {OUTPUT_SUMMARY_JSON}")
    print(f"  - {config.FIGURES_DIR / 'dask_aligned_daily_t2m.pdf'}")


if __name__ == "__main__":
    main()
