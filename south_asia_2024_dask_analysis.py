"""
Dask + Xarray ERA5 2024 analysis - South Asia subset.

This script mirrors the Pandas baseline analysis while using lazy loading and
chunked computation through Dask. It computes regional time series, daily
aggregates, monthly climatology, anomalies, and heat-extreme fractions.

Outputs are written to data/results using dask_* filenames to avoid
overwriting the existing Pandas baseline outputs.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import dask
import matplotlib

matplotlib.use("Agg")
import numpy as np
import pandas as pd
import xarray as xr
from dask.distributed import Client, LocalCluster

from core import config, utils
from core.config import (
    HEAT_DANGER_THRESHOLD,
    HEAT_WARN_THRESHOLD,
    LARGE_SOUTH_ASIA,
    MONTH_ABBR,
    REGIONS,
)
from south_asia_2024_large_analysis import figure_annual_cycle, figure_monthly_analysis


DATA_DIR = LARGE_SOUTH_ASIA["data_dir"]
INSTANT_NC = DATA_DIR / LARGE_SOUTH_ASIA["instant_file"]
ACCUM_NC = DATA_DIR / LARGE_SOUTH_ASIA["accum_file"]
REGION = REGIONS["south_asia"]

DASK_DAILY_RESULTS_CSV = config.RESULTS_DIR / "dask_daily_regional_means.csv"
DASK_MONTHLY_RESULTS_CSV = config.RESULTS_DIR / "dask_large_monthly_climatology.csv"
DASK_ANOMALY_RESULTS_CSV = config.RESULTS_DIR / "dask_large_temperature_anomalies.csv"
DASK_HEAT_RESULTS_CSV = config.RESULTS_DIR / "dask_heat_extreme_daily.csv"
DASK_SUMMARY_JSON = config.RESULTS_DIR / "dask_large_summary.json"
DASK_BENCH_JSON = config.RESULTS_DIR / "dask_run_metrics.json"


def _spatial_subset(ds: xr.Dataset, region: dict[str, float]) -> xr.Dataset:
    """Subset dataset by lat/lon bounds while handling coordinate ordering."""
    south = float(region["south"])
    north = float(region["north"])
    west = float(region["west"])
    east = float(region["east"])

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
    """Open NetCDF lazily with xarray+dask."""
    return xr.open_dataset(path, engine="netcdf4", chunks=chunks)


def _monthly_anomaly(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Compute anomaly by subtracting each variable's within-month mean."""
    anomaly = daily_df.copy()
    for col in daily_df.columns:
        anomaly[col] = daily_df[col] - daily_df[col].groupby(daily_df.index.month).transform("mean")
    anomaly.columns = [f"{c}_anom" for c in anomaly.columns]
    return anomaly


def run_dask_analysis(
    *,
    chunk_time: int,
    chunk_lat: int,
    chunk_lon: int,
    scheduler: str,
    workers: int,
    threads_per_worker: int,
    save_figures: bool,
) -> dict[str, float | int | str | dict[str, int]]:
    """Run the full Dask pipeline and persist outputs."""
    utils.ensure_output_directories()

    if not INSTANT_NC.exists() or not ACCUM_NC.exists():
        raise FileNotFoundError(
            f"Extracted NetCDF files not found in {DATA_DIR}.\n"
            f"Expected: {INSTANT_NC.name} and {ACCUM_NC.name}\n"
            f"Run: python -m core.housekeeping"
        )

    chunks = {
        "valid_time": int(chunk_time),
        "latitude": int(chunk_lat),
        "longitude": int(chunk_lon),
    }

    start = time.perf_counter()

    if scheduler == "distributed":
        cluster = LocalCluster(
            n_workers=workers,
            threads_per_worker=threads_per_worker,
            processes=True,
            dashboard_address=None,
        )
        client = Client(cluster)
    else:
        cluster = None
        client = None

    try:
        ds_i = _open_chunked_dataset(INSTANT_NC, chunks=chunks)
        ds_a = _open_chunked_dataset(ACCUM_NC, chunks=chunks)

        ds_i = _spatial_subset(ds_i, REGION)
        ds_a = _spatial_subset(ds_a, REGION)

        weights = np.cos(np.deg2rad(ds_i["latitude"]))
        weights = weights / weights.sum()

        t2m_c = ds_i["t2m"] - 273.15
        tcc = ds_i["tcc"]
        ssr = ds_a["ssr"] / 21600.0
        str_nlw = ds_a["str"] / 21600.0
        tisr = ds_a["tisr"] / 21600.0
        tsr = ds_a["tsr"] / 21600.0
        ttr = ds_a["ttr"] / 21600.0

        regional_ds = xr.Dataset(
            {
                "t2m_c": t2m_c.weighted(weights).mean(dim=("latitude", "longitude")),
                "tcc": tcc.weighted(weights).mean(dim=("latitude", "longitude")),
                "ssr": ssr.weighted(weights).mean(dim=("latitude", "longitude")),
                "str_nlw": str_nlw.weighted(weights).mean(dim=("latitude", "longitude")),
                "tisr": tisr.weighted(weights).mean(dim=("latitude", "longitude")),
                "tsr": tsr.weighted(weights).mean(dim=("latitude", "longitude")),
                "ttr": ttr.weighted(weights).mean(dim=("latitude", "longitude")),
            }
        )

        extremes_ds = xr.Dataset(
            {
                "frac_above_35c": (t2m_c > HEAT_WARN_THRESHOLD).mean(dim=("latitude", "longitude")),
                "frac_above_40c": (t2m_c > HEAT_DANGER_THRESHOLD).mean(dim=("latitude", "longitude")),
            }
        )

        daily_mean = regional_ds.resample(valid_time="1D").mean()
        daily_tmax = regional_ds[["t2m_c"]].resample(valid_time="1D").max().rename({"t2m_c": "t2m_c_max"})
        daily_tmin = regional_ds[["t2m_c"]].resample(valid_time="1D").min().rename({"t2m_c": "t2m_c_min"})

        daily_ds = xr.merge([daily_mean, daily_tmax, daily_tmin])
        daily_ds["net_rad"] = daily_ds["ssr"] + daily_ds["str_nlw"]
        daily_ds["reflected_solar"] = daily_ds["tisr"] - daily_ds["tsr"]

        monthly_ds = daily_ds.groupby("valid_time.month").mean()
        heat_daily_ds = extremes_ds.resample(valid_time="1D").max()

        daily_df = daily_ds.compute().to_dataframe()
        daily_df.index.name = "time"
        daily_df = daily_df.sort_index()

        monthly_df = monthly_ds.compute().to_dataframe().sort_index()
        monthly_df.index.name = "month"

        heat_daily_df = heat_daily_ds.compute().to_dataframe().sort_index()
        heat_daily_df.index.name = "time"

        anomaly_df = _monthly_anomaly(daily_df)
        anomaly_df.index.name = "time"

        daily_df.round(4).to_csv(DASK_DAILY_RESULTS_CSV, index_label="time")
        monthly_df.round(4).to_csv(DASK_MONTHLY_RESULTS_CSV, index_label="month")
        anomaly_df.round(4).to_csv(DASK_ANOMALY_RESULTS_CSV, index_label="time")
        heat_daily_df.round(4).to_csv(DASK_HEAT_RESULTS_CSV, index_label="time")

        if save_figures:
            utils.save_figure(figure_annual_cycle(daily_df, heat_daily_df), "dask_annual_temperature_cycle")
            utils.save_figure(figure_monthly_analysis(daily_df, anomaly_df), "dask_monthly_energy_budget")

        elapsed = time.perf_counter() - start
        summary = {
            "dataset": LARGE_SOUTH_ASIA["name"],
            "region": REGION,
            "scheduler": scheduler,
            "chunks": chunks,
            "rows_daily": int(daily_df.shape[0]),
            "rows_heat_daily": int(heat_daily_df.shape[0]),
            "daily_columns": int(daily_df.shape[1]),
            "elapsed_seconds": round(float(elapsed), 3),
            "temperature_2024": {
                "annual_mean_c": round(float(daily_df["t2m_c"].mean()), 2),
                "annual_max_c": round(float(daily_df["t2m_c_max"].max()), 2),
                "annual_min_c": round(float(daily_df["t2m_c_min"].min()), 2),
                "days_with_any_area_above_35c": int((heat_daily_df["frac_above_35c"] > 0).sum()),
                "days_with_any_area_above_40c": int((heat_daily_df["frac_above_40c"] > 0).sum()),
                "peak_frac_above_35c_pct": round(float(heat_daily_df["frac_above_35c"].max() * 100), 1),
                "hottest_month": MONTH_ABBR[int(monthly_df["t2m_c"].idxmax()) - 1],
            },
            "energy_budget": {
                "annual_mean_net_surface_rad_wm2": round(float(daily_df["net_rad"].mean()), 2),
                "peak_solar_month": MONTH_ABBR[int(monthly_df["ssr"].idxmax()) - 1],
            },
            "cloud_cover": {
                "annual_mean_pct": round(float(daily_df["tcc"].mean() * 100), 1),
                "peak_cloud_month": MONTH_ABBR[int(monthly_df["tcc"].idxmax()) - 1],
            },
            "outputs": {
                "daily_csv": str(DASK_DAILY_RESULTS_CSV),
                "monthly_csv": str(DASK_MONTHLY_RESULTS_CSV),
                "anomaly_csv": str(DASK_ANOMALY_RESULTS_CSV),
                "heat_csv": str(DASK_HEAT_RESULTS_CSV),
                "summary_json": str(DASK_SUMMARY_JSON),
            },
        }

        DASK_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        DASK_BENCH_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        return summary
    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()


def main() -> None:
    """CLI entrypoint for Dask South Asia analysis."""
    parser = argparse.ArgumentParser(description="South Asia ERA5 Dask analysis")
    parser.add_argument("--chunk-time", type=int, default=96, help="Chunk size along valid_time")
    parser.add_argument("--chunk-lat", type=int, default=64, help="Chunk size along latitude")
    parser.add_argument("--chunk-lon", type=int, default=64, help="Chunk size along longitude")
    parser.add_argument(
        "--scheduler",
        choices=["threads", "distributed"],
        default="threads",
        help="Dask scheduler mode",
    )
    parser.add_argument("--workers", type=int, default=4, help="Workers for distributed scheduler")
    parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=1,
        help="Threads per worker for distributed scheduler",
    )
    parser.add_argument(
        "--no-figures",
        action="store_true",
        help="Skip PDF figure generation for faster benchmarking",
    )
    args = parser.parse_args()

    dask.config.set(scheduler="threads" if args.scheduler == "threads" else "distributed")

    summary = run_dask_analysis(
        chunk_time=args.chunk_time,
        chunk_lat=args.chunk_lat,
        chunk_lon=args.chunk_lon,
        scheduler=args.scheduler,
        workers=args.workers,
        threads_per_worker=args.threads_per_worker,
        save_figures=not args.no_figures,
    )

    print("\n" + "=" * 70)
    print("Dask analysis completed")
    print("=" * 70)
    print(f"Elapsed time: {summary['elapsed_seconds']} s")
    print(f"Chunks: {summary['chunks']}")
    print(f"Daily rows: {summary['rows_daily']}")
    print(f"Daily mean temperature: {summary['temperature_2024']['annual_mean_c']} C")
    print(f"Summary file: {DASK_SUMMARY_JSON}")


if __name__ == "__main__":
    main()
