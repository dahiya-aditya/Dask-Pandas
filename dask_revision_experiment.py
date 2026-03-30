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
import time

import numpy as np
import pandas as pd
import xarray as xr

from core import config, utils
from core.config import HEAT_DANGER_THRESHOLD, HEAT_WARN_THRESHOLD, LARGE_SOUTH_ASIA, REGIONS
from south_asia_2024_dask_analysis import run_dask_analysis


DATA_DIR = LARGE_SOUTH_ASIA["data_dir"]
INSTANT_NC = DATA_DIR / LARGE_SOUTH_ASIA["instant_file"]
ACCUM_NC = DATA_DIR / LARGE_SOUTH_ASIA["accum_file"]
REGION = REGIONS["south_asia"]

EXPERIMENT_JSON = config.RESULTS_DIR / "dask_revision_experiment.json"
EXPERIMENT_CSV = config.RESULTS_DIR / "dask_revision_experiment.csv"


def _subset(ds: xr.Dataset) -> xr.Dataset:
    """Subset to South Asia bounds while handling latitude ordering."""
    south = float(REGION["south"])
    north = float(REGION["north"])
    west = float(REGION["west"])
    east = float(REGION["east"])

    lat0 = float(ds.latitude.isel(latitude=0).values)
    latn = float(ds.latitude.isel(latitude=-1).values)
    lat_slice = slice(north, south) if lat0 > latn else slice(south, north)

    lon0 = float(ds.longitude.isel(longitude=0).values)
    lonn = float(ds.longitude.isel(longitude=-1).values)
    lon_slice = slice(east, west) if lon0 > lonn else slice(west, east)

    return ds.sel(latitude=lat_slice, longitude=lon_slice)


def run_poor_pipeline(chunk_time: int, chunk_lat: int, chunk_lon: int) -> dict[str, float | int | str | dict[str, int]]:
    """
    Intentionally poor Dask pipeline.

    Anti-patterns used intentionally:
    1) tiny chunks that produce too many tasks
    2) repeated .compute() calls on separate intermediates
    3) eager conversion to Pandas early in the pipeline
    """
    if not INSTANT_NC.exists() or not ACCUM_NC.exists():
        raise FileNotFoundError(
            f"Extracted NetCDF files not found in {DATA_DIR}. "
            f"Run: python -m core.housekeeping"
        )

    chunks = {
        "valid_time": int(chunk_time),
        "latitude": int(chunk_lat),
        "longitude": int(chunk_lon),
    }

    start = time.perf_counter()

    ds_i = xr.open_dataset(INSTANT_NC, engine="netcdf4", chunks=chunks)
    ds_a = xr.open_dataset(ACCUM_NC, engine="netcdf4", chunks=chunks)
    ds_i = _subset(ds_i)
    ds_a = _subset(ds_a)

    weights = np.cos(np.deg2rad(ds_i["latitude"]))
    weights = weights / weights.sum()

    # Repeated compute calls force multiple graph executions and high overhead.
    t2m_series = (ds_i["t2m"] - 273.15).weighted(weights).mean(dim=("latitude", "longitude")).compute()
    tcc_series = ds_i["tcc"].weighted(weights).mean(dim=("latitude", "longitude")).compute()
    ssr_series = (ds_a["ssr"] / 21600.0).weighted(weights).mean(dim=("latitude", "longitude")).compute()
    str_series = (ds_a["str"] / 21600.0).weighted(weights).mean(dim=("latitude", "longitude")).compute()
    tisr_series = (ds_a["tisr"] / 21600.0).weighted(weights).mean(dim=("latitude", "longitude")).compute()
    tsr_series = (ds_a["tsr"] / 21600.0).weighted(weights).mean(dim=("latitude", "longitude")).compute()

    frac_35 = ((ds_i["t2m"] - 273.15) > HEAT_WARN_THRESHOLD).mean(dim=("latitude", "longitude")).compute()
    frac_40 = ((ds_i["t2m"] - 273.15) > HEAT_DANGER_THRESHOLD).mean(dim=("latitude", "longitude")).compute()

    df = pd.DataFrame(
        {
            "t2m_c": t2m_series.to_numpy(),
            "tcc": tcc_series.to_numpy(),
            "ssr": ssr_series.to_numpy(),
            "str_nlw": str_series.to_numpy(),
            "tisr": tisr_series.to_numpy(),
            "tsr": tsr_series.to_numpy(),
            "frac_above_35c": frac_35.to_numpy(),
            "frac_above_40c": frac_40.to_numpy(),
        },
        index=pd.to_datetime(t2m_series["valid_time"].to_numpy()),
    )

    daily = df.resample("D").mean()
    daily["t2m_c_max"] = df["t2m_c"].resample("D").max()
    daily["t2m_c_min"] = df["t2m_c"].resample("D").min()
    daily["net_rad"] = daily["ssr"] + daily["str_nlw"]
    daily["reflected_solar"] = daily["tisr"] - daily["tsr"]

    elapsed = time.perf_counter() - start
    return {
        "name": "poor",
        "scheduler": "threads",
        "chunks": chunks,
        "elapsed_seconds": round(float(elapsed), 3),
        "rows_daily": int(daily.shape[0]),
        "annual_mean_t2m_c": round(float(daily["t2m_c"].mean()), 4),
        "anti_patterns": [
            "tiny chunks",
            "repeated compute calls",
            "early pandas conversion",
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
