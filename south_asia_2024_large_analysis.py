"""
ERA5 2024 large-scale Pandas baseline analysis - South Asia subset.

Performs spatial subset analysis for South Asia (5-35 N, 65-100 E),
including temporal aggregation, anomalies, and energy budget metrics.
Generates comprehensive figures visualizing temperature cycles,
radiation budgets, and monthly anomalies.

Before running: Ensure extracted NetCDF files exist via:
    python -m core.housekeeping
"""

from __future__ import annotations

import argparse
import json
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import netCDF4 as nc
import numpy as np
import pandas as pd

from core import config, utils
from core.config import (
    LARGE_SOUTH_ASIA,
    REGIONS,
    MONTH_ABBR,
    HEAT_WARN_THRESHOLD,
    HEAT_DANGER_THRESHOLD,
    PLOT_STYLE,
)

# -- Setup dataset paths and region definition ---------------------------------

LARGE_SOUTH_ASIA_REGION = REGIONS["south_asia"]

DATA_DIR = LARGE_SOUTH_ASIA["data_dir"]
INSTANT_NC = DATA_DIR / LARGE_SOUTH_ASIA["instant_file"]
ACCUM_NC = DATA_DIR / LARGE_SOUTH_ASIA["accum_file"]
DAILY_RESULTS_CSV = config.RESULTS_DIR / "daily_regional_means.csv"
MONTHLY_RESULTS_CSV = config.RESULTS_DIR / "large_monthly_climatology.csv"
ANOMALY_RESULTS_CSV = config.RESULTS_DIR / "large_temperature_anomalies.csv"
HEAT_RESULTS_CSV = config.RESULTS_DIR / "heat_extreme_daily.csv"
SUMMARY_JSON = config.RESULTS_DIR / "large_summary.json"


# -- Load South Asia subset via netCDF4 + NumPy --------------------------------

def load_south_asia() -> tuple[pd.DatetimeIndex, np.ndarray, np.ndarray, pd.DataFrame, pd.DataFrame]:
    """
    Read ERA5 variables for South Asia region from NetCDF and compute regional statistics.
    
    Returns (times, lats, lons, regional_df, extremes_df)
    """
    print("=" * 70)
    print("South Asia spatial subset analysis")
    print("=" * 70)

    region = LARGE_SOUTH_ASIA_REGION
    s, n = region["south"], region["north"]
    w, e = region["west"], region["east"]

    try:
        ds_i = nc.Dataset(INSTANT_NC, "r")
        ds_a = nc.Dataset(ACCUM_NC, "r")
    except OSError as exc:
        raise FileNotFoundError(
            f"Missing NetCDF files in {DATA_DIR}.\n"
            f"Run: python -m core.housekeeping"
        ) from exc

    try:
        # Read coordinates and subset region
        lat_full = np.asarray(ds_i.variables["latitude"][:], dtype=np.float32)
        lon_full = np.asarray(ds_i.variables["longitude"][:], dtype=np.float32)
        time_raw = np.asarray(ds_i.variables["valid_time"][:])

        lat_idx = np.where((lat_full >= s) & (lat_full <= n))[0]
        lon_idx = np.where((lon_full >= w) & (lon_full <= e))[0]
        if len(lat_idx) == 0 or len(lon_idx) == 0:
            raise ValueError("Requested South Asia bounds map to an empty grid subset.")

        lat_min, lat_max = int(lat_idx.min()), int(lat_idx.max()) + 1
        lon_min, lon_max = int(lon_idx.min()), int(lon_idx.max()) + 1
        lat_sl = slice(lat_min, lat_max)
        lon_sl = slice(lon_min, lon_max)

        lats = lat_full[lat_sl]
        lons = lon_full[lon_sl]

        n_t, n_y, n_x = len(time_raw), len(lats), len(lons)
        per_var_mb = n_t * n_y * n_x * 4 / 1e6

        print(f"\n  Region       : {s}-{n} N,  {w}-{e} E")
        print(f"  Grid size    : {n_t} time-steps  x  {n_y} lat  x  {n_x} lon")
        print(f"  Peak memory  : ~{per_var_mb:.0f} MB per variable (subset-only)")
        print()

        # Compute area-weighted regional means.
        # Keep this float32 for lower memory and faster reductions.
        lat_weights = np.cos(np.deg2rad(lats)).astype(np.float32)
        lat_weights /= lat_weights.sum(dtype=np.float32)

        def _read_subset_float32(ds: nc.Dataset, var_name: str) -> np.ndarray:
            var = ds.variables[var_name]
            arr = var[:, lat_sl, lon_sl]
            if np.ma.isMaskedArray(arr):
                arr = arr.filled(np.nan)
            return np.asarray(arr, dtype=np.float32, order="C")

        def weighted_mean(arr_3d: np.ndarray) -> np.ndarray:
            # First reduce longitude, then apply latitude weights.
            # This avoids a large broadcasted 3D intermediate.
            lon_mean = arr_3d.mean(axis=2, dtype=np.float32)
            return (lon_mean @ lat_weights).astype(np.float32)

        def read_and_reduce(
            ds: nc.Dataset,
            var_name: str,
            *,
            label: str,
            subtract: float = 0.0,
            divide: float = 1.0,
        ) -> np.ndarray:
            print(f"  Loading {label} ...", flush=True)
            for attempt in range(2):
                t0 = time.perf_counter()
                try:
                    arr = _read_subset_float32(ds, var_name)
                    if subtract != 0.0:
                        arr -= np.float32(subtract)
                    if divide != 1.0:
                        arr /= np.float32(divide)
                    reduced = weighted_mean(arr)
                    del arr
                    elapsed = time.perf_counter() - t0
                    print(f"    done in {elapsed:.1f}s")
                    return reduced
                except KeyboardInterrupt:
                    if attempt == 0:
                        print("    interrupted during read; retrying once ...", flush=True)
                        continue
                    raise

            raise RuntimeError(f"Failed to load {label} after retry")

        times = pd.to_datetime(time_raw, unit="s", utc=True).tz_localize(None)

        print("  Loading temperature and extremes ...", flush=True)
        t0 = time.perf_counter()
        t2m_full = _read_subset_float32(ds_i, "t2m") - np.float32(273.15)
        t2m_mean = weighted_mean(t2m_full)
        frac_above_35c = (t2m_full > HEAT_WARN_THRESHOLD).mean(axis=(1, 2)).astype(np.float32)
        frac_above_40c = (t2m_full > HEAT_DANGER_THRESHOLD).mean(axis=(1, 2)).astype(np.float32)
        del t2m_full
        print(f"    done in {time.perf_counter() - t0:.1f}s")

        secs_per_step = 6 * 3600
        regional_df = pd.DataFrame(
            {
                "t2m_c": t2m_mean,
                "tcc": read_and_reduce(ds_i, "tcc", label="cloud cover (tcc)"),
                "ssr": read_and_reduce(ds_a, "ssr", label="surface shortwave (ssr)", divide=secs_per_step),
                "str_nlw": read_and_reduce(ds_a, "str", label="surface longwave (str)", divide=secs_per_step),
                "tisr": read_and_reduce(ds_a, "tisr", label="TOA incoming solar (tisr)", divide=secs_per_step),
                "tsr": read_and_reduce(ds_a, "tsr", label="TOA net solar (tsr)", divide=secs_per_step),
                "ttr": read_and_reduce(ds_a, "ttr", label="TOA thermal (ttr)", divide=secs_per_step),
            },
            index=times,
        )
        regional_df.index.name = "time"

        extremes_df = pd.DataFrame(
            {
                "frac_above_35c": frac_above_35c,
                "frac_above_40c": frac_above_40c,
            },
            index=times,
        )
        extremes_df.index.name = "time"
    finally:
        ds_i.close()
        ds_a.close()

    mem_mb = (regional_df.memory_usage(deep=True).sum() + extremes_df.memory_usage(deep=True).sum()) / 1e6
    print(f"  Loaded       : {mem_mb:.1f} MB")
    print(f"  Time range   : {times[0].date()} -> {times[-1].date()}\n")

    return times, lats, lons, regional_df, extremes_df


# -- Compute daily statistics and anomalies ------------------------------------

def compute_daily_stats(df_6h: pd.DataFrame) -> pd.DataFrame:
    """Resample 6-hourly regional means to daily statistics."""
    daily_mean = df_6h.resample("D").mean()
    t_max = df_6h[["t2m_c"]].resample("D").max().rename(columns={"t2m_c": "t2m_c_max"})
    t_min = df_6h[["t2m_c"]].resample("D").min().rename(columns={"t2m_c": "t2m_c_min"})
    daily = pd.concat([daily_mean, t_max, t_min], axis=1)
    daily["net_rad"] = daily["ssr"] + daily["str_nlw"]
    daily["reflected_solar"] = daily["tisr"] - daily["tsr"]
    return daily


def compute_monthly_climatology(daily: pd.DataFrame) -> pd.DataFrame:
    """Monthly mean of daily values - the within-year climatological reference."""
    monthly = daily.groupby(daily.index.month).mean()
    monthly.index.name = "month"
    return monthly


def compute_anomalies(daily: pd.DataFrame) -> pd.DataFrame:
    """Daily anomaly: deviation from monthly mean."""
    anomaly = daily.copy()
    for col in daily.columns:
        anomaly[col] = daily[col] - daily[col].groupby(daily.index.month).transform("mean")
    anomaly.columns = [c + "_anom" for c in anomaly.columns]
    return anomaly


def _read_time_indexed_csv(path) -> pd.DataFrame:
    """Read a CSV with a datetime index, tolerant of old index-column names."""
    table = pd.read_csv(path)
    if table.empty:
        raise ValueError(f"CSV is empty: {path}")
    time_col = table.columns[0]
    table[time_col] = pd.to_datetime(table[time_col])
    table = table.set_index(time_col)
    table.index.name = "time"
    return table


def csv_cache_available() -> bool:
    """Return True if the core CSV outputs needed for plotting already exist."""
    return all(
        path.exists()
        for path in (DAILY_RESULTS_CSV, MONTHLY_RESULTS_CSV, ANOMALY_RESULTS_CSV, HEAT_RESULTS_CSV)
    )


def load_plot_inputs_from_csv() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load daily, anomaly, and heat-extremes tables from saved CSV outputs."""
    daily = _read_time_indexed_csv(DAILY_RESULTS_CSV)
    anomaly = _read_time_indexed_csv(ANOMALY_RESULTS_CSV)
    heat_daily = _read_time_indexed_csv(HEAT_RESULTS_CSV)
    return daily, anomaly, heat_daily


def write_summary(
    daily: pd.DataFrame,
    monthly_clim: pd.DataFrame,
    heat_daily: pd.DataFrame,
    times_count: int | None = None,
    lat_count: int | None = None,
    lon_count: int | None = None,
) -> None:
    """Write summary JSON from computed tables."""
    grid_shape: dict[str, int] = {}
    if times_count is not None:
        grid_shape["time_steps"] = int(times_count)
    if lat_count is not None:
        grid_shape["lat_points"] = int(lat_count)
    if lon_count is not None:
        grid_shape["lon_points"] = int(lon_count)

    if not grid_shape and SUMMARY_JSON.exists():
        try:
            old = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
            old_shape = old.get("grid_shape", {})
            if isinstance(old_shape, dict):
                grid_shape = {k: int(v) for k, v in old_shape.items() if isinstance(v, (int, float))}
        except (json.JSONDecodeError, OSError, ValueError):
            grid_shape = {}

    summary = {
        "dataset": LARGE_SOUTH_ASIA["name"],
        "region": LARGE_SOUTH_ASIA_REGION,
        "grid_shape": grid_shape,
        "temperature_2024": {
            "annual_mean_c": round(float(daily["t2m_c"].mean()), 2),
            "annual_max_c": round(float(daily["t2m_c_max"].max()), 2),
            "annual_min_c": round(float(daily["t2m_c_min"].min()), 2),
            "days_with_any_area_above_35c": int((heat_daily["frac_above_35c"] > 0).sum()),
            "days_with_any_area_above_40c": int((heat_daily["frac_above_40c"] > 0).sum()),
            "peak_frac_above_35c_pct": round(float(heat_daily["frac_above_35c"].max() * 100), 1),
            "hottest_month": MONTH_ABBR[int(monthly_clim["t2m_c"].idxmax()) - 1],
        },
        "energy_budget": {
            "annual_mean_net_surface_rad_wm2": round(float(daily["net_rad"].mean()), 2),
            "peak_solar_month": MONTH_ABBR[int(monthly_clim["ssr"].idxmax()) - 1],
        },
        "cloud_cover": {
            "annual_mean_pct": round(float(daily["tcc"].mean() * 100), 1),
            "peak_cloud_month": MONTH_ABBR[int(monthly_clim["tcc"].idxmax()) - 1],
        },
    }

    SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def print_output_manifest() -> None:
    """Print output locations for generated tables, plots, and summary."""
    print()
    print("=" * 70)
    print("Output files saved:")
    print("=" * 70)
    print("  CSV Results:")
    print(f"    - {DAILY_RESULTS_CSV}")
    print(f"    - {MONTHLY_RESULTS_CSV}")
    print(f"    - {ANOMALY_RESULTS_CSV}")
    print(f"    - {HEAT_RESULTS_CSV}")
    print("  Figures:")
    print(f"    - {config.FIGURES_DIR}/annual_temperature_cycle.pdf")
    print(f"    - {config.FIGURES_DIR}/monthly_energy_budget.pdf")
    print("  Summary:")
    print(f"    - {SUMMARY_JSON}")
    print()


def finalize_outputs(
    daily: pd.DataFrame,
    anomaly: pd.DataFrame,
    heat_daily: pd.DataFrame,
    times_count: int | None = None,
    lat_count: int | None = None,
    lon_count: int | None = None,
) -> None:
    """Write summary and generate figures from prepared tables."""
    monthly_clim = compute_monthly_climatology(daily)
    write_summary(
        daily,
        monthly_clim,
        heat_daily,
        times_count=times_count,
        lat_count=lat_count,
        lon_count=lon_count,
    )
    print("Generating figures ...")
    utils.save_figure(figure_annual_cycle(daily, heat_daily), "annual_temperature_cycle")
    utils.save_figure(figure_monthly_analysis(daily, anomaly), "monthly_energy_budget")
    print_output_manifest()


# -- Generate figures ----------------------------------------------------------

def figure_annual_cycle(
    daily: pd.DataFrame,
    daily_ex: pd.DataFrame,
) -> plt.Figure:
    """
    Three-panel annual time-series figure:
      (a) Daily temperature range, rolling mean, and heat thresholds
      (b) Fraction of South Asia in extreme heat, with cloud cover overlay
      (c) Net surface radiation and component breakdown
    """
    daily_ex_d = daily_ex.resample("D").max()

    with plt.rc_context(PLOT_STYLE):
        fig, axes = plt.subplots(3, 1, figsize=(11, 10), sharex=True)
        fig.subplots_adjust(hspace=0.30)

        # -- (a) temperature ---------------------------------------------------
        ax = axes[0]
        ax.fill_between(
            daily.index, daily["t2m_c_min"], daily["t2m_c_max"],
            alpha=0.20, color="#d62728", label="Daily range",
        )
        ax.plot(daily.index, daily["t2m_c"], color="#d62728", lw=0.7, alpha=0.50)
        ax.plot(
            daily.index,
            daily["t2m_c"].rolling(15, center=True, min_periods=7).mean(),
            color="#d62728", lw=2.0, label="15-day rolling mean",
        )
        ax.axhline(HEAT_WARN_THRESHOLD, color="darkorange", ls="--", lw=1.1,
                   label=f"Heat alert   {HEAT_WARN_THRESHOLD:.0f}°C")
        ax.axhline(HEAT_DANGER_THRESHOLD, color="firebrick", ls="--", lw=1.1,
                   label=f"Severe heat  {HEAT_DANGER_THRESHOLD:.0f}°C")
        ax.set_ylabel("2 m temperature (C)", fontsize=9)
        ax.legend(fontsize=7, loc="upper right", ncol=2, frameon=False)
        ax.text(0.01, 0.97, "(a)", transform=ax.transAxes, fontsize=9,
                va="top", fontweight="bold")

        # -- (b) heat extremes + cloud cover -----------------------------------
        ax = axes[1]
        ax.fill_between(
            daily_ex_d.index, daily_ex_d["frac_above_35c"] * 100,
            color="darkorange", alpha=0.65, label="> 35 C (heat alert)",
        )
        ax.fill_between(
            daily_ex_d.index, daily_ex_d["frac_above_40c"] * 100,
            color="firebrick", alpha=0.70, label="> 40 C (severe heat)",
        )
        ax2 = ax.twinx()
        ax2.plot(daily.index, daily["tcc"] * 100, color="#1f77b4",
                 lw=1.0, alpha=0.80, label="Cloud cover")
        ax2.set_ylabel("Cloud cover (%)", color="#1f77b4", fontsize=9)
        ax2.tick_params(axis="y", labelcolor="#1f77b4")
        ax.set_ylabel("Area in extreme heat (%)", fontsize=9)
        ax.set_ylim(0, 100)
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, fontsize=7, loc="upper left", frameon=False, ncol=2)
        ax.text(0.01, 0.97, "(b)", transform=ax.transAxes, fontsize=9,
                va="top", fontweight="bold")

        # -- (c) surface radiation ---------------------------------------------
        ax = axes[2]
        ax.plot(daily.index, daily["net_rad"], color="#2ca02c", lw=1.2,
                label="Net surface radiation  (ssr + str)", alpha=0.90)
        ax.plot(daily.index, daily["ssr"], color="goldenrod", lw=0.9,
                ls="--", label="Surface net shortwave (ssr)", alpha=0.80)
        ax.plot(daily.index, daily["str_nlw"], color="#9467bd", lw=0.9,
                ls="--", label="Surface net longwave  (str)", alpha=0.80)
        ax.axhline(0, color="black", lw=0.6)
        ax.set_ylabel("Radiation flux  (W m-2)", fontsize=9)
        ax.set_xlabel("Date", fontsize=9)
        ax.legend(fontsize=7, loc="upper right", frameon=False)
        ax.text(0.01, 0.97, "(c)", transform=ax.transAxes, fontsize=9,
                va="top", fontweight="bold")

        fig.suptitle(
            "South Asia 2024  --  Annual Temperature Cycle and Surface Radiation  (ERA5)",
            fontsize=11, y=1.00,
        )

    return fig


def figure_monthly_analysis(
    daily: pd.DataFrame,
    _anomaly: pd.DataFrame,
) -> plt.Figure:
    """
    Three-panel monthly summary figure:
      (a) Monthly mean of surface radiation components (grouped bar chart)
      (b) Monthly temperature anomaly from within-year mean (bar chart)
      (c) Monthly cloud cover vs temperature (scatter, coloured by month)
    """
    monthly = daily.groupby(daily.index.month).mean()
    # Anomaly for panel (b): monthly mean temperature relative to annual mean.
    mon_anom = monthly["t2m_c"] - float(daily["t2m_c"].mean())

    with plt.rc_context(PLOT_STYLE):
        fig, axes = plt.subplots(1, 3, figsize=(14, 5))
        fig.subplots_adjust(wspace=0.40)
        x = np.arange(1, 13)

        # -- (a) surface radiation budget -------------------------------------
        ax = axes[0]
        bw = 0.25
        ax.bar(x - bw, monthly["ssr"],     bw, color="#e7941a", alpha=0.85, label="Surface SW (ssr)")
        ax.bar(x,      monthly["str_nlw"], bw, color="#4477aa", alpha=0.85, label="Surface LW (str)")
        ax.bar(x + bw, monthly["net_rad"], bw, color="#228b22", alpha=0.85, label="Net surface radiation")
        ax.axhline(0, color="black", lw=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(MONTH_ABBR, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("Radiation flux  (W m-2)", fontsize=9)
        ax.legend(fontsize=7, frameon=False)
        ax.set_title("Surface Radiation Budget", fontsize=9)
        ax.text(0.01, 0.97, "(a)", transform=ax.transAxes, fontsize=9,
                va="top", fontweight="bold")

        # -- (b) monthly temperature anomaly ---------------------------------
        ax = axes[1]
        bar_colors = ["#d62728" if v >= 0 else "#1f77b4" for v in mon_anom]
        ax.bar(x, mon_anom.values, color=bar_colors, alpha=0.80)
        ax.axhline(0, color="black", lw=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(MONTH_ABBR, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("Temperature anomaly  (C)", fontsize=9)
        ax.set_title("Monthly Temperature Anomaly\n(deviation from annual mean)", fontsize=9)
        ax.text(0.01, 0.97, "(b)", transform=ax.transAxes, fontsize=9,
                va="top", fontweight="bold")

        # -- (c) cloud-temperature monthly scatter ----------------------------
        ax = axes[2]
        sc = ax.scatter(
            monthly["tcc"] * 100,
            monthly["t2m_c"],
            c=monthly.index,
            cmap="twilight_shifted",
            s=80,
            zorder=3,
        )
        for m, row in monthly.iterrows():
            ax.annotate(
                MONTH_ABBR[m - 1],
                xy=(row["tcc"] * 100, row["t2m_c"]),
                xytext=(3, 3),
                textcoords="offset points",
                fontsize=7,
            )
        plt.colorbar(sc, ax=ax, label="Month")
        ax.set_xlabel("Total cloud cover  (%)", fontsize=9)
        ax.set_ylabel("Mean 2 m temperature  (C)", fontsize=9)
        ax.set_title("Cloud Cover vs Temperature\n(monthly means)", fontsize=9)
        ax.text(0.01, 0.97, "(c)", transform=ax.transAxes, fontsize=9,
                va="top", fontweight="bold")

        fig.suptitle(
            "South Asia 2024  --  Monthly Energy Budget and Temperature Anomalies  (ERA5)",
            fontsize=11,
        )

    return fig


# -- Main execution -----------------------------------------------

def main(recompute: bool = False) -> None:
    """Run complete South Asia large-scale analysis pipeline."""
    utils.ensure_output_directories()

    print("\n" + "=" * 70)
    print("South Asia 2024 Large-Scale Analysis")
    print("=" * 70)

    if not recompute and csv_cache_available():
        print("Using cached CSV tables from previous run (skip NetCDF load).")
        daily, anomaly, heat_daily = load_plot_inputs_from_csv()
        finalize_outputs(daily, anomaly, heat_daily)
        return

    if not INSTANT_NC.exists() or not ACCUM_NC.exists():
        raise FileNotFoundError(
            f"Extracted NetCDF files not found in {DATA_DIR}.\n"
            f"Expected: {INSTANT_NC.name} and {ACCUM_NC.name}\n"
            f"Run: python -m core.housekeeping"
        )

    # Load and process data
    try:
        times, lats, lons, df_6h, df_ex = load_south_asia()
    except Exception as exc:
        if csv_cache_available():
            print(f"NetCDF read failed ({type(exc).__name__}); using cached CSV tables.")
            daily, anomaly, heat_daily = load_plot_inputs_from_csv()
            finalize_outputs(daily, anomaly, heat_daily)
            return
        raise

    mem_mb = df_6h.memory_usage(deep=True).sum() / 1e6
    print(f"  Regional time-series DataFrame : {df_6h.shape[0]} rows x {df_6h.shape[1]} cols")
    print(f"  Memory usage                   : {mem_mb:.1f} MB\n")

    # Compute statistics
    daily = compute_daily_stats(df_6h)
    anomaly = compute_anomalies(daily)

    # Save results first (CSV-first workflow)
    print("Saving results ...")
    daily.round(4).to_csv(DAILY_RESULTS_CSV, index_label="time")
    monthly_clim = compute_monthly_climatology(daily)
    monthly_clim.round(4).to_csv(MONTHLY_RESULTS_CSV, index_label="month")
    anomaly.round(4).to_csv(ANOMALY_RESULTS_CSV, index_label="time")
    heat_daily = df_ex.resample("D").max()
    heat_daily.round(4).to_csv(HEAT_RESULTS_CSV, index_label="time")

    finalize_outputs(
        daily,
        anomaly,
        heat_daily,
        times_count=len(times),
        lat_count=len(lats),
        lon_count=len(lons),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="South Asia large-scale ERA5 analysis")
    parser.add_argument(
        "--recompute",
        action="store_true",
        help="Force NetCDF recomputation even if cached CSV tables already exist.",
    )
    args = parser.parse_args()
    main(recompute=args.recompute)

