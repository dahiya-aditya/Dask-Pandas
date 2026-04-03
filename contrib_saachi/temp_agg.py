"""
Dask Temporal Aggregation: Chunking Strategies


What this script does:
  1. Makes a LOCAL Dask cluster.
  2. Using the large 12 GB ERA5 dataset, each time with a different chunking strategy.
  3. For each strategy it runs the same monthly aggregation task and records:
       - How long it took
       - How much RAM it used at peak (MB)
       - How many Dask "tasks" were generated
  4. Produces comparison bar charts and line plots so you can SEE the differences visually.

Variables used (from the large dataset):
  Instant file:   t2m  (2-m temperature)
                  tcc  (total cloud cover)
  Accum file:     ssr  (surface net solar radiation)
                  str  (surface net thermal radiation) 
                  tisr (TOA incident solar radiation)
                  tsr  (top net solar radiation)
                  ttr  (top net thermal radiation)
"""
import time
import os
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psutil                  # for live RAM readings
from pathlib import Path
from dask.distributed import (Client,LocalCluster)
import logging
import traceback
import math

from _bootstrap import ensure_repo_root

ensure_repo_root()

from core import config

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# import config and helper functions
from core.config import LARGE_SOUTH_ASIA, MONTH_ABBR
from core.utils import ensure_output_directories, save_figure

        
# Output directory
OUTPUT_DIR = config.RESULTS_CONTRIBUTOR_DIRS["saachi"] / "temporal_aggregation"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ensure_output_directories()

config.FIGURES_DIR = config.FIGURES_CONTRIBUTOR_DIRS["saachi"] / "temporal_aggregation"
config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)

# NetCDF file paths from config
INSTANT_NC= LARGE_SOUTH_ASIA["data_dir"]/LARGE_SOUTH_ASIA["instant_file"]  # has t2m, tcc
ACCUM_NC = LARGE_SOUTH_ASIA["data_dir"] / LARGE_SOUTH_ASIA["accum_file"]  # has ssr, str, tisr, tsr, ttr

# Variable metadata 
# Maps NetCDF short name → (long name, unit, aggregation method, conversion fn)
#   agg:  "mean"  for averages (temperature, cloud cover, radiation means)
#         "sum"   for totals  (precipitation — NOT in large dataset,
#                              but radiation is sometimes summed too;
#                              we use mean here for demonstration)
VAR_META = {
    # Instant file
    "t2m":  ("2-m Temperature","°C","mean",lambda x: x - 273.15),# K to °C
    "tcc":  ("Total Cloud Cover","fraction", "mean",lambda x: x),# already 0–1
    # Accum file
    "ssr":  ("Surface Net Solar Radiation","W m⁻²","mean",lambda x: x/(6 * 3600)),# J/m² per 6-hr step W/m²
    "str":  ("Surface Net Thermal Radiation","W m⁻²", "mean",lambda x: x / (6 * 3600)),
    "tisr": ("TOA Incident Solar Radiation", "W m⁻²", "mean",lambda x: x / (6 * 3600)),
    "tsr":  ("Top Net Solar Radiation","W m⁻²", "mean",lambda x: x / (6 * 3600)),
    "ttr":  ("Top Net Thermal Radiation","W m⁻²", "mean",lambda x: x / (6 * 3600)),
}

# 3 Chunking strategies to compare
# The dataset is 6-hourly => 4 time steps/day => 120 steps/month.
# Chunk sizes below are chosen to produce around 50–200 MB chunks.
chunking_strat = {
    "balanced":{
        "chunks":{"valid_time": 120, "latitude": 30, "longitude": 30},# ~100 MB chunks
        "colour":"green",
        "label":"Balanced\n(120 × 30 × 30)",
    },
    "space_first": {
        "chunks":{"valid_time": -1, "latitude": 20, "longitude": 20},# Entire time axis in one chunk
        "colour":"red",
        "label":"Space-First\n(full time / 20 × 20 spatial)",
    },
    "time_first": {
        # Use 24 steps (6 days) instead of 4 to avoid excessive tiny task overhead on Windows.
        "chunks":{"valid_time": 24, "latitude": -1, "longitude": -1},
        "colour":"blue",
        "label":"Time-First\n(24 time steps / chunk)",
    },
}
chunk_size_exp = {
    "time_4":  {"chunks": {"valid_time": 4,  "latitude": -1, "longitude": -1}, "colour": "purple", "label": "time_4"},
    "time_8":  {"chunks": {"valid_time": 8,  "latitude": -1, "longitude": -1}, "colour": "purple", "label": "time_8"},
    "time_16": {"chunks": {"valid_time": 16, "latitude": -1, "longitude": -1}, "colour": "purple", "label": "time_16"},
}

#  HELPER FUNCTIONS

def get_ram_mb():
    # Returns how much RAM process is using right now
    proc = psutil.Process(os.getpid())
    return proc.memory_info().rss / 1_048_576   # bytes to MB


def open_dataset_with_chunks(nc_path, chunks):
    # opens a NetCDF file lazily - no data is actually loaded until .compute()
    # chunks dict tells Dask how to split the data, e.g. {"time": 4, "latitude": -1}
    # -1 means keep that whole dimension in one chunk

    return xr.open_dataset(
        nc_path,
        chunks=chunks,          # this is what creates Dask arrays
        engine="netcdf4",
        mask_and_scale=True,    # handles fill values and scaling automatically
    )


def monthly_aggregate_xarray(ds, var_names):
    results = {}

    for vname in var_names:
        if vname not in ds:
            continue

        # get aggregation method and unit conversion from VAR_META
        meta = VAR_META.get(vname)
        agg_method = meta[2] if meta else "mean"
        convert_fn = meta[3] if meta else (lambda x: x)

        # average across all lat/lon points → single time series
        spatial_mean = ds[vname].mean(dim=["latitude", "longitude"])

        # resample to monthly
        monthly = spatial_mean.resample(valid_time="ME")  # "ME" = month-end (pandas 2.2+)

        # sum for precipitation, mean for everything else
        if agg_method == "sum":
            agg = monthly.sum(skipna=True)
        else:
            agg = monthly.mean(skipna=True)

        # unit conversion
        results[vname] = xr.apply_ufunc(
            convert_fn, agg, dask="parallelized", output_dtypes=[float]
        )

    return xr.Dataset(results)




def benchmark_strategy(strategy_name, strategy_cfg, var_names_instant, var_names_accum):
    logging.info(f"Running strategy: {strategy_name}")
    logging.info(f"Chunks: {strategy_cfg['chunks']}")

    chunks = strategy_cfg["chunks"]

    # basic sanity check before we do anything
    assert len(var_names_instant) + len(var_names_accum) > 0, "no variables provided"

    ram_before = get_ram_mb()

    # open both datasets lazily (no data loaded yet)
    ds_instant = open_dataset_with_chunks(INSTANT_NC, chunks)
    ds_accum   = open_dataset_with_chunks(ACCUM_NC, chunks)

    # build aggregation graphs (still lazy)
    monthly_instant = monthly_aggregate_xarray(ds_instant, var_names_instant)
    monthly_accum   = monthly_aggregate_xarray(ds_accum, var_names_accum)

    # merge and count tasks before computing (measures graph complexity)
    merged_lazy = xr.merge([monthly_instant, monthly_accum], compat="no_conflicts")
    n_tasks = sum(
        len(arr.__dask_graph__())
        for arr in merged_lazy.data_vars.values()
        if hasattr(arr.data, "__dask_graph__")
    )
    logging.info(f"Dask task count: {n_tasks:,}")

    # actually compute - this is where disk reads and computation happen
    try:
        t_start = time.perf_counter()
        ram_start = get_ram_mb()

        result_ds = merged_lazy.compute()

        wall_s = time.perf_counter() - t_start
        ram_delta = get_ram_mb() - ram_start
        logging.info(f"Done in {wall_s:.1f}s | RAM increase: +{ram_delta:.0f} MB")

    except MemoryError:
        logging.error(f"Strategy '{strategy_name}' ran out of memory during compute")
        ds_instant.close()
        ds_accum.close()
        raise

    # convert to dataframe and add month labels
    df = result_ds.to_dataframe().reset_index()
    df["month"] = df["valid_time"].dt.month
    df["month_abbr"] = df["month"].apply(lambda m: MONTH_ABBR[m - 1])

    # rename columns to include unit suffix e.g. t2m -> t2m_°C
    rename = {}
    for vname in list(var_names_instant) + list(var_names_accum):
        if vname in df.columns:
            unit_suffix = VAR_META[vname][1].replace(" ", "").replace("⁻", "").replace("²", "2")
            rename[vname] = f"{vname}_{unit_suffix}"
    df.rename(columns=rename, inplace=True)

    ds_instant.close()
    ds_accum.close()

    return {
        "strategy": strategy_name,
        "wall_s": wall_s,
        "ram_delta_mb": ram_delta,
        "ram_before_mb": ram_before,
        "n_tasks": n_tasks,
        "df": df,
        "chunks": chunks,
    }

# PLOTTING FUNCTIONS

def plot_benchmark_comparison(metrics: list[dict]):
    """
    Bar chart comparing wall time, RAM delta, and task count across strategies.
    Saves as PDF and PNG.
    """
    names   = [m["strategy"] for m in metrics]
    labels  = [chunking_strat[n]["label"] for n in names]
    colours = [chunking_strat[n]["colour"] for n in names]

    wall_s = [m["wall_s"] for m in metrics]
    ram_delta = [m["ram_delta_mb"] for m in metrics]
    n_tasks = [m["n_tasks"] for m in metrics]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle("Dask Chunking Strategy Benchmark: Large Dataset (12 GB)\n Monthly temporal aggregation: all 7 variables", fontsize=13, fontweight="bold")

    # panel 1: wall time
    ax = axes[0]
    bars = ax.bar(labels, wall_s, color=colours, edgecolor="white")
    ax.bar_label(bars, fmt="%.1fs", padding=4)
    ax.set_title("Wall-Clock Time")
    ax.set_ylabel("Seconds")
    ax.set_ylim(0, max(max(wall_s), 1) * 1.25)  # uses wall_s
    ax.grid(axis="y", alpha=0.3)
    
    # panel 2: RAM usage
    ax = axes[1]
    bars = ax.bar(labels, ram_delta, color=colours, edgecolor="white")
    ax.bar_label(bars, fmt="%.0f MB", padding=4)
    ax.set_title("Peak RAM Increase")
    ax.set_ylabel("MB")
    ax.set_ylim(0, max(max(ram_delta), 1) * 1.25)  # uses ram_delta
    ax.grid(axis="y", alpha=0.3)
    
    # panel 3: task count
    ax = axes[2]
    bars = ax.bar(labels, n_tasks, color=colours, edgecolor="white")
    ax.bar_label(bars, fmt="%d", padding=4)
    ax.set_title("Dask Task Graph Size (lower = simpler)")
    ax.set_ylabel("Number of tasks")
    ax.set_ylim(0, max(max(n_tasks), 1) * 1.25)  # uses n_tasks
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    fig.subplots_adjust(bottom=0.2)

    save_figure(fig, "chunking_benchmark", formats=["pdf"])
    logging.info("benchmark chart saved")


def plot_monthly_climatology(best_df: pd.DataFrame, strategy_name: str):

    # Figure out which columns are actually present
    col_map = {}
    for vname in VAR_META.keys():
        unit_suffix = VAR_META[vname][1].replace(" ", "").replace("⁻","").replace("²","2")
        renamed = f"{vname}_{unit_suffix}"
        if renamed in best_df.columns:
            col_map[vname] = renamed

    if not col_map:
        logging.warning("no variable columns found in dataframe")
        return

    n_vars = len(col_map)
    ncols  = 3
    nrows = math.ceil(n_vars / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
    axes = axes.flatten()   # make indexing easy regardless of layout

    fig.suptitle(f"Monthly Climatology — ERA5 Large Dataset 2024\n (Chunking strategy: {strategy_name})",fontsize=13, fontweight="bold")

    x = np.arange(1, 13)   # months 1–12

    for idx, (vname, col) in enumerate(col_map.items()):
        ax = axes[idx]
        long_name = VAR_META[vname][0]
        unit = VAR_META[vname][1]

        monthly_vals = (best_df.groupby("month")[col].mean().reindex(range(1, 13)))

        # Choose colour based on variable type
        if vname == "t2m":
            c = "red"   # red for temperature
        elif vname == "tcc":
            c = "blue"   # blue for cloud cover
        else:
            c = "orange"   # orange for radiation

        ax.plot(x, monthly_vals.values, "o-", color=c,linewidth=2.2, markersize=7, markerfacecolor="white", markeredgewidth=2)
        ax.fill_between(x, monthly_vals.values, alpha=0.12, color=c)
        ax.set_title(long_name, fontsize=10, fontweight="bold", pad=6)
        ax.set_ylabel(unit, fontsize=9)
        ax.set_xticks(x)
        ax.set_xticklabels(MONTH_ABBR, fontsize=8)
        ax.grid(alpha=0.25)

    # Hide any unused subplot panels
    for j in range(len(col_map), len(axes)):
        axes[j].set_visible(False)

    plt.tight_layout()
    save_figure(fig, "monthly_climatology_all_vars", formats=["pdf"])
    logging.info("climatology chart saved")


def plot_chunking_strategy_overlay(metrics: list[dict]):
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle("Monthly Mean 2-m Temperature (t2m)\n All three chunking strategies overlaid — results must be identical",fontsize=12, fontweight="bold" )

    x = np.arange(1, 13)
    found_t2m = False

    for m in metrics:
        df   = m["df"]
        name = m["strategy"]
        cfg  = chunking_strat[name]

        # Find the t2m column (may have unit suffix)
        t2m_col = next((c for c in df.columns if c.startswith("t2m")), None)
        if t2m_col is None:
            continue
        found_t2m = True

        monthly = df.groupby("month")[t2m_col].mean().reindex(range(1, 13))
        ax.plot(x, monthly.values,"o-",color=cfg["colour"],label=cfg["label"].replace("\n", " "),linewidth=2.5,markersize=8,alpha=0.85)

    if not found_t2m:
        logging.warning("t2m column not found - skipping overlay plot")
        plt.close(fig)
        return

    ax.set_xticks(x)
    ax.set_xticklabels(MONTH_ABBR)
    ax.set_ylabel("Temperature (°C)")
    ax.set_xlabel("Month")
    ax.legend()
    ax.grid(alpha=0.25)

    plt.tight_layout()
    save_figure(fig, "t2m_strategy_overlay", formats=["pdf"])
    logging.info("overlay chart saved")


def plot_ram_timeline(metrics: list[dict]):
    fig, ax = plt.subplots(figsize=(10, 4))
    fig.suptitle("RAM Usage: Before vs Peak During Computation", fontsize=12, fontweight="bold")

    y_pos   = np.arange(len(metrics))
    names   = [m["strategy"] for m in metrics]
    labels  = [chunking_strat[n]["label"].replace("\n", " ") for n in names]
    colours = [chunking_strat[n]["colour"] for n in names]

    before = [m["ram_before_mb"] for m in metrics]
    after  = [m["ram_before_mb"] + m["ram_delta_mb"] for m in metrics]

    # Base bar (RAM before)
    ax.barh(y_pos, before, color="grey", edgecolor="white", height=0.5)
    # Extension (RAM increase)
    ax.barh(y_pos, [a - b for a, b in zip(after, before)],left=before, color=colours, edgecolor="white", height=0.5)
    
    x_max = max(max(after), max(before))
    for i, (b, a) in enumerate(zip(before, after)):
        ax.text(x_max + 20, y_pos[i], f"{a:.0f} MB peak", va="center")

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    ax.set_xlabel("RAM (MB)")
    ax.grid(axis="x", alpha=0.25)

    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor="grey", label="RAM before"),
        Patch(facecolor="blue", label="Time-First increase"),
        Patch(facecolor="green", label="Balanced increase"),
        Patch(facecolor="red", label="Space-First increase"),
    ]
    
    ax.legend(handles=legend_elements, loc="lower right", bbox_to_anchor=(1, -0.4), ncol=2)
    
    plt.tight_layout()
    fig.subplots_adjust(bottom=0.2)
    save_figure(fig, "ram_usage_comparison", formats=["pdf"])
    logging.info("RAM chart saved")


def plot_dask_chunk_anatomy():
    """
    A conceptual diagram showing what 'chunking' means visually. Draws a 3-D cube representing the dataset
    (time × lat × lon) sliced three different ways.
    """
    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    fig.suptitle("What does chunking mean?  Three strategies on a time × lat × lon dataset",fontsize=12, fontweight="bold")

    strategies_visual = [
        {
            "name":   "Time-First",
            "colour": "blue",
            "desc":   "Chunk = 1 day of data\n(all lats + lons in one chunk)\nBest for daily/monthly aggregation",
            "h_slices": 8, "v_slices": 1,
        },
        {
            "name":   "Balanced",
            "colour": "green",
            "desc":   "Chunk = ~100 MB block\n(30 days × 30 lat × 30 lon)\nGood all purpose default",
            "h_slices": 4, "v_slices": 3,
        },
        {
            "name":   "Space-First",
            "colour": "red",
            "desc":   "Chunk = small spatial tile\n(full time series per tile)\nBest for spatial operations,\n  BAD for temporal aggregation",
            "h_slices": 1, "v_slices": 6,
        },
    ]

    for ax, sv in zip(axes, strategies_visual):
        # Draw a rectangle representing the full dataset
        full = plt.Rectangle((0, 0), 10, 6, fc="#ECEFF1", ec="#607D8B", lw=1.5)
        ax.add_patch(full)

        # Draw horizontal lines (time divisions)
        for i in range(1, sv["h_slices"]):
            y = i * (6 / sv["h_slices"])
            ax.axhline(y, xmin=0, xmax=1, color=sv["colour"], lw=1, alpha=0.6)

        # Draw vertical chunk lines (spatial divisions)
        for j in range(1, sv["v_slices"]):
            x = j * (10 / sv["v_slices"])
            ax.axvline(x, ymin=0, ymax=1, color=sv["colour"], lw=1, alpha=0.6)

        # Shade one chunk to highlight it
        chunk_w = 10 / max(sv["v_slices"], 1)
        chunk_h = 6  / max(sv["h_slices"], 1)
        highlight = plt.Rectangle((0, 6 - chunk_h), chunk_w, chunk_h,fc=sv["colour"], alpha=0.3, ec=sv["colour"], lw=2)
        ax.add_patch(highlight)

        ax.set_xlim(0, 10)
        ax.set_ylim(0, 6)
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_xlabel("Space (lat × lon)")
        ax.set_ylabel("Time")
        ax.set_title(sv["name"], fontsize=11, fontweight="bold", color=sv["colour"])

        # Description text box
        ax.text(0.5, -0.18, sv["desc"],transform=ax.transAxes, fontsize=8.5, ha="center", va="top",bbox=dict(boxstyle="round,pad=0.4", fc="white", ec=sv["colour"], alpha=0.8))

    plt.tight_layout(rect=[0, 0.15, 1, 1])
    save_figure(fig, "chunking_anatomy_diagram", formats=["pdf"])
    logging.info("chunking anatomy diagram saved")

def plot_radiation_heatmap(best_df: pd.DataFrame):
   
    rad_vars = ["ssr", "str", "tisr", "tsr", "ttr"]
    # Build matrix: rows = months, columns = variables
    data_cols = {}
    for vname in rad_vars:
        col = next((c for c in best_df.columns if c.startswith(vname)), None)
        if col:
            monthly = best_df.groupby("month")[col].mean().reindex(range(1, 13))
            # Normalise to [0, 1]
            mn, mx = monthly.min(), monthly.max()
            normalised = (monthly - mn) / (mx - mn + 1e-9)
            data_cols[VAR_META[vname][0]] = normalised.values

    if len(data_cols) < 2:
        logging.warning("not enough radiation variables for heatmap - skipping")
        return

    mat = pd.DataFrame(data_cols, index=MONTH_ABBR)

    fig, ax = plt.subplots(figsize=(10, 5))
    
    im = ax.imshow(mat.T.values, aspect="auto", cmap="YlOrRd")

    ax.set_xticks(range(12))
    ax.set_xticklabels(MONTH_ABBR)
    ax.set_yticks(range(len(mat.columns)))
    ax.set_yticklabels(mat.columns)
    ax.set_title("Normalised Monthly Mean: Radiation Variables\n(each row normalised to [0,1] independently)",fontsize=12, fontweight="bold")

    plt.colorbar(im, ax=ax, label="Normalised intensity")
    plt.tight_layout()

    save_figure(fig, "radiation_heatmap", formats=["pdf"])
    logging.info("radiation heatmap saved")


#  DASK CLUSTER SETUP
 
def start_dask_cluster(n_workers=4, memory_limit = "3GB"):
    """
    Start a local Dask cluster: n workers separate processes running in parallel
    Total RAM used = n_workers x memory_limit. 4 workers x 3GB = 12GB

    Why a cluster instead of just using Dask with no client?
    ─────────────────────────────────────────────────────────
    When you create a Client, Dask switches from "synchronous" mode to
    "distributed" mode.  In distributed mode:
      - Each worker is a separate process with its own memory space memory limits are ENFORCED (workers spill to disk if they hit limit)
      - You get the Dask dashboard at http://localhost:8787 to watch live
      - Task scheduling is smarter (work-stealing, load balancing)
      - You can see exactly how much RAM each worker uses
    """
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=2,        # 2 threads per worker process
        memory_limit=memory_limit,   # per-worker RAM cap
        silence_logs=40,             # suppress INFO noise (40 = WARNING level)
        dashboard_address=":8787",   # open http://localhost:8787 to watch!
    )
    client = Client(cluster)

    logging.info(f"Dask cluster started - {n_workers} workers, {memory_limit} RAM each")
    logging.info(f"Dashboard: {client.dashboard_link}")

    return client, cluster


#  MAIN PIPELINE

def main():
    logging.info("ERA5 Dask Chunking Benchmark starting")

    client = None
    cluster = None
    try:
        client, cluster = start_dask_cluster(n_workers=min(4, os.cpu_count() or 2),memory_limit="3GB")

        # Check what variables are actually in the files
        with xr.open_dataset(INSTANT_NC, engine="netcdf4") as _ds:
            avail_instant = set(_ds.data_vars)
        with xr.open_dataset(ACCUM_NC, engine="netcdf4") as _ds:
            avail_accum = set(_ds.data_vars)

        vars_instant = [v for v in ["t2m", "tcc"] if v in avail_instant]
        vars_accum   = [v for v in ["ssr", "str", "tisr", "tsr", "ttr"] if v in avail_accum]
        logging.info(f"variables found - instant: {vars_instant}, accum: {vars_accum}")

        # Draw the conceptual diagram first (no data needed)
        plot_dask_chunk_anatomy()

        # Run benchmark for each chunking strategy
        all_metrics = []

        for strategy_name, strategy_cfg in chunking_strat.items():
            try:
                result = benchmark_strategy(strategy_name,strategy_cfg,vars_instant,vars_accum)
                all_metrics.append(result)

                csv_path = OUTPUT_DIR / f"{strategy_name}_monthly.csv"
                result["df"].to_csv(csv_path, index=False)
                logging.info(f"CSV saved: {csv_path}")

            except Exception as exc:
                logging.warning(f"strategy '{strategy_name}' failed: {exc}")
                traceback.print_exc()
                
        # secondary experiment - vary chunk size within time-first strategy

        size_metrics = []
        for name, strategy_cfg in chunk_size_exp.items():
            try:
                result = benchmark_strategy(name, strategy_cfg, vars_instant, vars_accum)
                size_metrics.append(result)
            except Exception as e:
                logging.warning(f"chunk size experiment failed for {name}: {e}")


        # save benchmark summary 
        summary_rows = []
        for m in all_metrics:
            summary_rows.append({
                "strategy": m["strategy"],
                "chunks":str(m["chunks"]),
                "wall_s":round(m["wall_s"], 2),
                "ram_delta_mb":round(m["ram_delta_mb"], 1),
                "n_dask_tasks":m["n_tasks"],
            })
        summary_df = pd.DataFrame(summary_rows)
        summary_path = OUTPUT_DIR / "benchmark_summary_dask.csv"
        summary_df.to_csv(summary_path, index=False)

        logging.info("benchmark summary:")
        print(summary_df.to_string(index=False))

        # Plots 
        plot_benchmark_comparison(all_metrics)
        plot_chunking_strategy_overlay(all_metrics)
        plot_ram_timeline(all_metrics)

        # Use the fastest strategy's results for the detailed variable plots
        best = min(all_metrics, key=lambda m: m["wall_s"])
        plot_monthly_climatology(best["df"], best["strategy"])

        if vars_accum:
            plot_radiation_heatmap(best["df"])

        if size_metrics:
            names = [m["strategy"] for m in size_metrics]
            times = [m["wall_s"] for m in size_metrics]
            
            fig, ax = plt.subplots()
            ax.plot(names, times, marker='o')
            ax.set_title("Effect of Chunk Size on Performance (Time-first)")
            ax.set_ylabel("Execution Time (s)")
            save_figure(fig, "chunk_size_comparison", formats=["pdf"])

        logging.info("done outputs saved to: " + str(OUTPUT_DIR))
    finally:
        plt.close("all")
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()

if __name__ == "__main__":
    main()
