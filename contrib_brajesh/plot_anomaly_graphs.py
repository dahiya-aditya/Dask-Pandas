"""
Generate anomaly-focused figures from South Asia 2024 anomaly outputs.

Reads:
- data/results/dask_large_temperature_anomalies.csv

Writes:
- data/figures/anomaly_temperature_timeseries.pdf
- data/figures/anomaly_monthly_summary.pdf
- data/figures/anomaly_relationships.pdf
"""

from __future__ import annotations

from _bootstrap import ensure_repo_root

ensure_repo_root()

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from core import config, utils

BRAJESH_RESULTS_DIR = config.RESULTS_CONTRIBUTOR_DIRS["brajesh"]
ANOMALY_CSV = BRAJESH_RESULTS_DIR / "dask_large_temperature_anomalies.csv"


def _read_anomaly_table() -> pd.DataFrame:
    """Load anomaly table with datetime index."""
    if not ANOMALY_CSV.exists():
        raise FileNotFoundError(
            f"Missing anomaly CSV: {ANOMALY_CSV}. "
            f"Run south_asia_2024_dask_analysis.py first."
        )

    df = pd.read_csv(ANOMALY_CSV)
    if df.empty:
        raise ValueError(f"Anomaly table is empty: {ANOMALY_CSV}")

    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time").sort_index()
    return df


def figure_temperature_anomaly_timeseries(df: pd.DataFrame) -> plt.Figure:
    """Daily temperature anomaly with rolling mean and warm/cool shading."""
    series = df["t2m_c_anom"]
    smooth = series.rolling(15, center=True, min_periods=7).mean()

    fig, ax = plt.subplots(figsize=(12, 4.2))
    ax.plot(series.index, series.values, color="#1f77b4", alpha=0.45, lw=0.9, label="Daily anomaly")
    ax.plot(smooth.index, smooth.values, color="#d62728", lw=2.0, label="15-day rolling mean")

    ax.fill_between(series.index, series.values, 0, where=(series.values >= 0), color="#ff7f0e", alpha=0.25)
    ax.fill_between(series.index, series.values, 0, where=(series.values < 0), color="#1f77b4", alpha=0.20)

    ax.axhline(0, color="black", lw=0.8)
    ax.set_title("South Asia 2024: Daily Temperature Anomaly")
    ax.set_ylabel("t2m anomaly (C)")
    ax.set_xlabel("Date")
    ax.legend(frameon=False)
    return fig


def figure_monthly_anomaly_summary(df: pd.DataFrame) -> plt.Figure:
    """Monthly mean anomaly summary for temperature, cloud, and net radiation."""
    monthly = df.groupby(df.index.month).mean(numeric_only=True)
    x = np.arange(1, 13)

    fig, axes = plt.subplots(3, 1, figsize=(11, 8), sharex=True)

    axes[0].bar(x, monthly["t2m_c_anom"], color="#d62728", alpha=0.8)
    axes[0].axhline(0, color="black", lw=0.7)
    axes[0].set_ylabel("Temp (C)")
    axes[0].set_title("Monthly Mean Anomalies (2024)")

    axes[1].bar(x, monthly["tcc_anom"] * 100, color="#1f77b4", alpha=0.8)
    axes[1].axhline(0, color="black", lw=0.7)
    axes[1].set_ylabel("Cloud (%)")

    axes[2].bar(x, monthly["net_rad_anom"], color="#2ca02c", alpha=0.85)
    axes[2].axhline(0, color="black", lw=0.7)
    axes[2].set_ylabel("Net rad (W m-2)")
    axes[2].set_xlabel("Month")
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(config.MONTH_ABBR)

    return fig


def figure_anomaly_relationships(df: pd.DataFrame) -> plt.Figure:
    """Scatter relationships among anomalies for quick physical interpretation."""
    months = df.index.month

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6))

    sc = axes[0].scatter(
        df["net_rad_anom"],
        df["t2m_c_anom"],
        c=months,
        cmap="viridis",
        s=18,
        alpha=0.8,
    )
    axes[0].axhline(0, color="black", lw=0.7)
    axes[0].axvline(0, color="black", lw=0.7)
    axes[0].set_xlabel("Net radiation anomaly (W m-2)")
    axes[0].set_ylabel("Temperature anomaly (C)")
    axes[0].set_title("Temp vs Net Radiation")

    axes[1].scatter(
        df["tcc_anom"] * 100,
        df["ssr_anom"],
        c=months,
        cmap="viridis",
        s=18,
        alpha=0.8,
    )
    axes[1].axhline(0, color="black", lw=0.7)
    axes[1].axvline(0, color="black", lw=0.7)
    axes[1].set_xlabel("Cloud anomaly (%)")
    axes[1].set_ylabel("Shortwave anomaly (W m-2)")
    axes[1].set_title("Cloud vs Surface Shortwave")

    cb = fig.colorbar(sc, ax=axes.ravel().tolist(), fraction=0.025, pad=0.02)
    cb.set_label("Month")

    return fig


def main() -> None:
    """Create anomaly figures from existing result tables."""
    utils.ensure_output_directories()
    BRAJESH_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    config.FIGURES_DIR = config.FIGURES_CONTRIBUTOR_DIRS["brajesh"]
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    df = _read_anomaly_table()

    out1 = utils.save_figure(figure_temperature_anomaly_timeseries(df), "anomaly_temperature_timeseries")
    out2 = utils.save_figure(figure_monthly_anomaly_summary(df), "anomaly_monthly_summary")
    out3 = utils.save_figure(figure_anomaly_relationships(df), "anomaly_relationships")

    print("\n" + "=" * 70)
    print("Anomaly figures generated")
    print("=" * 70)
    print(f"- {out1}.pdf")
    print(f"- {out2}.pdf")
    print(f"- {out3}.pdf")


if __name__ == "__main__":
    main()
