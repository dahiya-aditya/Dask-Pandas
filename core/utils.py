"""
Shared utilities for ERA5 analysis scripts.

Provides common functions for loading data, generating figures, and
managing output directories.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import netCDF4 as nc
import numpy as np
import pandas as pd

from core import config


def ensure_output_directories() -> None:
    """Create output and figure directories if they do not exist."""
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def save_figure(
    fig: plt.Figure,
    filename: str,
    formats: list[str] = ["pdf"],
    dpi: int = 300,
    apply_tight_layout: bool = False,
) -> Path:
    """
    Save figure to file in one or more formats.
    
    Parameters
    ----------
    fig : plt.Figure
        Matplotlib figure to save
    filename : str
        Output filename (without extension)
    formats : list[str]
        File formats to save (default: ["pdf"]). PDF is fast and vector-based.
    dpi : int
        Resolution for raster formats (ignored for PDF)
    apply_tight_layout : bool
        Apply fig.tight_layout() before saving. Disabled by default to avoid
        expensive or unstable layout calculations on first-run environments.
        
    Returns
    -------
    Path
        Path to the saved figure (base path, without extension)
    """
    if apply_tight_layout:
        fig.tight_layout()
    base_path = config.FIGURES_DIR / filename

    for fmt in formats:
        output_path = base_path.with_suffix(f".{fmt}")
        fig.savefig(output_path, bbox_inches="tight")

    plt.close(fig)
    return base_path


def open_netcdf(path: Path) -> nc.Dataset:
    """Open a NetCDF file using netcdf4 backend."""
    if not path.exists():
        raise FileNotFoundError(f"NetCDF file not found: {path}")
    return nc.Dataset(path, "r")


def extract_region_slice(
    data: np.ndarray,
    latitudes: np.ndarray,
    longitudes: np.ndarray,
    region: dict,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Extract spatial subset for a region from gridded data.
    
    Parameters
    ----------
    data : np.ndarray
        Full data array with shape (time, lat, lon)
    latitudes : np.ndarray
        Full latitude coordinate array
    longitudes : np.ndarray
        Full longitude coordinate array
    region : dict
        Region definition with keys: north, south, east, west
        
    Returns
    -------
    data_subset : np.ndarray
        Subset data (time, n_lat, n_lon)
    lat_subset : np.ndarray
        Subset latitudes (n_lat,)
    lon_subset : np.ndarray
        Subset longitudes (n_lon,)
    """
    south, north = region["south"], region["north"]
    west, east = region["west"], region["east"]

    # Find indices for region bounds
    lat_idx = np.where((latitudes >= south) & (latitudes <= north))[0]
    lon_idx = np.where((longitudes >= west) & (longitudes <= east))[0]

    if len(lat_idx) == 0 or len(lon_idx) == 0:
        raise ValueError(
            f"Region {region} has no valid indices in coordinate arrays. "
            f"Check region bounds."
        )

    lat_sl = slice(int(lat_idx.min()), int(lat_idx.max()) + 1)
    lon_sl = slice(int(lon_idx.min()), int(lon_idx.max()) + 1)

    return data[:, lat_sl, lon_sl], latitudes[lat_sl], longitudes[lon_sl]


def load_dataset(
    instant_path: Path,
    accum_path: Path,
) -> tuple[nc.Dataset, nc.Dataset]:
    """
    Open both instant and accumulated variable NetCDF files.
    
    Parameters
    ----------
    instant_path : Path
        Path to instant variables file
    accum_path : Path
        Path to accumulated variables file
        
    Returns
    -------
    instant_ds : nc.Dataset
    accum_ds : nc.Dataset
    """
    return open_netcdf(instant_path), open_netcdf(accum_path)
