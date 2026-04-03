"""
Configuration file for ERA5 analysis projects.

Manages dataset definitions, paths, and archive references.
Users can modify this file to add new datasets or update paths.
"""

from pathlib import Path
from typing import TypedDict


class DatasetConfig(TypedDict):
    """Configuration for a single ERA5 dataset."""
    name: str
    data_dir: Path
    instant_file: str
    accum_file: str
    archive_zip: Path | None  # None if already extracted


# ============================================================================
# Dataset Definitions
# ============================================================================

# Minimal 464 MB subset for Central India (used in central_india_464mb_analysis.py)
CONSISTENT_SLICE = DatasetConfig(
    name="ERA5 Consistent Slice (464 MB)",
    data_dir=Path("data/raw/era5_consistent_slice_extracted"),
    instant_file="data_stream-oper_stepType-instant.nc",
    accum_file="data_stream-oper_stepType-accum.nc",
    archive_zip=Path("data/raw/era5_consistent_slice_download.nc"),  # Set to None if pre-extracted
)

# Full 2024 South Asia dataset (used in south_asia_2024_large_analysis.py)
LARGE_SOUTH_ASIA = DatasetConfig(
    name="ERA5 Large South Asia (2024)",
    data_dir=Path("data/raw/era5_large_overwhelm"),
    instant_file="data_stream-oper_stepType-instant.nc",
    accum_file="data_stream-oper_stepType-accum.nc",
    archive_zip=Path("data/raw/era5_full_2024_12gb.zip"),
)

# ============================================================================
# Global Paths
# ============================================================================

ROOT_DIR = Path(".")
RAW_DIR = Path("data/raw")
RESULTS_DIR = Path("data/results")
FIGURES_DIR = Path("data/figures")
DOCS_DIR = Path("docs")

RESULTS_CENTRAL_INDIA_DIR = RESULTS_DIR / "central_india"
RESULTS_SOUTH_ASIA_DIR = RESULTS_DIR / "south_asia"
RESULTS_BENCHMARK_DIR = RESULTS_DIR / "benchmark"

RESULTS_CONTRIBUTOR_DIRS = {
    "adwita": RESULTS_DIR / "contrib_adwita",
    "brajesh": RESULTS_DIR / "contrib_brajesh",
    "saachi": RESULTS_DIR / "contrib_saachi",
}

FIGURES_CONTRIBUTOR_DIRS = {
    "adwita": FIGURES_DIR / "contrib_adwita",
    "brajesh": FIGURES_DIR / "contrib_brajesh",
    "saachi": FIGURES_DIR / "contrib_saachi",
}

DOCS_CONTRIBUTOR_DIRS = {
    "adwita": DOCS_DIR / "contrib_adwita",
    "brajesh": DOCS_DIR / "contrib_brajesh",
    "saachi": DOCS_DIR / "contrib_saachi",
}

# ============================================================================
# Analysis Region Definitions
# ============================================================================

REGIONS = {
    "central_india": {
        "name": "Central India",
        "north": 30,
        "south": 20,
        "west": 75,
        "east": 85,
    },
    "south_asia": {
        "name": "South Asia",
        "north": 35.0,
        "south": 5.0,
        "west": 65.0,
        "east": 100.0,
    },
}

# ============================================================================
# Analysis Parameters
# ============================================================================

# Heat-stress thresholds (°C)
HEAT_WARN_THRESHOLD = 35.0
HEAT_DANGER_THRESHOLD = 40.0

# Calendar labels
MONTH_ABBR = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]

# Matplotlib style defaults
PLOT_STYLE = {
    "font.family": "serif",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.linewidth": 0.8,
    "xtick.direction": "out",
    "ytick.direction": "out",
    "xtick.major.size": 4,
    "ytick.major.size": 4,
}
