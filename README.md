# Dask-Pandas: Scalable Weather Data Processing with ERA5

## Overview

This project investigates scalable data processing strategies for large-volume gridded weather data using ERA5 (Copernicus Climate Change Service / ECMWF). We compare traditional in-memory Pandas workflows against Dask-based distributed processing, demonstrating when and how lazy evaluation and intelligent chunking enable analysis of datasets that exceed available RAM.

**Dataset:** ERA5 hourly single-level reanalysis data  
**Region of focus:** South Asia (2024 calendar year)  
**Data volumes:** 464 MB (subset), 12 GB (full year)

## Quick Start

### 1. Environment Setup

Activate the Python virtual environment:
```bash
.venv\Scripts\Activate.ps1  # Windows PowerShell
# or
source .venv/bin/activate   # macOS/Linux
```

### 2. Verify Raw Data

Check that required ERA5 payloads are extracted:
```bash
python -m core.housekeeping
```

This verifies:
- `data/raw/era5_consistent_slice_extracted/` (464 MB subset)
- `data/raw/era5_large_overwhelm/` (12 GB full year)

### 3. Run Full Workflow

Execute all core and contributor analyses:
```bash
python .venv\Scripts\python.exe run_all_analyses.py
```

Optional: Include expensive benchmark
```bash
python .venv\Scripts\python.exe run_all_analyses.py --include-saachi-benchmark
```

Other flags:
- `--clean`: Delete previous results before running
- `--canonical-only`: Run only core analyses (skip contributor scripts)

## Repository Structure

```
Dask-Pandas/
├── core/                              # Shared utilities and configuration
│   ├── config.py                      # Dataset paths, regions, constants
│   ├── housekeeping.py                # Setup and diagnostics
│   └── utils.py                       # Figure saving, I/O helpers
│
├── data/
│   ├── raw/                           # ERA5 NetCDF payloads (git-ignored)
│   │   ├── era5_consistent_slice_extracted/
│   │   └── era5_large_overwhelm/
│   ├── results/                       # CSV outputs and summaries
│   │   ├── central_india/
│   │   ├── south_asia/
│   │   ├── benchmark/
│   │   └── contrib_*/
│   └── figures/                       # PDF and visualization outputs
│
├── Canonical Analyses (root level)
│   ├── central_india_464mb_analysis.py
│   ├── south_asia_2024_large_analysis.py
│   ├── south_asia_2024_dask_aligned.py
│   ├── south_asia_naive_load_demo.py
│   └── dask_vs_pandas_benchmark.py
│
├── contrib_adwita/                    # Contributor analyses
│   ├── temperature_cloudcover_dask_analysis.py
│   ├── toposphere_radiation_dask_analysis.py
│   └── surface_radiation_dask_analysis.py
│
├── contrib_brajesh/
│   ├── south_asia_2024_dask_analysis.py
│   ├── benchmark_dask_chunking.py
│   ├── validate_pandas_vs_dask.py
│   ├── dask_revision_experiment.py
│   ├── plot_anomaly_graphs.py
│   └── generate_case_study_report.py
│
├── contrib_saachi/
│   └── temp_agg.py                    # Chunking strategy benchmark (slow)
│
├── run_all_analyses.py                # Orchestrator script
├── report/                            # LaTeX writeup
│   ├── report.tex
│   ├── preamble.tex
│   └── refs.bib
└── README.md
```

## Workflow Description

### Core Analyses
├── Core Analyses (root level)

1. **central_india_464mb_analysis.py**
   - Pandas-based baseline on 464 MB subset
   - Spatial averaging and anomaly computation for Central India region
   - Validates methodology before scaling to 12 GB

2. **south_asia_2024_large_analysis.py**
   - Pandas analysis on full 12 GB dataset
   - Daily metrics, monthly climatology, temperature anomalies
   - Output: baseline results for validation against Dask

3. **south_asia_2024_dask_aligned.py**
   - Equivalent Dask implementation with lazy loading
   - Configurable chunking strategy (`--chunk-hours`)
   - Runtime and memory comparison with Pandas

4. **dask_vs_pandas_benchmark.py**
   - Quantitative comparison: wall time, peak memory, task-graph complexity
   - Optional chunk-size sweep (`--chunk-sweep`)
   - Summary statistics and consistency tables

5. **south_asia_naive_load_demo.py**
   - Educational: demonstrates the cost of loading full 12 GB into RAM at once

### Contributor Analyses

**Adwita (Multi-variable analysis):**
- `temperature_cloudcover_dask_analysis.py`: 2m temperature and cloud cover
- `toposphere_radiation_dask_analysis.py`: Top-of-atmosphere radiation budget
- `surface_radiation_dask_analysis.py`: Surface radiation fluxes

**Brajesh (Validation and experiments):**
- `south_asia_2024_dask_analysis.py`: Alternative Dask pipeline
- `benchmark_dask_chunking.py`: Chunk-size sensitivity study
- `validate_pandas_vs_dask.py`: Numeric consistency validation (MAE, RMSE)
- `dask_revision_experiment.py`: Methodological variations
- `plot_anomaly_graphs.py`: Publication-quality anomaly plots
- `generate_case_study_report.md`: Structured summary report

**Saachi (Benchmark experiment):**
- `temp_agg.py`: Systematic comparison of three chunking strategies (time-first, balanced, space-first) on monthly aggregation task
  - Note: Expensive (~15–40 min). Include only with `--include-saachi-benchmark` flag.

## Individual Script Usage

### Run a Single Analysis
```bash
python .venv\Scripts\python.exe south_asia_2024_large_analysis.py --recompute
```

### Dask Analysis with Custom Chunking
```bash
python .venv\Scripts\python.exe south_asia_2024_dask_aligned.py \
  --sample-days 0 --chunk-hours 96
```

### Benchmark with Chunk Sweep
```bash
python .venv\Scripts\python.exe dask_vs_pandas_benchmark.py \
  --sample-days 0 --chunk-hours 96 \
  --chunk-sweep "24,48,96,192,auto" --skip-central
```

### Run Saachi Benchmark
```bash
python .venv\Scripts\python.exe contrib_saachi/temp_agg.py
```

## Report

The project write-up, methodological justification, and quantitative results are compiled in:

**report/report.tex**

To regenerate the PDF:
```bash
cd report
pdflatex --shell-escape report.tex
bibtex refs
pdflatex --shell-escape report.tex
pdflatex --shell-escape report.tex
```


## Dependencies

Core requirements:
- Python 3.10+
- xarray, pandas, numpy
- dask[complete] (distributed scheduler and diagnostics)
- matplotlib
- netCDF4

See `.venv/` for pinned versions.

## Data Management

- **Raw ERA5 NetCDF files:** Git-ignored (size constraints). Must be downloaded via CDS API.
- **Derived outputs:** Versioned in `data/results/` and `data/figures/`
- **Intermediate artifacts:** Auto-generated by scripts; safe to delete and regenerate

## Notes

- Start with `run_all_analyses.py --canonical-only` to verify the core pipeline works (~10 min).
- Contributor analyses can be run independently; see individual script headers for requirements.


## Team

- Aditya Dahiya
- Adwita Joglekar
- Saachi Sirola
- Brajesh K. Mahto
