# Dask-Pandas

## Objective
Investigate how Dask enables scalable data processing when in-memory Pandas becomes impractical, using ERA5 as a concrete high-resolution weather dataset.

## Dataset
ERA5 hourly data on single levels (Copernicus Climate Change Service / ECMWF).

Data source:
https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels

Current request profiles used in this repository:
```text
Compact consistent-slice request (~464 MB)  -> baseline Pandas analysis
Full 2024 request (~12 GB)                  -> large-volume processing and scalability testing
```

## Workflow
```text
Step 01  Build baseline Pandas analyses on feasible subsets.
Step 02  Run large-volume South Asia pipeline with Pandas baseline.
Step 03  Implement aligned Dask pipeline with lazy loading and chunking.
Step 04  Benchmark runtime and memory across chunk strategies.
Step 05  Validate Pandas vs Dask consistency for daily metrics and anomalies.
```

## Repository Structure
```text
Dask-Pandas/
│
├── core/
│   ├── config.py
│   ├── housekeeping.py
│   └── utils.py
│
├── api_requests/
│   ├── request_era5_consistent_slice_464mb.py
│   └── request_era5_full_2024_12gb.py
│
├── data/
│   ├── raw/
│   │   ├── era5_consistent_slice_extracted/
│   │   └── era5_large_overwhelm/
│   ├── results/
│   │   ├── central_india/
│   │   │   ├── daily_metrics.csv
│   │   │   └── summary.json
│   │   ├── south_asia/
│   │   │   ├── pandas_daily_metrics.csv
│   │   │   ├── pandas_summary.json
│   │   │   ├── dask_daily_metrics.csv
│   │   │   └── dask_summary.json
│   │   └── benchmark/
│   │       ├── runtime_table.csv
│   │       ├── consistency_daily.csv
│   │       ├── consistency_anomaly.csv
│   │       ├── chunk_sweep_table.csv
│   │       └── summary.json
│   └── figures/
│
├── central_india_464mb_analysis.py
├── south_asia_2024_large_analysis.py
├── south_asia_2024_dask_aligned.py
├── dask_vs_pandas_benchmark.py
├── south_asia_naive_load_demo.py
└── analysis_using_dask.py
```

## Setup
Run commands in any command-line interface from project root.

```text
Required raw folders
  data/raw/era5_consistent_slice_extracted/
  data/raw/era5_large_overwhelm/

Verification
  python -m core.housekeeping
```

## Run Analyses
```text
Central India Pandas analysis
  python central_india_464mb_analysis.py

South Asia Pandas analysis (force NetCDF recompute)
  python south_asia_2024_large_analysis.py --recompute

South Asia Dask aligned analysis (full window)
  python south_asia_2024_dask_aligned.py --sample-days 0 --chunk-hours auto
```

## Run Benchmark
Main full-window benchmark:
```text
python -u dask_vs_pandas_benchmark.py --sample-days 0 --chunk-hours 96 --skip-central
```

Full-window chunk sweep benchmark:
```text
python -u dask_vs_pandas_benchmark.py --sample-days 0 --chunk-hours 96 --chunk-sweep "24,48,96,192,auto" --skip-central
```

## Project Report
Detailed benchmarking results, consistency tables, methodological discussion,
and assignment write-up are maintained in:

```text
report/report.tex
```

Generated benchmark artifacts are stored under:

```text
data/results/benchmark/
```

### Compile the report
From the report directory, run:

```text
pdflatex --shell-escape report.tex
```

To regenerate bibliography entries after edits to refs.bib:

```text
bibtex refs
pdflatex --shell-escape report.tex
pdflatex --shell-escape report.tex
```

Do not modify:

```text
report/preamble.tex
```

Raw ERA5 payloads are intentionally ignored by Git due to size. Derived outputs, scripts, and figures remain versioned.

## Team Members
```text
Aditya Dahiya      (dahiya-aditya)
Adwita Joglekar    (adwita314)
Saachi Sirola      (saachisirola)
Brajesh K. Mahto   (Brajesh-k-Mahto)
```
