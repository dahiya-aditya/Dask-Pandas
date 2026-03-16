# Dask-Pandas

## Objective
To investigate how Dask enables scalable data processing for high-resolution weather datasets (ERA5) when in-memory Pandas workflows become impractical.

## Dataset
ERA5 hourly data on single levels from 1940 to present.

ERA5 is the fifth-generation global climate reanalysis produced by CDS ECMWF. It provides hourly estimates of a wide range of atmospheric, ocean-wave, and land-surface variables from 1940 to the present. The dataset is distributed by the Copernicus Climate Change Service and features a high-resolution 0.25 x 0.25 degree grid. Because of its high dimensionality, it serves as an excellent case study for evaluating how Dask enables scalable analysis when Pandas becomes impractical.

Data source: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels

Current request profiles used in this repository:
```text
Compact consistent-slice request (~464 MB)  -> baseline Pandas analysis
Full 2024 request (~12 GB)                  -> large-volume processing and scalability testing
```

## Workflow
```text
Step 01  Download a small chunk of the dataset and analyze it using Pandas and Dask.
Step 02  Implement baseline analysis on this small chunk with Pandas.
Step 03  Download a larger subset and confirm that Pandas becomes impractical.
Step 04  Implement a similar pipeline using Dask with lazy loading and chunked computation.
Step 05  Compare consistency of results between Dask and Pandas.
```

## Tasks completed
```text
Done 01  Downloaded and analyzed a small chunk of the dataset (about 464 MB, 3 variables) with Pandas.
Done 02  Downloaded more than 12 GB of ERA5 data (2024 full-year request profile).
Done 03  Implemented two Pandas analysis pipelines.
      Central India analysis on the compact (464 MB) subset.
      South Asia 2024 analysis on the large (12 GB) subset.
Done 04  Generated derived outputs in CSV, JSON, and PDF figure formats.
Done 05  Organized the codebase into dedicated folders for shared core modules and API request scripts.
Done 06  Dask implementation and direct Pandas vs Dask consistency benchmarking remain in progress.
```

## Repository structure
```text
Dask-Pandas/
│
├── core/
│   ├── config.py                               # dataset paths, regions, thresholds, plotting settings
│   ├── housekeeping.py                         # data layout checks and output directory setup
│   └── utils.py                                # shared output and figure helpers
│
├── api_requests/
│   ├── request_era5_consistent_slice_464mb.py # compact consistent-slice request
│   └── request_era5_full_2024_12gb.py         # full 2024 high-volume request
│
├── central_india_464mb_analysis.py             # Central India (464 MB) Pandas analysis
├── south_asia_2024_large_analysis.py           # South Asia 2024 large-scale Pandas analysis
└── south_asia_naive_load_demo.py               # minimal NetCDF loading demo
```

## Setup instructions
Run these commands in any command-line interface from the project root.

```text
Required raw folders
  data/raw/era5_consistent_slice_extracted/
  data/raw/era5_large_overwhelm/

Verification command
  python -m core.housekeeping
```

This command creates data/results and data/figures if missing, verifies required NetCDF files, and extracts configured archives when applicable.

## Run analyses
Use the same command-line interface to run each script below.

```text
Central India seasonal analysis    : python central_india_464mb_analysis.py
South Asia 2024 large-scale run    : python south_asia_2024_large_analysis.py
Minimal loading example            : python south_asia_naive_load_demo.py
```

## API request scripts
Run these request scripts from the project root in the same command-line interface.

```text
Compact consistent-slice request   : python api_requests/request_era5_consistent_slice_464mb.py
Full 2024 request                  : python api_requests/request_era5_full_2024_12gb.py
```

## Workspace layout
```text
Dask-Pandas/
│
├── central_india_464mb_analysis.py  # Central India analysis entry point
├── south_asia_2024_large_analysis.py # South Asia 2024 analysis entry point
├── south_asia_naive_load_demo.py     # minimal loading demo
│
├── core/
│   ├── config.py                    # dataset paths, regions, thresholds, plotting defaults
│   ├── housekeeping.py              # setup checks and output directory verification
│   └── utils.py                     # shared helper functions
├── api_requests/                    # ERA5 API download scripts
├── docs/                            # assignment PDFs and background material
│
└── data/
    ├── raw/             # local ERA5 payloads and extracted NetCDF files
    ├── results/         # CSV and JSON analysis outputs
    └── figures/         # generated PDF plots for Central India and South Asia analyses
```

Raw ERA5 downloads and extracted NetCDF files are intentionally ignored by Git through .gitignore because they are large local datasets. The derived tables, summaries, figures, and code remain in the repository. The raw data can be downloaded using the API request scripts.

## Team Members
```text
Aditya Dahiya      (dahiya-aditya)
Adwita Joglekar    (adwita314)
Saachi Sirola      (saachisirola)
Brajesh K. Mahto   (Brajesh-k-Mahto)
```