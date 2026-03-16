# Dask-Pandas

## Objective
To investigate how Dask enables scalable data processing for high-resolution weather datasets (ERA5) when in-memory Pandas workflows become impractical.

## Dataset
ERA5 hourly data on single levels from 1940 to present.

ERA5 is the fifth-generation global climate reanalysis produced by CDS ECMWF. It provides hourly estimates of a wide range of atmospheric, ocean-wave, and land-surface variables from 1940 to the present. The dataset is distributed by the Copernicus Climate Change Service and features a high-resolution 0.25 x 0.25 degree grid. Because of its high dimensionality, it serves as an excellent case study for evaluating how Dask enables scalable analysis when Pandas becomes impractical.

Data source: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels

Current request profiles used in this repository:
- Compact consistent-slice request (about 464 MB) for baseline Pandas analysis.
- Full 2024 request (about 12 GB) for large-volume processing and scalability testing.

## Workflow
1. Download a small chunk of the huge dataset and analyze it using Pandas and Dask.
2. Implement baseline analysis on this small chunk with Pandas.
3. Download a larger subset of the dataset and ensure that Pandas crashes or becomes impractical.
4. Implement a similar pipeline using Dask, using lazy loading and chunked computation.
5. Examine the consistency of results between Dask and Pandas.

## Tasks completed
1. Downloaded and analyzed a small chunk of the dataset (about 464 MB, 3 variables) with Pandas.
2. Downloaded more than 12 GB of ERA5 data (2024 full-year request profile).
3. Implemented two Pandas analysis pipelines:
   - Central India analysis on the compact (464 MB) subset.
   - South Asia 2024 analysis on the large (12 GB) subset.
4. Generated derived outputs in CSV, JSON, and PDF figure formats.
5. Organized the codebase into dedicated folders for shared core modules and API request scripts.
6. Dask implementation and direct Pandas vs Dask consistency benchmarking remain in progress.

## Repository structure
- core/config.py: centralized dataset paths, regions, thresholds, and plotting settings.
- core/housekeeping.py: setup utility to verify data layout and create output directories.
- core/utils.py: shared helpers for output management and figure saving.
- central_india_464mb_analysis.py: Central India (464 MB) Pandas analysis.
- south_asia_2024_large_analysis.py: South Asia 2024 large-scale Pandas analysis.
- south_asia_naive_load_demo.py: minimal NetCDF loading demo.
- api_requests/request_era5_consistent_slice_464mb.py: CDS API request script for the compact consistent-slice dataset.
- api_requests/request_era5_full_2024_12gb.py: CDS API request script for the full 2024 high-volume dataset.

## Setup instructions
Run these commands in any command-line interface from the project root.

1. Place extracted NetCDF files in:
   - data/raw/era5_consistent_slice_extracted/
   - data/raw/era5_large_overwhelm/

2. Verify paths and output structure:
   - python -m core.housekeeping

This command creates data/results and data/figures if missing, verifies required NetCDF files, and extracts configured archives when applicable.

## Run analyses
Use the same command-line interface to run each script below.

- Central India seasonal analysis:
  - python central_india_464mb_analysis.py
- South Asia 2024 large-scale analysis:
  - python south_asia_2024_large_analysis.py
- Minimal loading example:
  - python south_asia_naive_load_demo.py

## API request scripts
Run these request scripts from the project root in the same command-line interface.

- Compact consistent-slice request:
  - python api_requests/request_era5_consistent_slice_464mb.py
- Full 2024 request:
  - python api_requests/request_era5_full_2024_12gb.py

## Workspace layout
- docs/: assignment PDFs and background material.
- data/raw/: local ERA5 payloads and extracted NetCDF files used for analysis.
- data/results/: CSV and JSON analysis outputs that summarize the Pandas runs.
- data/figures/: generated PDF plots for the Central India and South Asia analyses.

Raw ERA5 downloads and extracted NetCDF files are intentionally ignored by Git through .gitignore because they are large local datasets. The derived tables, summaries, figures, and code remain in the repository. The raw data can be downloaded using the API request scripts.

## Team Members
Aditya Dahiya (dahiya-aditya)
Adwita Joglekar (adwita314)
Saachi Sirola (saachisirola)
Brajesh K. Mahto (Brajesh-k-Mahto)