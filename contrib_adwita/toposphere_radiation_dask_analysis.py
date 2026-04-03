import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import xarray as xr
from dask.distributed import LocalCluster
import zipfile
import os
import time
import glob
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from _bootstrap import ensure_repo_root

ensure_repo_root()

from core import config, utils
from core.config import RAW_DIR

if __name__ == "__main__":
    utils.ensure_output_directories()
    config.FIGURES_DIR = config.FIGURES_CONTRIBUTOR_DIRS["adwita"]
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    #1. Setting up Dask cluster 
    print("Starting Dask Local Cluster...")

    # Set up the cluster explicitly
    cluster = LocalCluster() 

    # Give arguments n_worker, threads_per_worker, memory_limit to LocalCluster(). Imp for analysis. For efficieny, no limits are needed, Dask will use 100% of available RAM effeciently. 
    client = cluster.get_client() 

    # Print the dashboard link so you can click it in the terminal
    print(f"--> View the Dask Dashboard at: {client.dashboard_link}") 

    try:
        #2. Specifying data path
        data_path = str(RAW_DIR / "era5_large_overwhelm" / "*.nc")
        
        #3. Optimal pipeline with xarray - temporal and spatial analysis using Dask
        # Chunking wrt valid_time dimension.  
        ds = xr.open_mfdataset(data_path, chunks={'valid_time': 168, 'latitude': 50, 'longitude': 50}, parallel=True, compat='no_conflicts')
        south_asia_ds = ds.sel(latitude=slice(35, 5), longitude=slice(65, 100))
        print("Dataset opened with Dask chunks. Ready for analysis.\n")
        print("Dataset variables:", list(ds.data_vars.keys()), "\n" )

        
        # Spatial averaging 

        #1. Top net thermal radiation (ttr)
        print("Calculating Spatial Average for ttr (Global and South Asia)...")
        start = time.time()

        ttr_avg_global = ds['ttr'].mean(dim=['latitude', 'longitude'])
        ttr_avg_south_asia = south_asia_ds['ttr'].mean(dim=['latitude', 'longitude'])

        ttr_spatial_avg_global, ttr_spatial_avg_south_asia = dask.compute(ttr_avg_global, ttr_avg_south_asia)
        print(f"Done in {time.time() - start:.2f} seconds.\n")

        # this analysis however is flawed as taking mean temperature across the globe is not scientifically meaningful.
        print("Note: Spatial averaging across the globe may not be scientifically meaningful.\n")

        # Plotting spatial average for the Global time series and South Asia time series
        plt.figure(figsize=(12, 6))
        plt.plot(ttr_spatial_avg_global['valid_time'], (ttr_spatial_avg_global / 3600).rolling(valid_time=24, center=True).mean(), label='Global Average')
        plt.plot(ttr_spatial_avg_global['valid_time'], ttr_spatial_avg_global / 3600, color='#1f77b4', alpha=0.3, label='Hourly Range')
        plt.plot(ttr_spatial_avg_south_asia['valid_time'], (ttr_spatial_avg_south_asia / 3600).rolling(valid_time=24, center=True).mean(), color="#ff120e", label='South Asia Average')
        plt.plot(ttr_spatial_avg_south_asia['valid_time'], ttr_spatial_avg_south_asia / 3600, color="#ff120e", alpha=0.3, label='Hourly Range')

        plt.xlabel('Time')
        plt.ylabel('Top Net Thermal Radiation (W/m²)')
        plt.title('Spatial Average of Top Net Thermal Radiation')
        plt.legend()
        plt.grid()
        out = utils.save_figure(plt.gcf(), "ttr_global_vs_southasia", formats=["pdf"])
        print(f"Saved figure: {out}.pdf")


        #2. Top short wave radiation (tsr)
        print("Calculating Spatial Average of Top Short Wave Radiation (Global and South Asia)...")
        start = time.time()

        global_avg_tsr = ds['tsr'].mean(dim=['latitude', 'longitude'])
        south_asia_avg_tsr = south_asia_ds['tsr'].mean(dim=['latitude', 'longitude'])

        global_avg_tsr, south_asia_avg_tsr = dask.compute(global_avg_tsr, south_asia_avg_tsr)
        print(f"Done in {time.time() - start:.2f} seconds.\n")  

        # Plotting spatial average for the Global time series and South Asia time series
        plt.figure(figsize=(12, 6))
        plt.plot(global_avg_tsr['valid_time'], (global_avg_tsr / 3600).rolling(valid_time=24, center=True).mean(), color='#2ca02c', label='Global Average')
        plt.plot(global_avg_tsr['valid_time'], global_avg_tsr / 3600, color='#2ca02c', alpha=0.3, label='Global Hourly Range')
        plt.plot(south_asia_avg_tsr['valid_time'], (south_asia_avg_tsr / 3600).rolling(valid_time=24, center=True).mean(), color='#9467bd', label='South Asia Average')
        plt.plot(south_asia_avg_tsr['valid_time'], south_asia_avg_tsr / 3600, color='#9467bd', alpha=0.3, label='South Asia Hourly Range')

        plt.xlabel('Time')
        plt.ylabel('Top Short Wave Radiation (W/m²)')
        plt.title('Spatial Average of Top Short Wave Radiation')
        plt.legend()
        plt.grid()
        out = utils.save_figure(plt.gcf(), "tsr_global_vs_southasia", formats=["pdf"])
        print(f"Saved figure: {out}.pdf")
    finally:
        plt.close("all")
        client.close()
        cluster.close()