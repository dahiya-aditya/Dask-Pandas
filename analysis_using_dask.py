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
import matplotlib.pyplot as plt

from core.config import HEAT_DANGER_THRESHOLD, HEAT_WARN_THRESHOLD, PLOT_STYLE 

if __name__ == "__main__":
    #1. Setting up Dask cluster 
    print("Starting Dask Local Cluster...")

    # Set up the cluster explicitly
    cluster = LocalCluster() 

    # Give arguments n_worker, threads_per_worker, memory_limit to LocalCluster(). Imp for analysis. For efficieny, no limits are needed, Dask will use 100% of available RAM effeciently. 
    client = cluster.get_client() 

    # Print the dashboard link so you can click it in the terminal
    print(f"--> View the Dask Dashboard at: {client.dashboard_link}") 

    #2. Specifying data path
    data_path = "api_requests/data/raw/era5_12gb_2024_adwita/*.nc"

    #3. Optimal pipeline with xarray - temporal and spatial analysis using Dask
    # Chunking wrt valid_time dimension.  
    ds = xr.open_mfdataset(data_path, chunks={'valid_time': 24}, parallel=True)
    south_asia_ds = ds.sel(latitude=slice(35, 5), longitude=slice(65, 100))
    print("Dataset opened with Dask chunks. Ready for analysis.\n")
    print("Dataset variables:", list(ds.data_vars.keys()), "\n" )

    # Spatial averaging 

    #1. t2m
    # t2m is the 2m temperature variable in the dataset. We will calculate the spatial average of t2m across the globe and also over South Asia.
    print("Calculating Spatial Average for t2m (Global and South Asia)...")
    start = time.time()

    t2m_avg_global = ds['t2m'].mean(dim=['latitude', 'longitude'])
    t2m_avg_south_asia = south_asia_ds['t2m'].mean(dim=['latitude', 'longitude'])

    spatial_avg, spatial_avg_south_asia = dask.compute(t2m_avg_global, t2m_avg_south_asia)
    print(f"Done in {time.time() - start:.2f} seconds.\n")

    # this analysis however is flawed as taking mean temperature across the globe is not scientifically meaningful.
    print("Note: Spatial averaging across the globe may not be scientifically meaningful.\n")

    # Plotting spatial average for the Global time series and South Asia time series
    plt.figure(figsize=(12, 6))
    plt.plot(t2m_avg_global['valid_time'], (t2m_avg_global - 273.15).rolling(valid_time=24, center=True).mean(), label='Global Average')
    plt.plot(t2m_avg_global['valid_time'], t2m_avg_global - 273.15, color='#1f77b4', alpha=0.3, label='Hourly Range')
    plt.plot(t2m_avg_south_asia['valid_time'], (t2m_avg_south_asia - 273.15).rolling(valid_time=24, center=True).mean(), color="#ff120e", label='South Asia Average')
    plt.plot(t2m_avg_south_asia['valid_time'], t2m_avg_south_asia - 273.15, color="#ff120e", alpha=0.3, label='Hourly Range')

    plt.xlabel('Time')
    plt.ylabel('Temperature (°C)')
    plt.title('Spatial Average of 2m Temperature')
    plt.legend()
    plt.grid()
    plt.show()


    #2. Total Cloud Cover (tcc)
    print("Calculating Spatial Average of Total Cloud Cover (Global and South Asia)...")
    start = time.time()

    ds_chunked = dask.chunk({'valid_time': 168, 'latitude': 50, 'longitude': 50}, data_path, parallel=True)
    # south_asia_ds_chunked = south_asia_ds.chunk({'valid_time': 168, 'latitude': 50, 'longitude': 50})
    south_asia_ds_chunked = ds_chunked.sel(latitude=slice(35, 5), longitude=slice(65, 100))

    global_avg_tcc = ds_chunked['tcc'].mean(dim=['latitude', 'longitude'])
    south_asia_avg_tcc = south_asia_ds_chunked['tcc'].mean(dim=['latitude', 'longitude'])   

    global_avg_tcc, south_asia_avg_tcc = dask.compute(global_avg_tcc, south_asia_avg_tcc)
    print(f"Done in {time.time() - start:.2f} seconds.\n")  

    # Plotting spatial average for the Global time series and South Asia time series
    plt.figure(figsize=(12, 6))
    plt.plot(global_avg_tcc['valid_time'], global_avg_tcc.rolling(valid_time=24, center=True).mean(), color='#2ca02c', label='Global Average')
    plt.plot(global_avg_tcc['valid_time'], global_avg_tcc, color='#2ca02c', alpha=0.3, label='Global Hourly Range')
    plt.plot(south_asia_avg_tcc['valid_time'], south_asia_avg_tcc.rolling(valid_time=24, center=True).mean(), color='#9467bd', label='South Asia Average')
    plt.plot(south_asia_avg_tcc['valid_time'], south_asia_avg_tcc, color='#9467bd', alpha=0.3, label='South Asia Hourly Range')

    plt.xlabel('Time')
    plt.ylabel('Total Cloud Cover (%)')
    plt.title('Spatial Average of Total Cloud Cover')
    plt.legend()
    plt.grid()
    plt.show() 


## these dont run simultanuously as they are computationally intensive. Run one at a time. 
## solution: make different scripts for different chunking strategies for all variables (at once). 