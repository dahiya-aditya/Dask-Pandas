import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import xarray as xr
from dask.distributed import LocalCluster
import zipfile
import os
import time
import glob

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
    print("Dataset opened with Dask chunks. Ready for analysis.\n")
    print("Variables in the dataset:", list(ds.data_vars))

    # Spatial averaging
    print("1. Calculating Spatial Average (Global Time Series)...")
    start = time.time()
    spatial_avg = ds['t2m'].mean(dim=['latitude', 'longitude']).compute()
    print(f"Done in {time.time() - start:.2f} seconds.\n")
    # this analysis however is flawed as taking mean temperature across the globe is not scientifically meaningful.
    print("Note: Spatial averaging across the globe may not be scientifically meaningful.\n")
    # to make it more meaningful, we can do spatial averaging over a specific region, e.g., South Asia.
    print("1.1 Calculating Spatial Average over South Asia (5N-35N, 65E-100E)...")
    start = time.time()
    south_asia_ds = ds.sel(latitude=slice(35, 5), longitude=slice(65, 100))
    spatial_avg_south_asia = south_asia_ds['t2m'].mean(dim=['latitude', 'longitude']).compute()
    print(f"Done in {time.time() - start:.2f} seconds.\n")

    # Plotting spatial average for the Global time series and South Asia time series
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    plt.plot(spatial_avg['valid_time'], spatial_avg.values - 273.15, label='Global Average')
    plt.plot(spatial_avg_south_asia['valid_time'], spatial_avg_south_asia.values - 273.15, label='South Asia Average')
    plt.xlabel('Time')
    plt.ylabel('Temperature (°C)')
    plt.title('Spatial Average of 2m Temperature')
    plt.legend()
    plt.grid()
    plt.show()  

    # # Temporal aggregation
    # print("2. Calculating Temporal Aggregation (Time-Averaged Map)...")
    # start = time.time()
    # temporal_agg = ds['t2m'].mean(dim='valid_time').compute()
    # print(f"   Done in {time.time() - start:.2f} seconds.\n")

    # # Anomaly computation
    # print("3. Calculating Monthly Anomalies...")
    # start = time.time()
    # climatology = ds['t2m'].groupby('valid_time.month').mean('valid_time')
    # anomalies = (ds['t2m'].groupby('valid_time.month') - climatology).compute()
    # print(f"   Done in {time.time() - start:.2f} seconds.\n")

    # #4. To exit the dashboard 
    # input("Press Enter to shut down the cluster...")

