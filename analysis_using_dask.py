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

    #3. Optimal pipeline with xarray, chunking wrt time
    ds_time_1 = xr.open_mfdataset(data_path, chunks={'valid_time': 24}, parallel=True)

    # Spatial averaging
    print("1. Calculating Spatial Average (Global Time Series)...")
    start = time.time()
    spatial_avg = ds_time_1['t2m'].mean(dim=['latitude', 'longitude']).compute()
    print(f"Done in {time.time() - start:.2f} seconds.\n")

    # Temporal aggregation
    print("2. Calculating Temporal Aggregation (Time-Averaged Map)...")
    start = time.time()
    temporal_agg = ds_time_1['t2m'].mean(dim='valid_time').compute()
    print(f"   Done in {time.time() - start:.2f} seconds.\n")

    # Anomaly  computation
    print("3. Calculating Monthly Anomalies...")
    start = time.time()
    climatology = ds_time_1['t2m'].groupby('valid_time.month').mean('valid_time')
    anomalies = (ds_time_1['t2m'].groupby('valid_time.month') - climatology).compute()
    print(f"   Done in {time.time() - start:.2f} seconds.\n")

    #4. To exit the dashboard 
    input("Press Enter to shut down the cluster...")  # move this to the end 