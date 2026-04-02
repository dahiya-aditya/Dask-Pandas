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
    ds = xr.open_mfdataset(data_path, chunks="auto", parallel=True)
    south_asia_ds = ds.sel(latitude=slice(35, 5), longitude=slice(65, 100))
    print("Dataset opened with Dask chunks. Ready for analysis.\n")
    print("Dataset variables:", list(ds.data_vars.keys()), "\n" )

    # Spatial averaging 

    #1. TOA incident short wave radiation (tisr)
    # tisr is the TOA incident short wave radiation variable in the dataset. We will calculate the spatial average of tisr across the globe and also over South Asia.
    print("Calculating Spatial Average for tisr (Global and South Asia)...")
    start = time.time()

    tisr_avg_global = ds['tisr'].mean(dim=['latitude', 'longitude'])
    tisr_avg_south_asia = south_asia_ds['tisr'].mean(dim=['latitude', 'longitude'])

    spatial_avg, spatial_avg_south_asia = dask.compute(tisr_avg_global, tisr_avg_south_asia)
    print(f"Done in {time.time() - start:.2f} seconds.\n")

    # this analysis however is flawed as taking mean temperature across the globe is not scientifically meaningful.
    print("Note: Spatial averaging across the globe may not be scientifically meaningful.\n")

    # Plotting spatial average for the Global time series and South Asia time series
    plt.figure(figsize=(12, 6))
    plt.plot(tisr_avg_global['valid_time'], (tisr_avg_global - 273.15).rolling(valid_time=24, center=True).mean(), label='Global Average')
    plt.plot(tisr_avg_global['valid_time'], tisr_avg_global - 273.15, color='#1f77b4', alpha=0.3, label='Hourly Range')
    plt.plot(tisr_avg_south_asia['valid_time'], (tisr_avg_south_asia - 273.15).rolling(valid_time=24, center=True).mean(), color="#ff120e", label='South Asia Average')
    plt.plot(tisr_avg_south_asia['valid_time'], tisr_avg_south_asia - 273.15, color="#ff120e", alpha=0.3, label='Hourly Range')

    plt.xlabel('Time')
    plt.ylabel('TOA Incident Short Wave Radiation (W/m²)')
    plt.title('Spatial Average of TOA Incident Short Wave Radiation')
    plt.legend()
    plt.grid()

    print("Displaying tisr plot. CLOSE THE PLOT WINDOW TO CONTINUE TO TCC...")
    plt.show()


    #2. Surface net thermal radiation (str)
    print("\nCalculating Spatial Average of Surface Net Thermal Radiation (Global and South Asia)...")
    start = time.time()

    global_avg_str = ds['str'].mean(dim=['latitude', 'longitude'])
    south_asia_avg_str = south_asia_ds['str'].mean(dim=['latitude', 'longitude'])   

    global_avg_str, south_asia_avg_str = dask.compute(global_avg_str, south_asia_avg_str)
    print(f"Done in {time.time() - start:.2f} seconds.\n")  

    # Plotting spatial average for the Global time series and South Asia time series
    plt.figure(figsize=(12, 6))
    plt.plot(global_avg_str['valid_time'], global_avg_str.rolling(valid_time=24, center=True).mean(), color='#2ca02c', label='Global Average')
    plt.plot(global_avg_str['valid_time'], global_avg_str, color='#2ca02c', alpha=0.3, label='Global Hourly Range')
    plt.plot(south_asia_avg_str['valid_time'], south_asia_avg_str.rolling(valid_time=24, center=True).mean(), color='#9467bd', label='South Asia Average')
    plt.plot(south_asia_avg_str['valid_time'], south_asia_avg_str, color='#9467bd', alpha=0.3, label='South Asia Hourly Range')

    plt.xlabel('Time')
    plt.ylabel('Surface Net Thermal Radiation (W/m²)')
    plt.title('Spatial Average of Surface Net Thermal Radiation')
    plt.legend()
    plt.grid()
    plt.show() 


    #3. Surface net short wave radiation (ssr)
    print("\nCalculating Spatial Average of Surface Net Short Wave Radiation (Global and South Asia)...")
    start = time.time()

    global_avg_ssr = ds['ssr'].mean(dim=['latitude', 'longitude'])
    south_asia_avg_ssr = south_asia_ds['ssr'].mean(dim=['latitude', 'longitude'])   

    global_avg_ssr, south_asia_avg_ssr = dask.compute(global_avg_ssr, south_asia_avg_ssr)
    print(f"Done in {time.time() - start:.2f} seconds.\n")  

    # Plotting spatial average for the Global time series and South Asia time series
    plt.figure(figsize=(12, 6))
    plt.plot(global_avg_ssr['valid_time'], global_avg_ssr.rolling(valid_time=24, center=True).mean(), color='#2ca02c', label='Global Average')
    plt.plot(global_avg_ssr['valid_time'], global_avg_ssr, color='#2ca02c', alpha=0.3, label='Global Hourly Range')
    plt.plot(south_asia_avg_ssr['valid_time'], south_asia_avg_ssr.rolling(valid_time=24, center=True).mean(), color='#9467bd', label='South Asia Average')
    plt.plot(south_asia_avg_ssr['valid_time'], south_asia_avg_ssr, color='#9467bd', alpha=0.3, label='South Asia Hourly Range')

    plt.xlabel('Time')
    plt.ylabel('Surface Net Short Wave Radiation (W/m²)')
    plt.title('Spatial Average of Surface Net Short Wave Radiation')
    plt.legend()
    plt.grid()
    plt.show() 