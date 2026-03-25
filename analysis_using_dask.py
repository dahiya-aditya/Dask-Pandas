import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import xarray as xr
from dask.distributed import LocalCluster

# Setting up Dask cluster 

if __name__ == "__main__":
    print("Starting Dask Local Cluster...")

    # Set up the cluster explicitly
    cluster = LocalCluster() 
    #Give arguments n_worker, threads_per_worker, memory_limit to LocalCluster(). Imp for analysis. For efficieny, no limits are needed, Dask will use 100% of available RAM effeciently. 
    client = cluster.get_client() 

    # Print the dashboard link so you can click it in the terminal
    print(f"--> View the Dask Dashboard at: {client.dashboard_link}") 
    
    input("Press Enter to shut down the cluster...")
