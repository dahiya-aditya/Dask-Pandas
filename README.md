# Dask-Pandas

## Team Members
Aditya Dahiya (dahiya-aditya)  
Adwita Joglekar (adwita314)  
Saachi Sirola (saachisirola)  
Brajesh ()                       

## Objective
To investigate how Dask enables scalable data processing for high-resolution weather datasets (ERA5) when in-memory Pandas workflows become impractical.

## Dataset
#### ERA5 hourly data on single levels from 1940 to present. 
ERA5 is the fifth-generation global climate reanalysis produced by CDS ECMWF. It provides hourly estimates of a wide range of atmospheric, ocean-wave, and land-surface variables from 1940 to the present. The dataset is distributed by the Copernicus Climate Change Service, and features a high-resolution 0.25° x 0.25° grid. Because of its high dimentionality it serves as an excellent case study for evaluating how Dask enables scalable analysis when Pandas becomes impractical.
Data source: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels

## Workflow
1. Download a small chunk of the huge dataset and analyse it in Pandas and Dask.
2. Implement baseline analysis on this small chunk with Pandas.
3. Download a larger subset of the dataset and ensure that Pandas crashes. 
4. Implement a similar pipeline using Dask, using lazy loading and chunked computation. 
5. Examine consistancy of results between Dask and Pandas. 

## Tasks completed
1. Downloaded a small chunk of the dataset (464.2 MB) to analyse with Pandas. 