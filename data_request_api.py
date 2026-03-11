import cdsapi

dataset = "reanalysis-era5-single-levels"
request = {
    "product_type": ["reanalysis"],
    "variable": [
        "2m_temperature",
        "surface_pressure",
        "total_precipitation"
    ],
    "year": [
        "2021", "2022", "2023",
        "2024", "2025"
    ],
    "month": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12"
    ],
    "day": [
        "01", "05", "09",
        "13", "17", "21",
        "25", "29"
    ],
    "time": [
        "00:00", "04:00", "08:00",
        "12:00", "16:00", "20:00"
    ],
    "data_format": "netcdf",
    "download_format": "unarchived",
    "area": [50, 60, 0, 110]
}

client = cdsapi.Client() 
client.retrieve(dataset, request).download()
