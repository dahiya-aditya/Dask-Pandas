"""CDS API request for the full 2024 ERA5 single-level dataset (about 12 GB)."""

import cdsapi


DATASET = "reanalysis-era5-single-levels"
REQUEST = {
    "product_type": ["reanalysis"],
    "variable": [
        "2m_temperature",
        "surface_net_solar_radiation",
        "surface_net_thermal_radiation",
        "toa_incident_solar_radiation",
        "top_net_solar_radiation",
        "top_net_thermal_radiation",
        "total_cloud_cover",
    ],
    "year": ["2024"],
    "month": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
    ],
    "day": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
        "13", "14", "15",
        "16", "17", "18",
        "19", "20", "21",
        "22", "23", "24",
        "25", "26", "27",
        "28", "29", "30",
        "31",
    ],
    "time": ["00:00", "06:00", "12:00", "18:00"],
    "data_format": "netcdf",
    "download_format": "zip",
}


def main() -> None:
    client = cdsapi.Client()
    client.retrieve(DATASET, REQUEST).download("data/raw/era5_full_2024_12gb.zip")


if __name__ == "__main__":
    main()
