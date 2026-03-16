"""CDS API request for the compact ERA5 consistent-slice (about 464 MB)."""

import cdsapi


DATASET = "reanalysis-era5-single-levels"
REQUEST = {
    "product_type": ["reanalysis"],
    "variable": [
        "2m_temperature",
        "surface_pressure",
        "total_precipitation",
    ],
    "year": ["2021", "2022", "2023", "2024", "2025"],
    "month": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
    ],
    "day": [
        "01", "05", "09",
        "13", "17", "21",
        "25", "29",
    ],
    "time": [
        "00:00", "04:00", "08:00",
        "12:00", "16:00", "20:00",
    ],
    "data_format": "netcdf",
    "download_format": "unarchived",
    "area": [50, 60, 0, 110],
}


def main() -> None:
    client = cdsapi.Client()
    client.retrieve(DATASET, REQUEST).download("data/raw/era5_consistent_slice_download.nc")


if __name__ == "__main__":
    main()
