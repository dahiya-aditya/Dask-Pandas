"""
Minimal ERA5 data loading example - Large South Asia subset.

This script demonstrates basic NetCDF loading for the 2024 large-scale
South Asia dataset (5-35 N, 65-100 E) using netCDF4 and NumPy.
It flattens the spatial grid into a Pandas DataFrame for further analysis.

Before running: Ensure extracted NetCDF files exist via:
    python -m core.housekeeping
"""

from __future__ import annotations

import netCDF4 as nc
import numpy as np
import pandas as pd

from core.config import LARGE_SOUTH_ASIA


def main() -> None:
    """Load ERA5 data and demonstrate basic grid flattening."""
    # Construct paths from dataset config
    data_dir = LARGE_SOUTH_ASIA["data_dir"]
    instant_path = data_dir / LARGE_SOUTH_ASIA["instant_file"]
    accum_path = data_dir / LARGE_SOUTH_ASIA["accum_file"]

    # Verify files exist
    if not instant_path.exists():
        raise FileNotFoundError(
            f"Missing extracted NetCDF file:\n  {instant_path}\n"
            f"Run: python -m core.housekeeping"
        )
    if not accum_path.exists():
        raise FileNotFoundError(
            f"Missing extracted NetCDF file:\n  {accum_path}\n"
            f"Run: python -m core.housekeeping"
        )

    print("\n" + "=" * 70)
    print(f"Loading {LARGE_SOUTH_ASIA['name']}")
    print("=" * 70)

    # Open instant variables
    with nc.Dataset(instant_path) as inst:
        nt = len(inst.dimensions["valid_time"])
        ny = len(inst.dimensions["latitude"])
        nx = len(inst.dimensions["longitude"])

        time_vals = np.asarray(inst.variables["valid_time"][:])
        lat_vals = np.asarray(inst.variables["latitude"][:])
        lon_vals = np.asarray(inst.variables["longitude"][:])

        t2m = np.asarray(inst.variables["t2m"][:])
        tcc = np.asarray(inst.variables["tcc"][:])

    # Open accumulated variables
    with nc.Dataset(accum_path) as acc:
        ssr = np.asarray(acc.variables["ssr"][:])
        str_nlw = np.asarray(acc.variables["str"][:])
        tisr = np.asarray(acc.variables["tisr"][:])
        tsr = np.asarray(acc.variables["tsr"][:])
        ttr = np.asarray(acc.variables["ttr"][:])

    print(f"\nGrid dimensions:")
    print(f"  Time steps : {nt}")
    print(f"  Latitude   : {ny}")
    print(f"  Longitude  : {nx}")

    print(f"\nData shapes:")
    print(f"  t2m        : {t2m.shape}")
    print(f"  tcc        : {tcc.shape}")
    print(f"  ssr        : {ssr.shape}")
    print(f"  str        : {str_nlw.shape}")
    print(f"  tisr       : {tisr.shape}")
    print(f"  tsr        : {tsr.shape}")
    print(f"  ttr        : {ttr.shape}")

    # Flatten spatial grid into columns
    cells = nt * ny * nx
    print(f"\nFlattening to single table ...")
    print(f"  Total cells: {cells:,}")

    time_flat = np.repeat(time_vals, ny * nx)
    lat_flat = np.tile(np.repeat(lat_vals, nx), nt)
    lon_flat = np.tile(lon_vals, nt * ny)

    df = pd.DataFrame(
        {
            "valid_time": time_flat,
            "latitude": lat_flat,
            "longitude": lon_flat,
            "t2m": t2m.reshape(cells),
            "tcc": tcc.reshape(cells),
            "ssr": ssr.reshape(cells),
            "str": str_nlw.reshape(cells),
            "tisr": tisr.reshape(cells),
            "tsr": tsr.reshape(cells),
            "ttr": ttr.reshape(cells),
        }
    )

    print(f"  DataFrame  : {df.shape}")
    print(f"\n{df.head()}")
    print(f"\nDataFrame info:")
    print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
