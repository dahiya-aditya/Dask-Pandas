"""
Housekeeping utility: verify and initialize analysis directory structure.

This script checks for required directories and files, and extracts
zip archives as needed. Run this once before running any analysis scripts.

Usage:
    python -m core.housekeeping          # Check/extract all datasets
    python -m core.housekeeping --verify # Only verify, do not extract
"""

from __future__ import annotations

import argparse
import shutil
import sys
import zipfile
from pathlib import Path

from core import config


def ensure_directory(directory: Path) -> bool:
    """Create directory if missing. Return True if created, False if existed."""
    if directory.exists():
        if directory.is_dir():
            return False
    else:
        directory.mkdir(parents=True, exist_ok=True)
        return True


def extract_zip_to_directory(
    archive_path: Path,
    extract_dir: Path,
    verbose: bool = True,
) -> list[Path]:
    """
    Extract NetCDF files from a zip archive to extract_dir.
    
    Parameters
    ----------
    archive_path : Path
        Path to the .zip file
    extract_dir : Path
        Directory to extract NetCDF files into
    verbose : bool
        Print progress messages
        
    Returns
    -------
    list[Path]
        Paths to extracted NetCDF files
    """
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive not found: {archive_path}")

    if verbose:
        print(f"  Extracting from: {archive_path.name}")

    extracted_paths: list[Path] = []
    with zipfile.ZipFile(archive_path) as zf:
        nc_members = [name for name in zf.namelist() if name.lower().endswith(".nc")]
        if not nc_members:
            raise RuntimeError(f"No NetCDF files found in {archive_path}")

        for member in nc_members:
            output_path = extract_dir / Path(member).name
            if output_path.exists():
                if verbose:
                    print(f"    ✓ {output_path.name} (exists)")
            else:
                with zf.open(member) as src, output_path.open("wb") as dst:
                    # Stream copy in chunks to avoid loading very large NetCDF
                    # members fully into memory.
                    shutil.copyfileobj(src, dst, length=16 * 1024 * 1024)
                if verbose:
                    print(f"    ✓ {output_path.name}")
            extracted_paths.append(output_path)

    return extracted_paths


def verify_dataset(dataset: config.DatasetConfig, verbose: bool = True) -> bool:
    """
    Verify that a dataset has all required files.
    
    Returns True if valid, False otherwise.
    """
    data_dir = dataset["data_dir"]
    instant = data_dir / dataset["instant_file"]
    accum = data_dir / dataset["accum_file"]

    if instant.exists() and accum.exists():
        if verbose:
            print(f"✓ {dataset['name']:<40} : ready")
        return True
    else:
        if verbose:
            print(f"✗ {dataset['name']:<40} : missing files")
            if not instant.exists():
                print(f"    Missing: {instant}")
            if not accum.exists():
                print(f"    Missing: {accum}")
        return False


def initialize_datasets(
    datasets: list[config.DatasetConfig],
    extract: bool = True,
    verbose: bool = True,
) -> dict[str, bool]:
    """
    Initialize all datasets: create directories, extract archives if needed.
    
    Parameters
    ----------
    datasets : list[DatasetConfig]
        Datasets to initialize
    extract : bool
        Whether to extract archives (vs. only verify existing)
    verbose : bool
        Print progress messages
        
    Returns
    -------
    dict[str, bool]
        Status for each dataset: {name: is_ready}
    """
    results = {}

    for dataset in datasets:
        name = dataset["name"]
        data_dir = dataset["data_dir"]
        archive = dataset["archive_zip"]

        if verbose:
            print(f"\n{name}")
            print("-" * 60)

        # Create data directory
        created = ensure_directory(data_dir)
        if created and verbose:
            print(f"  Created: {data_dir}")

        # If required files are already present, do not require archive extraction.
        already_ready = verify_dataset(dataset, verbose=False)
        if already_ready:
            if verbose:
                print(f"✓ {name:<40} : ready")
            results[name] = True
            continue

        # Extract archive if needed
        if extract and archive is not None:
            if not archive.exists():
                if verbose:
                    print(f"  Archive not found: {archive}")
                results[name] = False
                continue
            try:
                extract_zip_to_directory(archive, data_dir, verbose=verbose)
            except Exception as e:
                if verbose:
                    print(f"  Error extracting: {e}")
                results[name] = False
                continue

        # Verify completion
        is_ready = verify_dataset(dataset, verbose=verbose)
        results[name] = is_ready

    return results


def main() -> int:
    """Run housekeeping: verify or initialize all datasets."""
    parser = argparse.ArgumentParser(
        description="Verify and initialize ERA5 analysis datasets.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Only verify existing files; do not extract archives",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print detailed progress messages",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("ERA5 Analysis - Directory Structure Verification")
    print("=" * 60)

    # Create output directories
    print("\nEnsuring output directories ...")
    ensure_directory(config.RESULTS_DIR)
    ensure_directory(config.FIGURES_DIR)
    print(f"  ✓ {config.RESULTS_DIR}")
    print(f"  ✓ {config.FIGURES_DIR}")

    # Initialize or verify datasets
    datasets = [config.CONSISTENT_SLICE, config.LARGE_SOUTH_ASIA]
    extract_mode = not args.verify
    results = initialize_datasets(datasets, extract=extract_mode, verbose=True)

    # Summary
    print("\n" + "=" * 60)
    all_ready = all(results.values())
    if all_ready:
        print("✓ All datasets ready. You can now run the analysis scripts.")
        return 0
    else:
        failed = [name for name, ready in results.items() if not ready]
        print(f"✗ {len(failed)} dataset(s) failed:")
        for name in failed:
            print(f"  - {name}")
        print("\nPlease verify zip files exist and run python -m core.housekeeping again.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
