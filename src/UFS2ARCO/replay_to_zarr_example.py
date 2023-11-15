"""
This file is the script version of Tim Smith's read_and_store_replay.ipynb notebook.
The demonstration is to read an S3 data store, pick some variables, and write a new
local data file in zarr format.
This leverages USF2ARCO's FV3Dataset class.

The execution depends on the file 'config-replay.yaml' being located in the
folder './scripts' and configures various aspects of this demo.
"""
# required dependancies
# python -m pip install s3fs
# python -m pip install dask
# python -m pip install xarray
# python -m pip install h5netcdf

from datetime import datetime
import sys
from typing import List

import xarray as xr


# This assumes that the UFS2ARCO package is installed or located in a src folder
# relative to the execution path
sys.path.append("./src")
from UFS2ARCO.fv3dataset import FV3Dataset


def replay_path(date: datetime, forecast_hours: List[int], file_prefixes: List[str]) -> List[str]:
    """Generate the S3 bucket names for the date, hours, and filetypes given

    Args:
        date (datetime): the date of interest
        forecast_hours (List[int]): a list of hours to be used
        file_prefixes (List[str]): a list of file prefixes

    Returns:
        List[str]: a list of S3 bucket paths
    """
    upper = "s3://noaa-ufs-gefsv13replay-pds/1deg"

    this_dir = f"{date.year:04d}/{date.month:02d}/{date:%Y%m%d%H}"
    files = []
    for fp in file_prefixes:
        for fhr in forecast_hours:
            files.append(f"{fp}{date:%Y%m%d%H}_fhr{fhr:02d}_control")
    return [f"{upper}/{this_dir}/{this_file}" for this_file in files]


def cached_path(date: datetime, forecast_hours: List[int], file_prefixes: List[str]) -> List[str]:
    """Create a cache path version of S3 bucket descriptors

        date (datetime): the date of interest
        forecast_hours (List[int]): a list of hours to be used
        file_prefixes (List[str]): a list of file prefixes

    Returns:
        List[str]: a list of S3 bucket paths
    """
    return [f"simplecache::{u}" for u in replay_path(date, forecast_hours, file_prefixes)]


def demo() -> None:
    """The process of setting up, reading a replay dataset, and writing it to zarr store (local)
    This does some of the work, but mainly is example function resulting in
    a local zarr file and some print statements to show status
    """
    # the date of interest
    cycle = datetime(1994, 1, 2, 0)

    # show what the S3 bucket descriptors would be
    print(f'Replay data paths: {replay_path(cycle, [0, 6], ["sfg_", "bfg_"])}')

    # actually load the data, but use the cached paths instead of the S3 path directly
    reader = FV3Dataset(path_in=cached_path, config_filename="./scripts/config-replay.yaml")
    ds = reader.open_dataset(cycle, fsspec_kwargs={"s3": {"anon": True}}, engine="h5netcdf")

    # display the temperature data descriptor
    print(f'Temperature data: {ds["tmp"]}')

    # display the chunking information
    print(f"Chunking information: {reader.chunks_out}")

    # store the dataset as a zarr file.  The output path is defined in config-replay.yaml
    reader.store_dataset(ds, mode="w")

    # get the coordinates data by reading back the zarr store (local)
    cds = xr.open_zarr(reader.coords_path)
    print(f"Coordinate data: {cds}")

    # get the data by reading back the zarr store (local)
    xds = xr.open_zarr(f"{reader.forecast_path}")
    print(f"xds: {xds}")
    print(f"xds.tmp: {xds.tmp}")


if __name__ == "__main__":
    demo()
