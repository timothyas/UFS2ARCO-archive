
from os.path import join
import datetime
import fsspec
import xarray as xr

import sys
sys.path.append("../src")
from UFS2ARCO import FV3Dataset

def local_path(date: datetime.datetime, fhrs: list, file_prefixes: list):

    upper = "../test_files"
    files = []
    for fp in file_prefixes:
        for fhr in fhrs:
            files.append(
                    f"{fp}{date.year:04d}{date.month:02d}{date.day:02d}{date.hour:02d}_fhr{fhr:02d}_control")
    return [join(upper, this_file) for this_file in files]



if __name__ == "__main__":

    replay = FV3Dataset(path_in=local_path, config_filename="config-replay.yaml")

    date = datetime.datetime(year=1994, month=1, day=1, hour=0)
    ds = replay.open_dataset(date)
    replay.store_dataset(ds, mode="w")#, mode="a", append_dim="time")
