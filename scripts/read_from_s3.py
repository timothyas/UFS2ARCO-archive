
from os.path import join
import datetime
import fsspec
import xarray as xr

from xesn.timer import Timer
import sys
sys.path.append("../src")
from UFS2ARCO import FV3Dataset

def replay_path(date: datetime.datetime, fhrs: list):
    """TODO: expand to dynamics variables"""

    upper = "s3://noaa-ufs-gefsv13replay-pds/1deg"
    this_dir = f"{date.year:04d}/{date.month:02d}/{date.year:04d}{date.month:02d}{date.day:02d}{date.hour:02d}"
    files = [f"bfg_{date.year:04d}{date.month:02d}{date.day:02d}{date.hour:02d}_fhr{fhr:02d}_control"
             for fhr in fhrs]
    return [join(upper, this_dir, this_file) for this_file in files]

def cached_path(date: datetime.datetime, fhrs: list):
    return [f"simplecache::{u}" for u in replay_path(date, fhrs)]




if __name__ == "__main__":

    localtime = Timer()

    replay = FV3Dataset(path_in=cached_path, config_filename="config-replay.yaml")

    date = datetime.datetime(year=1994, month=1, day=1, hour=0)

    localtime.start("reading")
    ds = replay.open_dataset(date, fsspec_kwargs={"s3":{"anon": True}}, engine="h5netcdf")
    localtime.stop()
    localtime.start("writing")
    replay.store_dataset(ds)
    localtime.stop()
