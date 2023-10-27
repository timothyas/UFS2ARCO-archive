from datetime import datetime
import numpy as np
import xarray as xr

from .ufsdataset import UFSDataset

class FV3Dataset(UFSDataset):
    def __init__(self, *args, **kwargs):
        super(FV3Dataset, self).__init__(*args, **kwargs)
        self.zarr_name = "fv3.zarr"
        self.chunks_in = self.chunks_in if len(self.chunks_in) != 0 else {
            "pfull": 5,
            "grid_yt": -1,
            "grid_xt": -1,
        }

        self.chunks_out = self.chunks_out if len(self.chunks_out) != 0 else {
            "time": 1,
            "pfull": 5,
            "grid_yt": 30,
            "grid_xt": 30,
        }

    def open_dataset(self, cycle: datetime, fsspec_kwargs=None, **kwargs):
        xds = super().open_dataset(cycle, fsspec_kwargs, **kwargs)

        # Deal with time
        xds = xds.rename({"time": "cftime"})
        time = self._cftime2time(xds["cftime"])
        xds["time"] = xr.DataArray(
            time,
            coords=xds["cftime"].coords,
            dims=xds["cftime"].dims,
            attrs={
                "long_name": "time",
                "axis": "T",
            },
        )
        xds["ftime"] = xr.DataArray(
            time - np.datetime64(cycle),
            coords=xds["cftime"].coords,
            dims=xds["cftime"].dims,
            attrs={
                "long_name": "forecast_time",
                "description": f"time passed since {str(cycle)}",
                "axis": "T",
            },
        )
        xds = xds.swap_dims({"cftime": "time"})
        xds = xds.set_coords(["time", "cftime", "ftime"])

        # convert ak/bk attrs to coordinate arrays
        for key in ["ak", "bk"]:
            if key in xds.attrs:
                xds[key] = xr.DataArray(
                    xds.attrs.pop(key),
                    coords=xds["phalf"].coords,
                    dims=xds["phalf"].dims,
                )
                xds = xds.set_coords(key)

        # rename grid_yt.long_name to avoid typo
        xds["grid_yt"].attrs["long_name"] = "T-cell latitude"
        return xds
