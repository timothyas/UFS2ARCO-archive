import os
from os.path import join
import fsspec
import yaml
import warnings

import numpy as np
import xarray as xr
from zarr import NestedDirectoryStore

from datetime import datetime, timedelta
from cftime import DatetimeJulian

class UFSDataset():
    """Open and store a UFS generated NetCDF dataset to zarr from a single model component (FV3, MOM6, CICE6)
    and a single DA window. Note that this class does nothing on its own, only the children
    (FV3Dataset, MOMDataset, and CICEDataset) will work.

    The two main methods that are useful are :meth:`open_dataset` and :meth:`store_dataset`.
    """
    path_out        = ""
    forecast_hours  = None

    chunks_in       = None
    chunks_out      = None
    coords          = None
    data_vars       = None
    zarr_name       = None

    @property
    def forecast_path(self):
        """Where to write forecast data variables to"""
        return join(self.path_out, "forecast", self.zarr_name)

    @property
    def coords_path(self):
        """Where to write static coordinates to"""
        return join(self.path_out, "coordinates", self.zarr_name)

    @property
    def default_open_dataset_kwargs(self):
        kw = {"parallel"        : True,
              "chunks"          : self.chunks_in,
              "decode_times"    : True,
              }
        return kw


    def __init__(self, path_in, config_filename):
        """Look for the config yaml file, grab from it:
        Required: lists of variables as coords and data_vars
        Optional: chunks_in and chunks_out, determining how to read in the data and how to store it

        Args:
            path_in (callable): map the following arguments to a path (str):
                cycle (datetime.datetime): with the date/time of the current DA cycle (to be read)
                forecast_hours (list of int): with the hours of forecast data to read, e.g. [3, 6]
            config_filename (str): to yaml file containing the overall configuration

        Sets Attributes:
            coords, data_vars (list): with variable names of coordinates and data variables
            chunks_in, chunks_out (dict): specifying how to chunk the data when reading and writing
            filenames_in, zarr_name (str): denotes the files to read in and the name to write out to
            fhr (list): with e.g. ["0h", "3h"] to select the output at forecast hour 0 and 3
        """

        super(UFSDataset, self).__init__()
        name = self.__class__.__name__ # e.g., FV3Dataset, MOMDataset

        self.path_in = path_in
        with open(config_filename, "r") as f:
            contents = yaml.safe_load(f)
            self.config = contents[name]

        # look for these requited inputs
        for key in ["path_out", "forecast_hours"]: # TBD: add filenames
            try:
                setattr(self, key, self.config[key])
            except KeyError:
                raise KeyError(f"{name}.__init__: Could not find {key} in {config_filename}, but this is required")

        # look for these optional inputs
        for key in ["chunks_in", "chunks_out", "zarr_name", "coords", "data_vars"]:
            if key in self.config:
                setattr(self, key, self.config[key])
            else:
                print(f"{name}.__init__: Could not find {key} in {config_filename}, using default.")

        # warn user about not finding coords
        if self.coords is None:
            warnings.warn(f"{name}.__init__: Could not find 'coords' in {config_filename}, will not store coordinate data")

        if self.data_vars is None:
            warnings.warn(f"{name}.__init__: Could not find 'data_vars' in {config_filename}, will store all data variables")

        ## set filenames_in, which we read from
        #self.filenames_in = mystuff["filenames_in"]
        #self.filenames_in = [self.filenames_in] if isinstance(self.filenames_in, str) else self.filenames_in


    def open_dataset(self, cycle, fsspec_kwargs=None, **kwargs):
        """For now, read a single timestep from each cycle

        Args:
            cycle (datetime.datetime): datetime object giving initial time for this DA cycle
            fsspec_kwargs (dict, optional): optional arguments passed to :func:`fsspec.open_files`
            **kwargs (dict, optional): optional arguments passed to :func:`xarray.open_mfdataset`

        Returns:
            xds (xarray.Dataset): with output from a single model component (e.g., FV3)
                and a single forecast window, with all output times in
                that window merged into one dataset
        """

        kw = self.default_open_dataset_kwargs.copy()
        kw.update(kwargs)

        fnames = self.path_in(cycle, self.forecast_hours)

        fskw = fsspec_kwargs if fsspec_kwargs is not None else {}
        with fsspec.open_files(fnames, **fskw) as f:
            xds = xr.open_mfdataset(f, **kw)

        # Date is not supplied explicitly right now, just via more general pathargs
        xds.attrs.update({
                "cycle"                     : str(cycle),
                })

        return xds


    def chunk(self, xds):
        """Using the yaml-provided or default chunking scheme, chunk all arrays in this dataset

        Note:
            This should probably be replaced with rechunker https://rechunker.readthedocs.io/en/latest/

        Args:
            xds (xarray.Dataset): as provided by meth:`open_dataset`

        Returns:
            xds (xarray.Dataset): rechunked as specified
        """

        chunks = self.chunks_out.copy()
        for key in self.chunks_out.keys():
            if key not in xds.dims:
                chunks.pop(key)

        xds = xds.transpose(*list(chunks.keys()))
        return xds.chunk(chunks)


    def store_dataset(self, xds):
        """Open all netcdf files for this model component and at this DA window, store
        coordinates one time only, select data based on
        desired forecast hour, then store it.

        Args:
            xds (xarray.Dataset): as provided by meth:`open_dataset`
        """

        xds = xds.reset_coords()

        # need to store coordinates dataset only one time
        if not os.path.isdir(self.coords_path) and self.coords is not None:
            coords = [x for x in self.coords if x in xds]
            cds = xds[coords].set_coords(coords)
            if "member" in cds:
                cds = cds.isel(member=0).drop("member")
            self._store_coordinates(cds)

        # now data variables at this cycle
        # make various time variables as coordinates
        xds = xds.set_coords(["time", "cftime", "ftime"])
        if self.data_vars is not None:
            data_vars = [x for x in self.data_vars if x in xds]
            xds = xds[data_vars]

        self._store_data_vars(xds)


    def _store_coordinates(self, cds):
        """Store the static coordinate information to zarr

        Args:
            cds (xarray.Dataset): with only the static coordinate information
        """

        try:
            assert len(cds.data_vars) == 0
        except AssertionError:
            msg = f"UFSDataset._store_coordinates: we should not have any data variables in this dataset, but we found some."
            msg += f"\n{cds.data_vars}"
            raise AttributeError(msg)

        # these don't need to be chunked, coordinates are opened in memory
        store = NestedDirectoryStore(path=self.coords_path)
        cds.to_zarr(store, mode="w")
        print(f"Stored coordinate dataset at {self.coords_path}")


    def _store_data_vars(self, xds):
        """Store the data variables

        Args:
            xds (xarray.Dataset): the big dataset with all desired data variables, for this model component
                and at this particular DA window
        """

        xds = self.chunk(xds)

        store = NestedDirectoryStore(path=self.forecast_path)
        xds.to_zarr(store, mode="w")
        print(f"Stored dataset at {self.forecast_path}")


    @staticmethod
    def _cftime2time(cftime):
        """Convert cftime array to numpy.datetime64

        Args:
            cftime (array_like): with DatetimeJulian objects

        Returns:
            time (array_like): with numpy.datetime64 objects
        """
        time = np.array([
            np.datetime64(
                datetime(
                    int(t.dt.year),
                    int(t.dt.month),
                    int(t.dt.day),
                    int(t.dt.hour),
                    int(t.dt.minute),
                    int(t.dt.second))
                )
            for t in cftime
            ])
        return time


    @staticmethod
    def _time2cftime(time):
        """Convert numpy.datetime64 array to cftime

        Args:
            time (array_like): with numpy.datetime64 objects

        Returns:
            cftime (array_like): with DatetimeJulian objects
        """
        cftime = np.array([
            DatetimeJulian(
                int(t.dt.year),
                int(t.dt.month),
                int(t.dt.day),
                int(t.dt.hour),
                int(t.dt.minute),
                int(t.dt.second),
                has_year_zero=False)
            for t in time
            ])
        return cftime


class FV3Dataset(UFSDataset):
    zarr_name   = "fv3.zarr"
    chunks_in   = {
            "pfull"     : 5,
            "grid_yt"   : -1,
            "grid_xt"   : -1,
            }

    chunks_out  = {
            "time"      : 1,
            "pfull"     : 5,
            "grid_yt"   : 30,
            "grid_xt"   : 30,
            }

    def open_dataset(self, cycle, fsspec_kwargs, **kwargs):
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
                    "axis": "T"}
                )
        xds["ftime"] = xr.DataArray(
                time-np.datetime64(cycle),
                coords=xds["cftime"].coords,
                dims=xds["cftime"].dims,
                attrs={
                    "long_name": "forecast_time",
                    "description": f"time passed since {str(cycle)}",
                    "axis": "T"},
                )
        xds = xds.swap_dims({"cftime": "time"})

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
