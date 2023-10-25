# require packages for this file
# python -m pip install fsspec pyyaml numpy xarray zarr cftime

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


class UFSDataset:
    """Open and store a UFS generated NetCDF dataset to zarr from a single model component (FV3, MOM6, CICE6)
    and a single DA window. The two main methods that are useful are :meth:`open_dataset` and :meth:`store_dataset`.

    Note:
        This class does nothing on its own, only the children (for now just :class:`FV3Dataset`) will work.

    Note:
        The ``path_in`` argument on __init__ probably needs some attention, especially in relation to the ``file_prefixes`` option. This should be addressed once we start thinking about datasets other than replay.

    Required Fields in Config:
        path_out (str): the outermost directory to store the dataset
        forecast_hours (list of int): with the forecast hours to save
        file_prefixes (list of str): with the filename prefixes to read, e.g. ["sfg", "bfg"] to read all files starting with this prefix at this cycle, for all forecast_hours

    Optional Fields in Config:
        coords (list of str): containing static coordinate variables to store only one time
        coords_path_out (str): path to store coordinates, if not provided then store under 'coordinates' directory in data_path
        data_vars (list of str): containing variables that evolve in time to be stored if not provided, all variables will be stored
        chunks_in, chunks_out (dict): containing chunksizes for each dimension

    Args:
        path_in (callable): map the following arguments to a path (str):
        config_filename (str): to yaml file containing the overall configuration
        is_nested (bool, optional): if True write a :class:`NestedDirectoryStore`, otherwise write Zarr's default directory structure

    Sets Attributes:
        path_out (str): the outermost directory to store the dataset
        forecast_hours (list of int): with the forecast hours to save
        file_prefixes (list of str): with the filename prefixes inside of each cycle's directory,
        coords, data_vars (list): with variable names of coordinates and data variables
        chunks_in, chunks_out (dict): specifying how to chunk the data when reading and writing
        config (dict): with the configuration provided by the file
        is_nested (bool): whether or not to write with :class:`NestedDirectoryStore`
    """

    path_out = ""
    forecast_hours = None
    file_prefixes = None

    chunks_in = None
    chunks_out = None
    coords = None
    data_vars = None
    zarr_name = None

    is_nested = None

    @property
    def data_path(self):
        """Where to write forecast data variables to"""
        return join(self.path_out, self.zarr_name)

    @property
    def coords_path(self):
        """Where to write static coordinates to"""
        if self.coords_path_out is None:
            return join(self.path_out, "coordinates", self.zarr_name)
        else:
            return join(self.coords_path_out, self.zarr_name)

    @property
    def default_open_dataset_kwargs(self):
        kw = {
            "parallel": True,
            "chunks": self.chunks_in,
            "decode_times": True,
            "preprocess": self._preprocess,
        }
        return kw

    def __init__(self, path_in, config_filename, is_nested=False):
        super(UFSDataset, self).__init__()
        name = self.__class__.__name__  # e.g., FV3Dataset, MOMDataset

        self.path_in = path_in
        self.is_nested = is_nested
        with open(config_filename, "r") as f:
            contents = yaml.safe_load(f)
            self.config = contents[name]

        # look for these requited inputs
        for key in ["path_out", "forecast_hours", "file_prefixes"]:
            try:
                setattr(self, key, self.config[key])
            except KeyError:
                raise KeyError(f"{name}.__init__: Could not find {key} in {config_filename}, but this is required")

        # look for these optional inputs
        for key in ["chunks_in", "chunks_out", "coords", "data_vars", "coords_path_out"]:
            if key in self.config:
                setattr(self, key, self.config[key])
            else:
                print(f"{name}.__init__: Could not find {key} in {config_filename}, using default.")

        # warn user about not finding coords
        if self.coords is None:
            warnings.warn(
                f"{name}.__init__: Could not find 'coords' in {config_filename}, will not store coordinate data"
            )

        if self.data_vars is None:
            warnings.warn(
                f"{name}.__init__: Could not find 'data_vars' in {config_filename}, will store all data variables"
            )

        # check that file_prefixes is a list
        self.file_prefixes = [self.file_prefixes] if isinstance(self.file_prefixes, str) else self.file_prefixes

    def open_dataset(self, cycle, fsspec_kwargs=None, **kwargs):
        """Read data from a single DA cycle

        Args:
            cycle (datetime.datetime): datetime object giving initial time for this DA cycle
            fsspec_kwargs (dict, optional): optional arguments passed to :func:`fsspec.open_files`
            **kwargs (dict, optional): optional arguments passed to :func:`xarray.open_mfdataset`, in addition to
                the ones provided by :attr:`default_open_dataset_kwargs`

        Returns:
            xds (xarray.Dataset): with output from a single model component (e.g., FV3)
                and a single forecast window, with all output times in
                that window merged into one dataset
        """

        kw = self.default_open_dataset_kwargs.copy()
        kw.update(kwargs)

        fnames = self.path_in(cycle, self.forecast_hours, self.file_prefixes)

        # Maybe there's a more elegant way to handle this, but with local files, fsspec closes them
        # before dask reads them...
        if fsspec_kwargs is None:
            xds = xr.open_mfdataset(fnames, **kw)
        else:
            with fsspec.open_files(fnames, **fsspec_kwargs) as f:
                xds = xr.open_mfdataset(f, **kw)
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

    def store_dataset(self, xds, **kwargs):
        """Open all netcdf files for this model component and at this DA window, store
        coordinates one time only, select data based on
        desired forecast hour, then store it.

        Args:
            xds (xarray.Dataset): as provided by :meth:`open_dataset`
            kwargs (dict): optional arguments passed to :func:`xarray.to_zarr` via :meth:`_store_data_vars`
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

        self._store_data_vars(xds, **kwargs)

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
        store = NestedDirectoryStore(path=self.coords_path) if self.is_nested else self.coords_path
        cds.to_zarr(store, mode="w")
        print(f"Stored coordinate dataset at {self.coords_path}")

    def _store_data_vars(self, xds, **kwargs):
        """Store the data variables

        Args:
            xds (xarray.Dataset): the big dataset with all desired data variables, for this model component
                and at this particular DA window
        """

        xds = self.chunk(xds)

        store = NestedDirectoryStore(path=self.data_path) if self.is_nested else self.data_path
        xds.to_zarr(store, **kwargs)
        print(f"Stored dataset at {self.data_path}")

    @staticmethod
    def _preprocess(xds):
        """Used to remove a redundant surface pressure found in both physics and dynamics FV3 files,
        which are slightly different and so cause a conflict. This method is not used on its own, but
        given as an option to :func:`xarray.open_mfdataset`

        Args:
            xds (xarray.Dataset): A single netcdf file from the background forecast

        Returns:
            xds (xarray.Dataset): with ``pressfc`` variable removed if this is from FV3 dynamics output
        """
        # We can't rely on xds.encoding when reading from s3, so have to infer if this is dynamics
        # vs physics dataset by the other fields that exist in the dataset
        dyn_vars = ["tmp", "ugrd", "vgrd", "spfh", "o3mr"]
        if "pressfc" in xds.data_vars and any(v in xds.data_vars for v in dyn_vars):
            del xds["pressfc"]
        return xds

    @staticmethod
    def _cftime2time(cftime):
        """Convert cftime array to numpy.datetime64

        Args:
            cftime (array_like): with DatetimeJulian objects

        Returns:
            time (array_like): with numpy.datetime64 objects
        """
        time = np.array(
            [
                np.datetime64(
                    datetime(
                        int(t.dt.year),
                        int(t.dt.month),
                        int(t.dt.day),
                        int(t.dt.hour),
                        int(t.dt.minute),
                        int(t.dt.second),
                    )
                )
                for t in cftime
            ]
        )
        return time

    @staticmethod
    def _time2cftime(time):
        """Convert numpy.datetime64 array to cftime

        Args:
            time (array_like): with numpy.datetime64 objects

        Returns:
            cftime (array_like): with DatetimeJulian objects
        """
        cftime = np.array(
            [
                DatetimeJulian(
                    int(t.dt.year),
                    int(t.dt.month),
                    int(t.dt.day),
                    int(t.dt.hour),
                    int(t.dt.minute),
                    int(t.dt.second),
                    has_year_zero=False,
                )
                for t in time
            ]
        )
        return cftime


class FV3Dataset(UFSDataset):
    zarr_name = "fv3.zarr"
    chunks_in = {
        "pfull": 5,
        "grid_yt": -1,
        "grid_xt": -1,
    }

    chunks_out = {
        "time": 1,
        "pfull": 5,
        "grid_yt": 30,
        "grid_xt": 30,
    }

    def open_dataset(self, cycle, fsspec_kwargs=None, **kwargs):
        xds = super().open_dataset(cycle, fsspec_kwargs, **kwargs)

        # Deal with time
        xds = xds.rename({"time": "cftime"})
        time = self._cftime2time(xds["cftime"])
        xds["time"] = xr.DataArray(
            time, coords=xds["cftime"].coords, dims=xds["cftime"].dims, attrs={"long_name": "time", "axis": "T"}
        )
        xds["ftime"] = xr.DataArray(
            time - np.datetime64(cycle),
            coords=xds["cftime"].coords,
            dims=xds["cftime"].dims,
            attrs={"long_name": "forecast_time", "description": f"time passed since {str(cycle)}", "axis": "T"},
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
