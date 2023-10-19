"""
This skeleton file serves as a starting point for a Python console script.

Run ``pip install .`` (or ``pip install -e .`` for editable mode) to install
the UFS2ARCO package.
This will install the command ``skeleton.main`` inside your current environment.

    ---- Python API ----

The functions defined in this section can be imported by users in their
Python scripts/interactive interpreter, e.g. via ``from UFS2ARCO import skeleton``
when using this Python module as a library.

"""

# pip install xarray netcdf4 h5netcdf pyyaml

# import numpy as np
# import pandas as pd
# import netCDF4
# import h5netcdf
# import argparse
import pathlib
import logging
import sys
import inspect
import yaml
import xarray

_logger = logging.getLogger(__name__)
handler = logging.FileHandler(f"{__file__}.log")
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:: %(message)s")
handler.setFormatter(formatter)
_logger.addHandler(handler)

_logger.setLevel(logging.DEBUG)


def requested_vars_xarray(yml_fn: str, data_fn: str) -> xarray.Dataset:
    """_summary_

    Args:
        yml_fn (str): The YAML configuration file
        data_fn (str): The data file to be extracted from

    Returns:
        xarray.Dataset: The subset of `data_fn` as desribed by `yml_fn`
    """
    _logger.debug(f"Opening file: {yml_fn}")
    with open(yml_fn, mode="r", encoding="UTF-8") as stream:
        out = yaml.load(stream, Loader=yaml.SafeLoader)
        user_requested_vars = out["requested_variables"]

    _logger.debug("subsetting data")
    subset_data = xarray.open_dataset(data_fn)[user_requested_vars]
    _logger.debug(f"returning from {inspect.currentframe().f_code.co_name}")
    return subset_data


def main(yaml_file: str, data_file: str) -> int:
    """
    Wrapper allowing :func:`requested_vars_xarray` to be called with string
    arguments in a CLI fashion

    Args:
        yaml_file (str): This string is the YAML configuration filename
        data_file (str): This string is the data filename

    Returns:
        int: An integer = 0 if successful, otherwise 1
    """
    _logger.debug(f"main called with parameters yaml_file: {yaml_file}, and data_file: {data_file}")

    # /home/leldridge/sandbox/bfg_1994010100_fhr03_control
    if not pathlib.Path(yaml_file).is_file():
        _logger.error(f"The file {yaml_file} does not exist.")
        return 1

    if not pathlib.Path(data_file).is_file():
        _logger.error(f"The file {data_file} does not exist.")
        return 1

    output = requested_vars_xarray(yaml_file, data_file)
    print(output)
    _logger.debug("successful call")

    return 0


if __name__ == "__main__":
    # ^  This is a guard statement that will prevent the following code from
    #    being executed in the case someone imports this file instead of
    #    executing it as a script.
    #    https://docs.python.org/3/library/__main__.html

    # Example execution:
    #   python -m UFS2ARCO.skeleton \
    #      /home/leldridge/sandbox/s3_source_amsua_first_pass.yaml \
    #      /home/leldridge/sandbox/bfg_1994010100_fhr03_control
    # or
    #   python -m UFS2ARCO.skeleton \
    #      test_files/s3_source_amsua_first_pass.yaml \
    #     'S:/NOAA Ecosystem Project/UFS2ARCO/bfg_1994010100_fhr03_control'

    if len(sys.argv) != 3:
        _logger.error(f"skeleton.py called with the incorrect number of parameters: Should be 2, was {len(sys.argv)-1}")
        sys.exit()

    _logger.debug(f"__main__ called with parameters {sys.argv[1:2][0]} and {sys.argv[2:3][0]}")
    main(sys.argv[1:2][0], sys.argv[2:3][0])
    _logger.debug("__main__ exiting")
