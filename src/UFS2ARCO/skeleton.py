"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
``[options.entry_points]`` section in ``setup.cfg``::

    console_scripts =
         fibonacci = UFS2ARCO.skeleton:run

Then run ``pip install .`` (or ``pip install -e .`` for editable mode)
which will install the command ``fibonacci`` inside your current environment.

Besides console scripts, the header (i.e. until ``_logger``...) of this file can
also be used as template for Python modules.

Note:
    This file can be renamed depending on your needs or safely removed if not needed.

References:
    - https://setuptools.pypa.io/en/latest/userguide/entry_point.html
    - https://pip.pypa.io/en/stable/reference/pip_install
    
    ---- Python API ----
The functions defined in this section can be imported by users in their
Python scripts/interactive interpreter, e.g. via
`from UFS2ARCO.skeleton import fib`,
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
import yaml as yl
import xarray

_logger = logging.getLogger(__name__)
log_format = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
logging.basicConfig(level=10, stream=sys.stdout, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")


def requested_vars_xarray(yml_fp: str, data_fp: str) -> xarray.Dataset:
    """_summary_

    Args:
        yml_fp (str): _description_
        data_fp (str): _description_

    Returns:
        xarray.Dataset: _description_
    """
    with open(yml_fp, mode='r', encoding="UTF-8") as stream:
        out = yl.load(stream, Loader=yl.SafeLoader)
        user_requested_vars = out['requested_variables']

    subset_data = xarray.open_dataset(data_fp)[user_requested_vars]
    return subset_data


def main(args) -> int:
    """
    Wrapper allowing :func:`requested_vars_xarray` to be called with string arguments in a CLI fashion

    Args:
        args (Array[str]): This is a two element list of strings to drive the data conversion
        The first element is the YAML configuration filename
        The second element is the data filename

    Returns:
        int: An integer = 0 if successful, otherwise 1
    """
    _logger.debug("Starting Script...")

    # Example execution:
    # python3 skeleton.py /home/leldridge/sandbox/s3_source_amsua_first_pass.yaml /home/leldridge/sandbox/bfg_1994010100_fhr03_control
    # python skeleton.py ../../test_files/s3_source_amsua_first_pass.yaml 'S:/NOAA Ecosystem Project/UFS2ARCO/bfg_1994010100_fhr03_control'

    # /home/leldridge/sandbox/bfg_1994010100_fhr03_control
    if len(args) != 2:
        _logger.error(f'main called with the incorrect number of parameters.  Should be 2, was {len(args)}')
        return 1

    yaml_file = args[0]
    if not pathlib.Path(yaml_file).is_file():
        _logger.error(f'The file {yaml_file} does not exist.')
        return 1
    
    data_file = args[1]
    if not pathlib.Path(data_file).is_file():
        _logger.error(f'The file {data_file} does not exist.')
        return 1

    output = requested_vars_xarray(yaml_file, data_file)
    print(output)
    _logger.debug("successful call.")
    return 0


if __name__ == "__main__":
    # ^  This is a guard statement that will prevent the following code from
    #    being executed in the case someone imports this file instead of
    #    executing it as a script.
    #    https://docs.python.org/3/library/__main__.html

    # After installing your project with pip, users can also run your Python
    # modules as scripts via the ``-m`` flag, as defined in PEP 338::
    #
    #     python -m UFS2ARCO.skeleton s3_source_amsua_first_pass.yaml
    main(sys.argv[1:])
