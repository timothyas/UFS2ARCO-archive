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


# import numpy as np
# import pandas as pd
# import netCDF4
# import h5netcdf
# import argparse
import yaml as yl
import logging
import sys
import xarray

_logger = logging.getLogger(__name__)
logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
logging.basicConfig(level=10, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def requested_vars_xarray(yml_fp: str, data_fp: str) -> xarray.Dataset:
    """
    """
    with open(yml_fp, mode='r') as stream:
        out = yl.load(stream, Loader=yl.SafeLoader)
        user_requested_vars = out['requested_variables']

    subset_data = xarray.open_dataset(data_fp)[user_requested_vars]
    return subset_data


def main(args):
    """
    Wrapper allowing :func:`requested_vars_xarray` to be called with string arguments in a CLI fashion
    """
    _logger.debug("Starting Script...")

    # Example execution:
    # python3 skeleton.py /home/leldridge/sandbox/s3_source_amsua_first_pass.yaml /home/leldridge/sandbox/bfg_1994010100_fhr03_control
    output = requested_vars_xarray(args[0], args[1])
    print(output)
    _logger.info("Script ends here")


if __name__ == "__main__":
    # ^  This is a guard statement that will prevent the following code from
    #    being executed in the case someone imports this file instead of
    #    executing it as a script.
    #    https://docs.python.org/3/library/__main__.html

    # After installing your project with pip, users can also run your Python
    # modules as scripts via the ``-m`` flag, as defined in PEP 338::
    #
    #     python -m UFS2ARCO.skeleton 42
    main(sys.argv[1:])
