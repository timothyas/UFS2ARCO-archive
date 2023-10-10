"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
``[options.entry_points]`` section in ``setup.cfg``::

    console_scripts =
         fibonacci = era5_vs_gefvs13.skeleton:run

Then run ``pip install .`` (or ``pip install -e .`` for editable mode)
which will install the command ``fibonacci`` inside your current environment.

Besides console scripts, the header (i.e. until ``_logger``...) of this file can
also be used as template for Python modules.

Note:
    This file can be renamed depending on your needs or safely removed if not needed.

References:
    - https://setuptools.pypa.io/en/latest/userguide/entry_point.html
    - https://pip.pypa.io/en/stable/reference/pip_install
"""
import numpy as np
import pandas as pd
import netCDF4
import h5netcdf
import xarray as xr
import yaml as yl
import argparse
import logging
import sys

from era5_vs_gefvs13 import __version__

__author__ = "leldr"
__copyright__ = "leldr"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
user_requested_vars = {}

# ---- Python API ----
# The functions defined in this section can be imported by users in their
# Python scripts/interactive interpreter, e.g. via
# `from era5_vs_gefvs13.skeleton import fib`,
# when using this Python module as a library.


def requested_vars_xarray(filepath):
    with open(filepath, 'r') as stream:
        out = yl.load(stream, Loader=yl.SafeLoader)
        user_requested_vars = out['requested_variables']

    subset_data = data[user_requested_vars]
    return subset_data


def setup_logging():
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=10, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args):
    """Wrapper allowing :func:`fib` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    setup_logging()
    _logger.debug("Starting Script...")
    output = requested_vars_xarray("/home/leldridge/sandbox/s3_source_amsua_first_pass.yaml")
    print(output)
    _logger.info("Script ends here")


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    # ^  This is a guard statement that will prevent the following code from
    #    being executed in the case someone imports this file instead of
    #    executing it as a script.
    #    https://docs.python.org/3/library/__main__.html

    # After installing your project with pip, users can also run your Python
    # modules as scripts via the ``-m`` flag, as defined in PEP 338::
    #
    #     python -m era5_vs_gefvs13.skeleton 42
    #
    run()
