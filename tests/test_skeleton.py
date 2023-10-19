"""
This is a set of tests to exercise the skeleton.py program

KJW: To make the testing work I had to, I think, 'install' this project as an
    editable package by exectuting:
        ``python -m pip install -e .``
    from the path containing the UFS2ARCO package.  (e.g. `E:/temp/UFS2ARCO`)
"""

from pathlib import Path
from UFS2ARCO import skeleton


def test_test():
    """dummy test to be sure tests are running"""
    assert 1 == 1


def test_skeleton_main_yaml_file_missing():
    """
    This will exectute the main function in the UFS2ARCO skeleton.py file
    This passes a yaml filename that is known not to exist
    """
    arg1 = Path("test_files") / "bad_yaml_filename.yaml"
    arg2 = Path("S:/") / "NOAA Ecosystem Project" / "UFS2ARCO" / "bfg_1994010100_fhr03_control"

    retval = skeleton.main(str(arg1), str(arg2))
    assert retval == 1  # expected fail


def test_skeleton_main_data_file_missing():
    """
    This will exectute the main function in the UFS2ARCO skeleton.py file
    This passes a data file name that is known not to exist
    """
    arg1 = Path("test_files") / "s3_source_amsua_first_pass.yaml"
    arg2 = Path("S:/") / "NOAA Ecosystem Project" / "UFS2ARCO" / "BadDataFileName"

    retval = skeleton.main(str(arg1), str(arg2))
    assert retval == 1  # expected fail

# KJW: Removing this test until we can place the data file (arg2) into a
#  testable location for GitHub
#def test_skeleton_main_success():
#    """
#    This will exectute the main function in the UFS2ARCO skeleton.py file
#    This test should succeed
#    """
#    arg1 = Path("test_files") / "s3_source_amsua_first_pass.yaml"
#    arg2 = Path("S:/") / "NOAA Ecosystem Project" / "UFS2ARCO" / "bfg_1994010100_fhr03_control"
#
#    retval = skeleton.main(str(arg1), str(arg2))
#    assert retval == 0
