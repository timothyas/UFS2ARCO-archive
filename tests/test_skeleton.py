"""
This is a set of tests to exercise the skeleton.py program
"""

from UFS2ARCO import skeleton


def test_test():
    """dummy test to be sure tests are running"""
    assert 1 == 1


def test_skeleton_bad_param_count():
    """
    This will exectute the main function in the UFS2ARCO skeleton.py file
    This test passes the incorrect number of parameters to the function
    """
    arg1 = "../../test_files/s3_source_amsua_first_pass.yaml"

    retval = skeleton.main([arg1])
    assert retval == 1  # expected fail


def test_skeleton_main_yaml_file_missing():
    """
    This will exectute the main function in the UFS2ARCO skeleton.py file
    This passes a yaml filename that is known not to exist
    """
    arg1 = "../../test_files/bad_yaml_filename.yaml"
    arg2 = "S:/NOAA Ecosystem Project/UFS2ARCO/bfg_1994010100_fhr03_control"

    retval = skeleton.main([arg1, arg2])
    assert retval == 1  # expected fail


def test_skeleton_main_data_file_missing():
    """
    This will exectute the main function in the UFS2ARCO skeleton.py file
    This passes a data file name that is known not to exist
    """
    arg1 = "../../test_files/s3_source_amsua_first_pass.yaml"
    arg2 = "S:/NOAA Ecosystem Project/UFS2ARCO/bfg_1994010100_fhr03_control"

    retval = skeleton.main([arg1, arg2])
    assert retval == 1  # expected fail


def test_skeleton_main_success():
    """
    This will exectute the main function in the UFS2ARCO skeleton.py file
    This passes a data file name that is known not to exist
    """
    arg1 = "../../test_files/s3_source_amsua_first_pass.yaml"
    arg2 = "S:/NOAA Ecosystem Project/UFS2ARCO/bfg_1994010100_fhr03_control"

    retval = skeleton.main([arg1, arg2])
    assert retval == 0
