import pytest

from USF2ARCO.skeleton import main


def test_fib():
    """API Tests"""
    # assert fib(1) == 1
    # assert fib(2) == 1
    # assert fib(7) == 13
    # with pytest.raises(AssertionError):
    #     fib(-10)


def test_local_file():
    """_summary_
    """
    arg1 = "../../test_files/s3_source_amsua_first_pass.yaml"
    arg2 = 'S:/NOAA Ecosystem Project/UFS2ARCO/bfg_1994010100_fhr03_control'

    main([arg1, arg2])
    assert("The skeleton was called and returned")


# def test_main(capsys):
#     """CLI Tests"""
#     # capsys is a pytest fixture that allows asserts against stdout/stderr
#     # https://docs.pytest.org/en/stable/capture.html
#     main(["7"])
#     captured = capsys.readouterr()
#     assert "The 7-th Fibonacci number is 13" in captured.out
