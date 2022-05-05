import unittest, os, sys
from unittest import mock

def notty(x):
    """ Used by mock"""
    return False

def function_to_be_tested():
    ostty = os.isatty(0) 
    assert ostty is False, f"ostty: {ostty}"

class TestTestingInfrastructure(unittest.TestCase):
    """ 
    Want to be able to test interaction of
    unittesting environment with sys.stdin and
    os.isatty/ sys.stdin.isatty
    """

    with mock.patch('os.isatty', notty) as mock_tty:
       function_to_be_tested()


if __name__=="__main__":
    unittest.main()