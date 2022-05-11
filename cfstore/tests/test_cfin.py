import unittest, os

from unittest import mock

from cfstore.cfdb import cli
from cfstore.cfin import cli as incli
from test_cmd import _check, _mysetup, MissingTestEnvVar
from test_basic import _dummy
from cfstore.plugins.ssh import SSHlite


from click.testing import CliRunner

@mock.patch.dict(os.environ, {
    'TEST_RP_HOST': 'xfer1',
    'TEST_RP_PATH': 'hiresgw/cftest',
    'TEST_RP_USER': 'lawrence',
    'TEST_RP_EXPECTED_DIR': 'subdir'
})

def setup_ssh():
    rhost = os.getenv('TEST_RP_HOST', default='NONE')
    rpath = os.getenv('TEST_RP_PATH', default='NONE')
    ruser = os.getenv('TEST_RP_USER', default='NONE')
    expected = os.getenv('TEST_RP_EXPECTED_DIR', default='NONE')
    if rhost == 'NONE' or rpath == 'NONE' or ruser == 'NONE' or expected == 'NONE':
        raise MissingTestEnvVar(
            f'RemotePosix test requires TEST_RP_HOST, TEST_RP_PATH, TEST_RP_USER, TEST_RP_EXPECTED_DIR env variables')
    else:
        s = SSHlite(rhost, ruser)
        assert s.isalive(), 'SSH test configuration does not work'
    return rhost, rpath, ruser, expected

class Test_ssh(unittest.TestCase):

    def test_ssh(self):
        """
        test the remote path includes an expected subdirectory
        """
        rhost, rpath, ruser, expected = setup_ssh()
        s = SSHlite(rhost, ruser)
        dlist = s.globish(rpath, '*')
        assert expected in dlist


class Test_cfin(unittest.TestCase):
    """
    Test the cfin command line interface
    """
    
    def test_create_location1(self):
        """ Test creating location which we know doesn't exist """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(incli, ['rp', 'setup', 'a_location', 'host_does_not_exist', 'user'])

    def test_create_location2(self):
        """ Test creating location which we know does exist """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(incli, ['rp', 'setup', 'location1', 'host_does_not_exist','user'])
            # this should raise a ValueError given we already have this location
            # it looks like it does, but somehow the test environment is removing it ...
            result = runner.invoke(incli, ['rp', 'setup', 'location1', 'host_does_not_exist', 'user'])
            assert result.exit_code == 1
            assert str(result.exception).find('already exists in') != -1

    def test_add_remote_posix(self):
        """
        This test requires you to have an ssh host and location set in environment variables.
        They are mocked here in setup_ssh, you might need to do the same.
        You also need to sort out what is in the test directory.
        """
        rhost, rpath, ruser, expected = setup_ssh()
        runner = CliRunner()
        with runner.isolated_filesystem():
            dfile = _mysetup()
            result = runner.invoke(incli, ['rp', 'setup', 'testloc', rhost, ruser])
            result = runner.invoke(incli, ['rp', 'add', 'testloc', rpath, 'test_collection', f'--description={dfile}'])
            result = runner.invoke(cli, ['ls', '--collection=test_collection'] )
            # there are supposed to be three files in the test collection
            _check(self, result, 3)


if __name__=="__main__":
    unittest.main()