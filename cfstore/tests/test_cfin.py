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

    def test_add_local_posix(self):
        """
        This test requires you to sort out what is in the test directory.
        """
        lpath = "G:\cfstore\cfstore\cfstore\plugins"
        #This path has 7 files and a folder containing 5 more files
        #3 files in each of the main and subfolder start with "et"
        runner = CliRunner()
        with runner.isolated_filesystem():
            dfile = _mysetup()
            result = runner.invoke(incli, ['local', 'add', 'testloc', lpath, f'--description={dfile}','subcollections=False'])
            result = runner.invoke(cli, ['ls', '--collection=testloc'] )
            _check(self, result, 8)

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
            result = runner.invoke(incli, ['rp', 'setup', 'testrem', rhost, ruser])
            result = runner.invoke(incli, ['rp', 'add', 'testrem', rpath, 'testrem_collection', f'--description={dfile}'])
            result = runner.invoke(cli, ['ls', '--collection=testrem_collection'] )
            # there are supposed to be three files in the test collection
            _check(self, result, 3)

    def test_add_local_posix_subcollections(self):
        """
        This test requires you to sort out what is in the test directory.
        """
        lpath = "G:\cfstore\cfstore\cfstore\plugins"
        #This path has 7 files and a folder containing 5 more files
        #3 files in each of the main and subfolder start with "et"
        runner = CliRunner()
        with runner.isolated_filesystem():
            dfile = _mysetup()
            result = runner.invoke(incli, ['local', 'add', 'testlocsc', lpath, f'--description={dfile}','--subcollections=True'])
            result = runner.invoke(cli, ['ls', '--collection=testlocsc'] )
            _check(self, result, 13)

    def test_add_local_posix_regex(self):
        """
        This test requires you to sort out what is in the test directory.
        """
        lpath = "G:\cfstore\cfstore\cfstore\plugins"
        #This path has 7 files and a folder containing 5 more files
        #3 files in each of the main and subfolder start with "et"
        runner = CliRunner()
        with runner.isolated_filesystem():
            dfile = _mysetup()
            result = runner.invoke(incli, ['local', 'add', 'testlocrx', lpath, f'--description={dfile}', '--regexselect=^et'])
            result = runner.invoke(cli, ['ls', '--collection=testlocrx'] )
            _check(self, result, 4)

    def test_add_local_posix_all_optional(self):
        """
        This test requires you to sort out what is in the test directory.
        """
        lpath = "G:\cfstore\cfstore\cfstore\plugins"
        #This path has 7 files and a folder containing 5 more files
        #3 files in each of the main and subfolder start with "et"
        runner = CliRunner()
        with runner.isolated_filesystem():
            dfile = _mysetup()
            result = runner.invoke(incli, ['local', 'add', 'testlocall', lpath, f'--description={dfile}','--subcollections=True', '--regexselect=^et'])
            result = runner.invoke(cli, ['ls', '--collection=testlocall'] )
            _check(self, result, 6)

    def test_add_remote_posix_subcollections(self):
        pass

    def test_add_remote_posix_regex(self):
        pass

    def test_add_remote_posix_all_optional(self):
        pass

if __name__=="__main__":
    unittest.main()