

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


class Test_b_metadata_remote(unittest.TestCase):
    """
    Test the b metadata gathering functions
    """

    def test_run_script_offline(self):
        """
        Runs the script offline on the "scripts" folder itself
        If you want to test a .nc file (reccommended) then put one in there
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['python cfstore/scripts/getallbmetadata.py'] )
            _check(self, result, 3)

    def test_script_output(self):
        """
        Checks that the script makes a json file - doesn't test for emptiness or anything else
        #TODO This is a very minimal test that could do with some more things
        """
        assert os.path.exists("cfstore/scripts/bmetadata.json")

    def run_bmetadata_cmd(self):
        """
        Runs the bmetadata command line   
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['rp','getallbmetadata', 'host_does_not_exist','remote_location','a_location', 'a_location', 'script'])
            #cfin rp getbmetadata xfer1 /home/users/gobncas/ xjanpcmip3 /home/george/Documents/cfs/cfstore/cfstore/scripts/getallbmetadata.py
            #for now, as long as we reach the end we're happy
            self.assertEqual(1,1)
            
if __name__=="__main__":
    unittest.main()