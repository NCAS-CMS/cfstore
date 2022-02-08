import unittest, os
from unittest import mock
from click.testing import CliRunner

from cfstore.cfdb import cli
from cfstore.cfin import cli as incli
from cfstore.config import CFSconfig
from cfstore.tests.test_basic import _dummy
from cfstore.plugins.ssh import SSHlite

def _mysetup():
    """
    Setup database context on the isolated file systems within the click runner context
    """
    config = CFSconfig('tmp.ini')
    os.environ['CFS_CONFIG_FILE'] = 'tmp.ini'
    _dummy(config.db)
    filename = 'description_eg.txt'
    with open(filename, 'w') as f:
        f.write('dummy description text')
    return filename


def _check(instance, result, linecount=None, noisy=False):
    """
    Make sure we have a proper result, and optionally break into lines and check there is an expected number
    of output lines.
    """
    if result.exit_code != 0:
        raise result.exception
    if noisy:
        print(result.output)
    if linecount:
        lines = result.output.split('\n')[:-1]
        if len(lines) != linecount:
            print(lines)
        instance.assertEqual(linecount, len(lines))
        return lines
    return []

def notty(x):
    """ Used for mocking os.isatty so that tests always run non-interactively
    no matter how run"""
    return False

class MissingTestEnvVar(Exception):
    pass


class TestNoConfig(unittest.TestCase):
    """ Test handling situation where there is no config file gracefully"""

    def setUp(self):
        # not sure we really need to worry about pre-existing, but just in case:
        self.original_config = os.getenv('CFS_CONFIG_FILE')
        os.unsetenv('CFS_CONFIG_FILE')
        # now keep this from the real file system
        # TODO: See https://github.com/ncas-cms/cfstore/issues/16

    def tearDown(self):
        if self.original_config:
            os.environ['CFS_CONFIG_FILE'] = self.original_config

    def test_no_config_file(self):
        " Test absence of a configuration file. May yet fail, see issue 16 "
        runner = CliRunner()
        with runner.isolated_filesystem():
            r = runner.invoke(cli, ['ls',])
            assert (r.exit_code == 0)


class TestConfig(unittest.TestCase):
    """
    Test raw configuration file
    """

    def setUp(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            self.c = CFSconfig('tmp.ini')

    def test_add_location(self):
        """ Test location adding works as expected at the config file level"""
        with self.assertRaises(ValueError) as context:
            self.c.add_location('X', 'Y')
        with self.assertRaises(ValueError) as context:
            self.c.add_location('rp', 'fred', user='loki', host='host')
            self.c.add_location('rp', 'fred', user='loki', host='host')
        self.c.save()


class Test_cfdb(unittest.TestCase):
    """
    Test the cfdb command line interface
    """

    def tearDown(self):
        os.unsetenv('CFS_CONFIG_FILE')

    def test_ls(self):
        """
        Test a variety of ls options:
            ls --collection=all
            ls (no collection and no default)
            ls --collection=dummy2
            ls (with previous default set)
        """
        option1, option2, option3 = (
            ['ls', '--collection=all'],
            ['ls', '--collection=dummy2'],
            ['ls',]
        )
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, option3)
            _check(self, result, 6)
        runner = CliRunner()
        # we have two bites of this cherry because of some subtle thing to do with
        # database flushing which only arises in tests (AFIK) ...
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, option2)
            _check(self, result, 10)
            result = runner.invoke(cli, option3)
            _check(self, result, 10)
            result = runner.invoke(cli, option1)
            _check(self, result, 6)

    def test_delete_col(self):
        """
        Test removal of an empty collection (and test raising an error for
        an attempt to raise a collection with contents).
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            # first add an empty collection to kill
            config = CFSconfig('tmp.ini')
            config.db.create_collection('for_the_chop', 'no description')
            # now, off with it's head
            result = runner.invoke(cli, ['delete-col', 'for_the_chop'])
            lines = _check(self, result, 0)
            result = runner.invoke(cli, ['delete-col', 'dummy1'])
            assert 'Collection dummy1 not empty' in str(result.exception)

    def test_findf_in_collection(self):
        """
        test command line "findf" method
        this test expects to find one file, with file name startgit ing with file1
        command is
           findf file1 --collection=dummy3
        check also works once that collection context is established without specification
           findf file1
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['findf', 'file1', '--collection=dummy3'])
            lines = _check(self, result, 1)
            self.assertTrue(lines[0].find('file1') != -1)
            # now do it again and make sure we are looking at the default
            result = runner.invoke(cli, ['findf', 'file1'])
            lines = _check(self, result, 1)

    def test_findf_across_collections(self):
        """
        test command line "find" method using
             findf file2 --collection=all
        this test expects to find five files, one from each dummy collection.
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['findf', 'file2', '--collection=all'])
            lines = _check(self, result, 5)

    def test_organise_new(self):
        """
        Test the method of organising files into a new collections, using data from stdin
        """
        runner = CliRunner()

        with mock.patch('os.isatty', notty) as mock_tty:
            with runner.isolated_filesystem():
                _mysetup()
                dummy_input = '/somewhere/in/unix_land/file12\n/somewhere/in/unix_land/file23\n'
                result = runner.invoke(cli, ['organise', 'newc'], input=dummy_input)
                _check(self, result, noisy=True)
                result = runner.invoke(cli, ['ls', '--collection=newc'])
                _check(self, result, 2)
                self.assertEqual(dummy_input, result.output)

    def test_organise_existing(self):
        """
        Test the method of organising files into an existing collections, using data from stdin
        """
        runner = CliRunner()
        with mock.patch('os.isatty', notty) as mock_tty:
            with runner.isolated_filesystem():
                _mysetup()
                dummy_input = '/somewhere/in/unix_land/file12\n/somewhere/in/unix_land/file13\n'
                result = runner.invoke(cli, ['organise', 'dummy4'], input=dummy_input)
                _check(self, result)
                result = runner.invoke(cli, ['ls', '--collection=dummy4'])
                _check(self, result, 12)

    def test_tag(self):
        """
        Test we can tag a collection, and then retrieve that collection via its tag
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['tag', 'dummy3', 'interesting'])
            _check(self, result)
            result = runner.invoke(cli, ['findc', '--tagname=interesting'])
            lines = _check(self, result, 1)
            self.assertEqual('dummy3', lines[0])

    def test_facet(self):
        """
        Test we can add and remove facets from a collection
        :return:
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['facet', 'color', 'green', '--collection=dummy1'])
            _check(self, result)
            result = runner.invoke(cli, ['findc', '--facet', 'color', 'green'])
            lines = _check(self, result, 1)
            self.assertEqual('dummy1', lines[0])

    def test_findc(self):
        """
        Test matching on content in name or description
            findc --match=abc
        These two following options are tested in tag and facet tests:
            findc --tag=def
            findc --facet=(key,value)
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['findc', '--match=my3'])
            lines = _check(self, result,1)
            self.assertEqual('dummy3',lines[0])

    def test_findr(self):
        """
        Test command line discovery of replicants in a collection
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            config = CFSconfig('tmp.ini')
            # attempting to replicate the basic test via the command line
            # this should create 5x3=15 duplicate files in another location with different collections:
            _dummy(config.db, location='pseudo tape', collection_stem="tdummy", files_per_collection=3)
            # now we need to see if these can be found, let's just look for the two replicas in dummy1
            result = runner.invoke(cli, ['findrx','--collection=dummy1'])
            lines = _check(self, result, 3)
            assert lines[0].find('file01') != -1
            # now just make sure we can get back the right answer if we go for a match as well
            # for this we have to muck with our test dataset to get a decent test case.
            # we add a file which we know to be in collection dummy2 and a replicant
            fset = config.db.retrieve_files_in_collection('dummy2', match='22', replicants=True)
            config.db.add_file_to_collection('dummy1', fset[0])
            # now do the actual second test
            result = runner.invoke(cli, ['findrx', 'file2',])
            lines = _check(self, result, 2)

    def test_linkto(self):
        """ test asymmetric linking and findr"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['linkto', 'dummy1', 'brother', 'dummy2'])
            result = runner.invoke(cli, ['findr', 'brother', '--collection=dummy1'])
            lines = _check(self, result, 1)
            self.assertEqual('dummy2', lines[0])
            result = runner.invoke(cli, ['findr', 'brother', '--collection=dummy2'])
            lines = _check(self, result, 0)

    def test_linkbetween(self):
        """ test symmetric linking and findr"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['linkbetween', 'dummy1', 'brother', 'dummy2'])
            result = runner.invoke(cli, ['findr', 'brother', '--collection=dummy1'])
            lines = _check(self, result, 1)
            self.assertEqual('dummy2', lines[0])
            result = runner.invoke(cli, ['findr', 'brother', '--collection=dummy2'])
            lines = _check(self, result, 1)
            self.assertEqual('dummy1', lines[0])

    def test_print(self):
        """ Test we can print information about a collection to output.
        Information should include description, any tags, and
        any relationships.
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            _mysetup()
            result = runner.invoke(cli, ['pr', 'dummy1'])
            raise NotImplementedError('Still developing this test, results not checked')


if __name__ == "__main__":
    unittest.main()







