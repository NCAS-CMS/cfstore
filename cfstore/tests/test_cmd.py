import unittest

from click.testing import CliRunner
import os
from cfstore.cfdb import cli
from cfstore.config import CFSconfig
from cfstore.tests.test_basic import _dummy


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
            self.c.add_location('X','Y')
        with self.assertRaises(ValueError) as context:
            self.c.add_location('RemotePosix', 'fred', agws='gws', user='user', host='host')
        self.c.add_location('RemotePosix', 'fred', gws='gws', user='user', host='host')
        self.c.save()


class TestClick(unittest.TestCase):
    """
    Test the command line interface
    """

    def _mysetup(self, runner):
        """
        setup database context, we want to do that within the click runner context, hence not using default setUp.
        """
        config = CFSconfig('tmp.ini')
        os.environ['CFS_CONFIG_FILE'] = 'tmp.ini'
        _dummy(config.db)

    def tearDown(self):
        os.unsetenv('CFS_CONFIG_FILE')


    def _check(self, result, linecount=None):
        """
        Make sure we have a proper result, and optionally break into lines and check there is an expected number
        of output lines.
        """
        if result.exit_code != 0:
            raise result.exception
        if linecount:
            lines = result.output.split('\n')[:-1]
            if len(lines) != linecount:
                print(lines)
            self.assertEqual(linecount, len(lines))
            return lines

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
            self._mysetup(runner)
            result = runner.invoke(cli, option3)
            self._check(result, 6)
        runner = CliRunner()
        # we have two bites of this cherry because of some subtle thing to do with
        # database flushing which only arises in tests (AFIK) ...
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, option2)
            self._check(result, 10)
            result = runner.invoke(cli, option3)
            self._check(result, 10)
            result = runner.invoke(cli, option1)
            self._check(result, 6)

    def test_findf_in_collection(self):
        """
        test command line "findf" method
        this test expects to find one file, with file name starting with file1
        command is
           findf file1 --collection=dummy3
        check also works once that collection context is established without specification
           findf file1
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, ['findf', 'file1', '--collection=dummy3'])
            lines = self._check(result, 1)
            self.assertTrue(lines[0].find('file1') != -1)
            # now do it again and make sure we are looking at the default
            result = runner.invoke(cli, ['findf', 'file1'])
            lines = self._check(result, 1)

    def test_findf_across_collections(self):
        """
        test command line "find" method using
             findf file2 --collection=all
        this test expects to find five files, one from each dummy collection.
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, ['findf', 'file2', '--collection=all'])
            lines = self._check(result, 5)

    def test_organise_new(self):
        """
        Test the method of organising files into a new collections, using data from stdin
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            dummy_input = '/somewhere/in/unix_land/file12\n/somewhere/in/unix_land/file23\n'
            result = runner.invoke(cli, ['organise', 'newc'], input=dummy_input)
            self._check(result)
            result = runner.invoke(cli, ['ls','--collection=newc'])
            self._check(result, 2)
            self.assertEqual(dummy_input, result.output)

    def test_organise_existing(self):
        """
        Test the method of organising files into an existing collections, using data from stdin
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            dummy_input = '/somewhere/in/unix_land/file12\n/somewhere/in/unix_land/file13\n'
            result = runner.invoke(cli, ['organise', 'dummy4'], input=dummy_input)
            self._check(result)
            result = runner.invoke(cli, ['ls', '--collection=dummy4'])
            self._check(result, 12)

    def test_tag(self):
        """
        Test we can tag a collection, and then retrieve that collection via its tag
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, ['tag', 'dummy3', 'interesting'])
            self._check(result)
            result = runner.invoke(cli, ['findc', '--tagname=interesting'])
            lines = self._check(result, 1)
            self.assertEqual('dummy3', lines[0])

    def test_facet(self):
        """
        Test we can add and remove facets from a collection
        :return:
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, ['facet', 'color', 'green', '--collection=dummy1'])
            self._check(result)
            result = runner.invoke(cli, ['findc', '--facet', 'color', 'green'])
            lines = self._check(result, 1)
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
            self._mysetup(runner)
            result = runner.invoke(cli, ['findc', '--match=my3'])
            lines = self._check(result,1)
            self.assertEqual('dummy3',lines[0])

    def test_linkto(self):
        """ test asymmetric linking and findr"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, ['linkto', 'dummy1', 'brother', 'dummy2'])
            result = runner.invoke(cli, ['findr', 'brother', '--collection=dummy1'])
            lines = self._check(result, 1)
            self.assertEqual('dummy2', lines[0])
            result = runner.invoke(cli, ['findr', 'brother', '--collection=dummy2'])
            lines = self._check(result, 0)


    def test_linkbetween(self):
        """ test symmetric linking and findr"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, ['linkbetween', 'dummy1', 'brother', 'dummy2'])
            result = runner.invoke(cli, ['findr', 'brother', '--collection=dummy1'])
            lines = self._check(result, 1)
            self.assertEqual('dummy2', lines[0])
            result = runner.invoke(cli, ['findr', 'brother', '--collection=dummy2'])
            lines = self._check(result, 1)
            self.assertEqual('dummy1', lines[0])


if __name__ == "__main__":
    unittest.main()
