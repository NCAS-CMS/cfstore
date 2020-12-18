import unittest
from interface import CollectionDB
from click.testing import CliRunner
import os
from command_line import cli


def _dummy(db):
    """ Set up a dummy dataset in db with accessible
    structure for testing
    """
    for i in range(5):
        c = f'dummy{i}'
        db.create_collection(c,'no description', {})
        files = [(f'/somewhere/in/unix_land/file{j}{i}', 0, ) for j in range(10)]
        db.upload_files_to_collection(c, files)


class BasicStructure(unittest.TestCase):
    """
    Test basic table structure python interface
    """
    def setUp(self):
        self.db = CollectionDB()
        self.db.init('sqlite://')

    def test_create_collection(self):
        kw = {'resolution': 'N512', 'workspace': 'testws'}
        self.db.create_collection('mrun1', 'no real description', kw)
        info = self.db.collection_info('mrun1')
        print(info)

    def test_unique_collection(self):
        kw = {'resolution': 'N512', 'workspace': 'testws'}
        self.db.create_collection('mrun1', 'no real description', kw)
        with self.assertRaises(ValueError) as context:
            self.db.create_collection('mrun1', 'no real description', kw)
            self.assertTrue('Cannot add duplicate collection' in str(context))

    def test_fileupload(self):
        """ Test uploading files """
        self.db.create_collection('mrun1', 'no real description', {})
        files = [(f'/somewhere/in/unix_land/file{i}', 0, ) for i in range(10)]
        self.db.upload_files_to_collection('mrun1', files)

        self.assertEquals(len(self.db.retrieve_files_in_collection('mrun1')), len(files))

    def test_add_and_retrieve_tag(self):
        """
        Need to add tags, and select by tags
        """
        tagname = 'test_tag'
        self.db.create_tag(tagname)
        for i in range(5):
            self.db.create_collection(f'mrun{i}', 'no real description', {})
        self.db.tag_collection('mrun1', tagname)
        self.db.tag_collection('mrun3', tagname)
        tagged = self.db.retrieve_collections(tagname=tagname)
        assert ['mrun1', 'mrun3'] == [x.name for x in tagged]


    def test_get_collections(self):
        """
        Test ability to get a subset of collections via name and/or description
        """
        for i in range(5):
            self.db.create_collection(f'dummy{i}','no description', {})
            self.db.create_collection(f'eg{i}','no description', {})
        self.db.create_collection('dummy11','real description',{})
        self.assertEquals(len(self.db.retrieve_collections()), 11)
        self.assertEquals(len(self.db.retrieve_collections(name_contains='g')), 5)
        self.assertEqual(len(self.db.retrieve_collections(description_contains='real')), 1)
        with self.assertRaises(ValueError) as context:
            self.db.retrieve_collections(description_contains='real', name_contains='x')
            self.assertTrue('Invalid Request' in str(context))


    def test_get_collection_fails(self):
        """
        Make sure we handle a request for a non-existent collection gracefully
        """
        for i in range(5):
            self.db.create_collection(f'dummy{i}', 'no description', {})
        # expect an empty set, not an error for this one:
        cset = self.db.retrieve_collections(name_contains='Fred')
        self.assertEqual(cset,[])
        with self.assertRaises(ValueError) as context:
            fset = self.db.retrieve_files_in_collection('Fred')
            self.assertTrue('No such collection' in str(context))
        with self.assertRaises(ValueError) as context:
            c = self.db.retrieve_collection('Fred')
            self.assertTrue('No such collection' in str(context))

    def test_get_files_match(self):
        """
        Make sure we can get files in a collection AND
        those in a collection that match a specfic string
        """
        # set it up
        _dummy(self.db)
        files = self.db.retrieve_files_in_collection('dummy3')
        self.assertEqual(len(files), 10)
        files = self.db.retrieve_files_in_collection('dummy3', 'file1')
        self.assertEqual(len(files), 1)


class TestClick(unittest.TestCase):
    """
    Test the command line interface
    """

    def _mysetup(self, runner):
        """
        setup database context, we want to do that within the click runner context, hence not using default setUp.
        """
        database = 'sqlite:///temp.db'
        result = runner.invoke(cli, [f'--db={database}', 'save'])
        db = CollectionDB()
        db.init(database)
        _dummy(db)

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

    def test_save(self):
        """
        Test default save
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ['--db=sqlite:///temp.db', 'save'])
            assert result.exit_code == 0

    def test_ls(self):
        """
        Test a variety of ls options:
            ls --collection=all
            ls (no collection and no default)
            ls --collection=dummy2
            ls (with previous default set
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

    def test_find_across_collections(self):
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



if __name__ == "__main__":
    unittest.main()
