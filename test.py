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
    Test basic table structure for cftape
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

        self.assertEquals(len(self.db.get_files_in_collection('mrun1')), len(files))

    def test_add_tag(self):
        """
        Need to add tags, and select by tags
        """
        tagname = 'test_tag'
        self.db.create_tag(tagname)
        for i in range(5):
            self.db.create_collection(f'mrun{i}', 'no real description', {})
        self.db.tag_collection(tagname, 'mrun1')
        self.db.tag_collection(tagname, 'mrun3')
        tagged = self.db.list_collections_with_tag(tagname)
        assert ['mrun1', 'mrun3'] == [x.name for x in tagged]

    def test_get_collections(self):
        """
        Test ability to get a subset of collections via name and/or description
        """
        for i in range(5):
            self.db.create_collection(f'dummy{i}','no description', {})
            self.db.create_collection(f'eg{i}','no description', {})
        self.db.create_collection('dummy11','real description',{})
        self.assertEquals(len(self.db.get_collections()), 11)
        self.assertEquals(len(self.db.get_collections(name_contains='g')), 5)
        self.assertEqual(len(self.db.get_collections(description_contains='real')), 1)
        with self.assertRaises(ValueError) as context:
            self.db.get_collections(description_contains='real', name_contains='x')
            self.assertTrue('Invalid Request' in str(context))

    def test_get_collection_fails(self):
        """
        Make sure we handle a request for a non-existent collection gracefully
        """
        for i in range(5):
            self.db.create_collection(f'dummy{i}', 'no description', {})
        # expect an empty set, not an error for this one:
        cset = self.db.get_collections(name_contains='Fred')
        self.assertEqual(cset,[])
        with self.assertRaises(ValueError) as context:
            fset = self.db.get_files_in_collection('Fred')
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
        files = self.db.get_files_in_collection('dummy3')
        self.assertEqual(len(files), 10)
        files = self.db.get_files_in_collection('dummy3','file1')
        self.assertEqual(len(files), 1)


class TestClick(unittest.TestCase):

    def _mysetup(self, runner):
        database = 'sqlite:///temp.db'
        result = runner.invoke(cli, [f'--db={database}', 'save'])
        db = CollectionDB()
        db.init(database)
        _dummy(db)

    def test_save(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ['--db=sqlite:///temp.db', 'save'])
            assert result.exit_code == 0

    def test_find_in_collection(self):
        """
        test command line "find" method
        this test expects to find one file, with file name starting with file1
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, ['find', 'file1', '--collection=dummy3'])
            assert result.exit_code == 0
            lines = result.output.split('\n')[:-1]
            self.assertEqual(len(lines), 1)
            self.assertTrue(lines[0].find('file1') != -1)
            # now do it again and make sure we are looking at the default
            result = runner.invoke(cli, ['find', 'file1'])
            assert result.exit_code == 0
            lines = result.output.split('\n')[:-1]
            self.assertEqual(len(lines), 1)

    def test_find_across_collections(self):
        """
        test command line "find" method
        this test expects to find five files,
        one from each dummy collection.
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            result = runner.invoke(cli, ['find', 'file2', '--collection=all'])
            if result.exit_code != 0:
                raise result.exception
            lines = result.output.split('\n')[:-1]
            self.assertEqual(len(lines), 5)

    def test_organise(self):
        """
        Test the method of organising files into collections, using data from stdin
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            self._mysetup(runner)
            dummy_input = '/somewhere/in/unix_land/file12\n/somewhere/in/unix_land/file23\n'
            result = runner.invoke(cli, ['organise', 'newc'], input=dummy_input)
            if result.exit_code != 0:
                raise result.exception
            result = runner.invoke(cli, ['ls','--collection=newc'])
            self.assertEqual(dummy_input, result.output)







if __name__ == "__main__":
    unittest.main()
