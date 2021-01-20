import unittest
from cfstore.interface import CollectionDB, CollectionError
from click.testing import CliRunner
import os
from cfstore.cfdb import cli


def _dummy(db):
    """ Set up a dummy dataset in db with accessible
    structure for testing
    """
    db.create_location('testing')
    for i in range(5):
        c = f'dummy{i}'
        db.create_collection(c,'no description', {})
        files = [{'path':'/somewhere/in/unix_land', 'name':f'file{j}{i}', 'size':0} for j in range(10)]
        db.upload_files_to_collection('testing', c, files)


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
        self.db.create_location('testing')
        files = [{'path': '/somewhere/in/unix_land', 'name': f'file{i}', 'size': 0} for i in range(10)]
        self.db.upload_files_to_collection('testing', 'mrun1', files)

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

    def test_collection_properties(self):
        """
        Test ability to create properties on collections and query against them.
        """
        _dummy(self.db)
        choice = ['dummy2', 'dummy3']
        for c in choice:
            cc = self.db.retrieve_collection(c)
            cc['color'] = 'green'
        r = self.db.retrieve_collections(facet=('color', 'green'))
        self.assertEqual(choice, [x.name for x in r])

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

    def test_add_relationship(self):
        """
        Make sure we can add relationships between collections which are symmetrical
        """
        _dummy(self.db)
        self.db.add_relationship('dummy1', 'dummy3', 'brother')
        x = self.db.retrieve_related('dummy1','brother')
        self.assertEqual('dummy3', x[0].name)
        x = self.db.retrieve_related('dummy3', 'brother')
        self.assertEqual('dummy1', x[0].name)

    def test_add_relationships(self):
        """
        Make sure we can add and use assymetric relationships between collections
        """
        _dummy(self.db)
        self.db.add_relationships('dummy1', 'dummy3', 'parent_of', 'child_of')
        x = self.db.retrieve_related('dummy1', 'parent_of')
        self.assertEqual(['dummy3'], [j.name for j in x])
        x = self.db.retrieve_related('dummy3', 'child_of')
        self.assertEqual(['dummy1', ], [j.name for j in x])

    def test_delete_collection(self):
        """
        Make sure delete collection works and respects files in collection
        """
        _dummy(self.db)
        with self.assertRaises(CollectionError) as context:
            self.db.delete_collection('dummy1')
        files = self.db.retrieve_files_in_collection('dummy1')
        for f in files:
            self.db.remove_file_from_collection('dummy1', f.path, f.name)
        self.db.delete_collection('dummy1')
        with self.assertRaises(ValueError) as context:
            c = self.db.retrieve_collection('dummy1')

    def test_remove_from_collection(self):
        """
        Test removing file from a collection
        """
        _dummy(self.db)
        path = '/somewhere/in/unix_land'
        files = self.db.retrieve_files_in_collection('dummy1')
        # first let's make sure the right thing happens if the file doesn't exist
        with self.assertRaises(FileNotFoundError):
            self.db.remove_file_from_collection('dummy1', path, 'abc123')
        # if it isn't in the collection
        with self.assertRaises(CollectionError):
            self.db.remove_file_from_collection('dummy2', path, 'file33')
        for f in files:
            self.db.remove_file_from_collection('dummy1', f.path, f.name)
            # this checks it's no longer in the collection
            with self.assertRaises(CollectionError):
                self.db.remove_file_from_collection('dummy1', f.path, f.name)
        # and this checks it still exists
        for f in files:
            f = self.db.retrieve_file(f.path, f.name)

    def test_retrieve_file(self):
        """
        Test retrieving files
        """
        _dummy(self.db)
        path = '/somewhere/in/unix_land'
        # first let's make sure the right thing happens if the file doesn't exist
        with self.assertRaises(FileNotFoundError) as context:
            f = self.db.retrieve_file(path, 'abc123')
        # now check we can find a particular file
        f = self.db.retrieve_file(path, 'file01')
        self.assertEqual(f.name, 'file01')

    def test_file_match(self):
        """
        Test what happens when we add files which have common characteristics in two different locations.
        What we want to happen is that the files appear as one file, with two different replicants.
        We also want to be able to find such files, so we test that here too.
        """
        # _dummy(self.db)
        raise NotImplementedError


if __name__ == "__main__":
    unittest.main()
