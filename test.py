import unittest
from interface import CollectionDB

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
        """ Need to add tags, and select by tags """
        tagname = 'test_tag'
        self.db.create_tag(tagname)
        for i in range(5):
            self.db.create_collection(f'mrun{i}', 'no real description', {})
        self.db.tag_collection(tagname, 'mrun1')
        self.db.tag_collection(tagname, 'mrun3')
        tagged = self.db.list_collections_with_tag(tagname)
        assert ['mrun1', 'mrun3'] == [x.name for x in tagged]

    def test_get_collections(self):
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



if __name__ == "__main__":
    unittest.main()
