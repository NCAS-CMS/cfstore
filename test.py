import unittest
from interface import CollectionDB


class BasicStructure(unittest.TestCase):
    """
    Test basic table structure for cftape
    """
    def setUp(self):
        pass

    def test_create_collection(self):
        db = CollectionDB()
        db.init('sqlite:///memory:')
        print(db.tables)

        kw = {'resolution': 'N512', 'workspace': 'testws'}
        db.add_collection('mrun1', 'no real description', kw)

        files = [f'/somewhere/in/unix_land/file{i}' for i in range(10)]
        db.upload_files_to_collection('mrun1', files)

        print(db.collection_info('mrun1'))
        print(db.list_files_in_collection('mrun1'))


if __name__ == "__main__":
    unittest.main()
