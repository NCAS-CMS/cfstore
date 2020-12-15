import os
from db import Collection, CoreDB, File


class CollectionDB(CoreDB):

    def add_collection(self, collection_name, description, kw):
        """
        Add a collection and any properties, and return instance
        """
        c = Collection(name=collection_name, description=description)
        for k in kw:
            c[k] = kw[k]
        self.session.add(c)
        self.session.commit()
        return c

    def list_files_in_collection(self, collection):
        """
        List files in a particular collection
        """
        dataset = self.session.query(Collection).filter_by(name=collection).one().holds_files
        print(dataset)

    def add_file_to_collection(self, collection, file):
        """
        Add file to a collection
        """
        pass

    def remove_file_from_collection(self, collection, file):
        """
        Remove a file from a collection.
        """
        pass

    def collection_info(self, name):
        """
        Return information about a collection
        """
        c = self.session.query(Collection).filter_by(name=name).first()
        return str(c)

    ### tag API

    def create_tag(self):
        """
        Create a tag and insert into a database
        """
        pass

    def tag_collection(self, tagname, collection_name):
        """
        Associate a tag with a collection
        """
        pass

    def remove_tag_from_collection(self, tagname, collection_name):
        """
        Remove a tag from a collection
        """
        pass

    def list_collections_with_tag(self, tag):
        """
        Find all collections with a given tag
        """
        pass

    ###
    # Files API
    ###

    def upload_file_to_collection(self, collection, name, path, checksum):
        """
        Add a new file into the database, and add details to collection
        """
        c = self.session.query(Collection).filter_by(name=collection).first()
        f = File(name=name, path=path, checksum=checksum, initial_collection=c.id)
        self.session.add(f)
        self.session.commit()

    def upload_files_to_collection(self, collection, files):
        """
        Add new files to a collection
        :param collection:
        :param files: list of file tuples [(name, checksum),...]
        :return: None
        """
        for f in files:
            full_name = f[0]
            try:
                checksum = f[1]
            except IndexError:
                checksum = ''
            path, name = os.path.split(full_name)
            self.upload_file_to_collection(collection, name, path, checksum)

    @property
    def tables(self):
        return self.engine.table_names()



