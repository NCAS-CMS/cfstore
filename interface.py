import os
from db import Collection, CoreDB, File, Tag
import sqlite3


class CollectionDB(CoreDB):

    def create_collection(self, collection_name, description, kw):
        """
        Add a collection and any properties, and return instance
        """
        c = Collection(name=collection_name, description=description)

        for k in kw:
            c[k] = kw[k]
        self.session.add(c)

        try:
            self.session.commit()
        except Exception as e:
            # Couldn't work out how to trap this properly ... so
            if 'UNIQUE constraint' in str(e):
                raise ValueError(f'Cannot add duplicate collection (name={collection_name})')
            else:
                raise
        return c

    def retrieve_collection(self, collection_name):
        c = self.session.query(Collection).filter_by(name=collection_name).one()
        assert c.name == collection_name
        return c

    def delete_collection(self, collection_name):
        # Will need to worry about files and whether they will be left hanging
        raise NotImplementedError

    def get_files_in_collection(self, collection):
        """
        List files in a particular collection
        """
        dataset = self.session.query(Collection).filter_by(name=collection).one().holds_files
        return dataset

    def add_file_to_collection(self, collection, file):
        """
        Add file to a collection
        """
        raise NotImplementedError

    def remove_file_from_collection(self, collection, file):
        """
        Remove a file from a collection.
        """
        raise NotImplementedError

    def collection_info(self, name):
        """
        Return information about a collection
        """
        c = self.session.query(Collection).filter_by(name=name).first()
        return str(c), [str(k) for k in c.tags]

    ### tag API

    def create_tag(self, tagname):
        """
        Create a tag and insert into a database
        """
        t = Tag(name=tagname)
        self.session.add(t)
        self.session.commit()

    def delete_tag(self, tagname):
        """
        Delete a tag, from wherever it is used
        """
        t = self.session.query(Tag).filter(name=tagname)
        self.session.delete(t)
        self.session.commit()

    def tag_collection(self, tagname, collection_name):
        """
        Associate a tag with a collection
        """
        tag = self.session.query(Tag).filter_by(name=tagname).one()
        if not tag:
            tag = Tag(name=tagname)
            self.session.add(tag)
        c = self.retrieve_collection(collection_name)
        c.tags.append(tag)
        self.session.commit()

    def remove_tag_from_collection(self, tagname, collection_name):
        """
        Remove a tag from a collection
        """
        c = self.retrieve_collection(collection_name)
        print(c)

    def list_collections_with_tag(self, tag):
        """
        Find all collections with a given tag
        """
        tag = self.session.query(Tag).filter_by(name=tag).one()
        return tag.in_collections

    ###
    # Files API
    ###

    def upload_file_to_collection(self, collection, name, path, checksum):
        """
        Add a new file into the database, and add details to collection
        """
        c = self.session.query(Collection).filter_by(name=collection).first()
        f = File(name=name, path=path, checksum=checksum, initial_collection=c.id)
        c.holds_files.append(f)
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

    def remove_file_from_collection(self, collection, file):
        raise NotImplementedError

    @property
    def tables(self):
        return self.engine.table_names()



