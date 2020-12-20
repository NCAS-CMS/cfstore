import os, sys
from db import Collection, CoreDB, File, Tag
from sqlalchemy import or_, and_
from sqlalchemy.orm.exc import NoResultFound


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

    def create_tag(self, tagname):
        """
        Create a tag and insert into a database
        """
        t = Tag(name=tagname)
        self.session.add(t)
        self.session.commit()


    def retrieve_collection(self, collection_name):
        try:
            c = self.session.query(Collection).filter_by(name=collection_name).one()
        except NoResultFound:
            raise ValueError(f'No such collection {collection_name}')
        assert c.name == collection_name
        return c

    def retrieve_collections(self, name_contains=None, description_contains=None, contains=None, tagname=None, facet=None):
        """
        Return a list of all collections as collection instances
        optionally including those which contain
            the string <name_contains> somewhere in their name OR
            <description_contains> somewhere in their description OR
            the string <contains> is either in the name or the description OR
            with specific tagname OR
            the properties dictionary for the collection contains key with value - facet = (key,value)
        """
        if [name_contains, description_contains, contains, tagname, facet].count(None) <= 3:
            raise ValueError(
                'Invalid request to <get_collections>, cannot search on more than one of name, description, tag, facet')

        if name_contains:
            return self.session.query(Collection).filter(Collection.name.like(f'%{name_contains}%')).all()
        elif description_contains:
            return self.session.query(Collection).filter(Collection.description.like(f'%{description_contains}%')).all()
        elif contains:
            contains = f'%{contains}%'
            return self.session.query(Collection).filter(or_(Collection.description.like(contains),
                Collection.name.like(contains))).all()
        elif tagname:
            tag = self.session.query(Tag).filter_by(name=tagname).one()
            return tag.in_collections
            #return self.session.query(Collection).join(Collection.tags).filter_by(name=tagname).all()
        elif facet:
            key, value = facet
            return self.session.query(Collection).filter(Collection.with_property(key, value)).all()
        else:
            return self.session.query(Collection).all()

    def retrieve_file(self, filename):
        path, name = os.path.split(filename)
        return self.session.query(File).filter(and_(File.name == name, File.path == path)).one()

    def retrieve_files_which_match(self, match):
        m = f'%{match}%'
        return self.session.query(File).filter(or_(File.name.like(m), File.path.like(m))).all()

    def retrieve_files_in_collection(self, collection, match=None):
        """
        List files in a particular collection
        """
        if match is None:
            return self.retrieve_collection(collection).holds_files
        else:
            m = f'%{match}%'
            # this gives the collection with files that match this ... not what we wanted
            #files = self.session.query(Collection).filter_by(name=collection).join(
            #    Collection.holds_files).filter(or_(File.name.like(m), File.path.like(m))).all()
            # However, given we know that the number of files is much greater than the number
            # of collections, it's likely that searching the files that match a collection first
            # could be faster. We can investigate that another day ...
            files = self.session.query(File).filter(or_(File.name.like(m), File.path.like(m))).join(
                File.in_collections).filter_by(name=collection).all()
            return files

    def delete_collection(self, collection_name):
        # Will need to worry about files and whether they will be left hanging
        raise NotImplementedError

    def delete_tag(self, tagname):
        """
        Delete a tag, from wherever it is used
        """
        t = self.session.query(Tag).filter(name=tagname)
        self.session.delete(t)
        self.session.commit()

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
        try:
            c = self.session.query(Collection).filter_by(name=name).first()
        except NoResultFound:
            raise ValueError(f'No such collection {name}')

        return str(c), [str(k) for k in c.tags]

    def organise(self, collection, files, description):
        """
        Organise files already known to the environment into collection,
        (creating collection if necessary)
        """
        try:
            c = self.retrieve_collection(collection)
        except ValueError:
            if not description:
                description = 'Manually organised collection'
            c = self.create_collection(collection, description, {})
        for f in files:
            ff = self.retrieve_file(f)
            c.holds_files.append(ff)
        self.session.commit()

    def tag_collection(self, collection_name, tagname):
        """
        Associate a tag with a collection
        """
        tag = self.session.query(Tag).filter_by(name=tagname).first()
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

    def upload_file_to_collection(self, collection, name, path, checksum, size):
        """
        Add a new file into the database, and add details to collection
        """
        c = self.session.query(Collection).filter_by(name=collection).first()
        f = File(name=name, path=path, checksum=checksum, size=size, initial_collection=c.id)
        c.holds_files.append(f)
        self.session.commit()

    def upload_files_to_collection(self, collection, files):
        """
        Add new files to a collection
        :param collection:
        :param files: list of file tuples [(name, size, checksum),...]
        :return: None
        """
        for f in files:
            full_name = f[0]
            size = f[1]
            try:
                checksum = f[2]
            except IndexError:
                checksum = ''
            path, name = os.path.split(full_name)
            self.upload_file_to_collection(collection, name, path, checksum, size)

    def remove_file_from_collection(self, collection, file):
        raise NotImplementedError



    @property
    def tables(self):
        return self.engine.table_names()

