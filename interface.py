import os, sys
from db import StorageLocation, Collection, CoreDB, File, Tag
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

    def create_location(self, location):
        """
        Create a storage location
        """
        loc = StorageLocation(name=location)
        self.session.add(loc)
        self.session.commit()

    def create_tag(self, tagname):
        """
        Create a tag and insert into a database
        """
        t = Tag(name=tagname)
        self.session.add(t)
        self.session.commit()

    def retrieve_collection(self, collection_name):
        """
        Retrieve a particular collection via it's name <collection_name>.
        """
        try:
            c = self.session.query(Collection).filter_by(name=collection_name).one()
        except NoResultFound:
            raise ValueError(f'No such collection {collection_name}')
        assert c.name == collection_name
        return c

    def retrieve_collections(self, name_contains=None, description_contains=None, contains=None, tagname=None, facet=None):
        """
        Return a list of all collections as collection instances,
        optionally including those which contain:

        - the string <name_contains> somewhere in their name OR
        - <description_contains> somewhere in their description OR
        - the string <contains> is either in the name or the description OR
        - with specific tagname OR
        - the properties dictionary for the collection contains key with value - facet = (key,value)

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

    def retrieve_file(self, path, name, size=None, checksum=None):
        """
        Retrieve a file with <path> and <name>.

        If one of <size> or <checksum> are provided (both is an error), make sure
        it has the correct size or checksum.

        (The use case for extra detail is to look for specific files amongst duplicates.)
        """

        if size and checksum:
            raise ValueError('Can only retrieve files by size OR checksum, not both!')
        if size:
            x = self.session.query(File).filter(
                and_(File.name == name, File.path == path, File.size == size)).all()
        elif checksum:
            x = self.session.query(File).filter(
                and_(File.name == name, File.path == path, File.checksum == checksum)).all()
        else:
            x = self.session.query(File).filter(
                and_(File.name == name, File.path == path)).all()
        if x:
            assert len(x) == 1
            return x[0]
        else:
            return None

    def retrieve_files_which_match(self, match):
        """
        Retrieve files where <match> appears in either the path or the name.
        """
        m = f'%{match}%'
        return self.session.query(File).filter(or_(File.name.like(m), File.path.like(m))).all()

    def retrieve_files_in_collection(self, collection, match=None):
        """
        Return a list of files in a particular collection.
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
        """
        Remove a collection from the database, ensuring that if it is primary upload
        collection, that all files have already been removed first.
        """
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
            path, name = os.path.split(f)
            ff = self.retrieve_file(path, name)
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

    def upload_file_to_collection(self, location, collection,
                                  name, path, checksum, size, lazy=0, update=False):
        """
        Add a (potentially) new file from <location> into the database, and add details to <collection>
        (both of which must already be known to the system).

        The assumption here is that the file is _new_, as otherwise we would be us using
        organise to put the file into a collection. However, depending on the value
        of update, we can decide whether or not to enforce that assumption.

        We check that assumption, by looking for

            if lazy==0: a file with the same path before uploading it ,or
            if lazy==1: a file with the same path and size,
            if lazy==2: a file with the same path and checksum.

        If we do find existing files, and <update> is True, then we will simply add
        a link to the new file as a replica. If <update> is False, we raise an error.
        """
        try:
            c = self.session.query(Collection).filter_by(name=collection).one()
            loc = self.session.query(StorageLocation).filter_by(name=location).one()
        except NoResultFound:
            raise ValueError('Either location or collection not yet available in database')

        if lazy == 0:
            check = self.retrieve_file(path, name)
        elif lazy == 1:
            check = self.retrieve_file(path, name, size=size)
        elif lazy == 2:
            check = self.retrieve_file(path, name, checksum=checksum)
        else:
            raise ValueError(f'Unexpected value of lazy {lazy}')

        if check:
            if update:
                raise ValueError(f'Cannot upload file {os.path.join(path, name)} as it already exists')
            else:
                check.replicas.append(location)
                c.holds_files.append(check)
        else:
            f = File(name=name, path=path, checksum=checksum, size=size, initial_collection=c.id)
            f.replicas.append(loc)
            c.holds_files.append(f)
        self.session.commit()

    def upload_files_to_collection(self, location, collection, files):
        """
        Add new files which exist at <location> to a <collection>. Both
        location and collection must already exist.

        <files>: list of file tuples [(name, size, checksum),...]
        """
        for f in files:
            full_name = f[0]
            size = f[1]
            try:
                checksum = f[2]
            except IndexError:
                checksum = ''
            path, name = os.path.split(full_name)
            self.upload_file_to_collection(location, collection, name, path, checksum, size)

    def remove_file_from_collection(self, collection, file_path, file_name, checksum=None):
        """
        Remove a file described by <path_name> and <file_name> (and optionally <checksum> from a particular
        <collection>.

        If it no longer belongs to any collection, it will be removed completely from the database
        """
        raise NotImplementedError


    @property
    def _tables(self):
        """
        List the names of all the tables in the database interface
        """
        return self.engine.table_names()

