import os, sys
from cfstore.db import StorageLocation, Collection, CoreDB, File, Tag, StorageLocation, StorageProtocol, CellMethod, Variable

from sqlalchemy import or_, and_, func
from sqlalchemy.orm.exc import NoResultFound
from cfstore.cfparse_file import cfparse_file
import hashlib

class CollectionError(Exception):
    def __init__(self, name, message):
        super().__init__(f'(Collection {name} {message}')


class CollectionDB(CoreDB):


    def cell_method_add(self, axis, method):
        """ 
        Add a new cell method to database, raise an error if it already exists.
        Returns the new cell method.
        """
        try:
            cm = self.cell_method_retrieve(axis=axis, method=method)
        except NoResultFound:
            cm = CellMethod(axis=axis, method=method)
            self.session.add(cm)
            self.session.commit()
            return cm
        else:
            raise ValueError(f'Attempt to add an existing cell method {cm}')

    def cell_method_get_or_make(self, axis, method):
        """
        Retrieve a specfic cell method, if it doesn't exist, create it, and return it.
        """
        try:
            self.cell_method_retrieve(axis=axis, method=method)
        except NoResultFound:
            return self.cell_method_add(axis=axis, method=method)

    def cell_method_retrieve(self, axis, method):
        """ 
        Retrieve a specific cell method
        """
        cm = self.session.query(CellMethod).filter(and_(CellMethod.axis==axis, CellMethod.method==method)).one()
        return cm


    def add_protocol(self, protocol_name, locations=[]):
        """
        Add a new protocol to the database, and if desired modify a set of existing or new
        locations by adding the protocol to their list of supported protocols.
        """
        
        try:
            pdb = self.session.query(StorageProtocol).filter_by(name=protocol_name).one()
        except NoResultFound:
            pdb = StorageProtocol(name=protocol_name)
            if locations:
                existing_locations = [e.name for e in self.retrieve_locations()]
                for p in locations:
                    if p not in existing_locations:
                        loc = StorageLocation(name=p)
                        self.session.add(loc)
                    else:
                        loc = self.retrieve_location(p)
                    pdb.used_by.append(loc)
            self.session.add(pdb)
            self.session.commit()
        else: 
            raise ValueError(f"Attempt to add existing protocol - {protocol_name}")

    def add_relationship(self, collection_one, collection_two, relationship):
        """
        Add a symmetrical <relationship> between <collection_one> and <collection_two>.
        e.g. add_relationship('romulus','remus','brother')
        romulus is a brother of remus and vice versa.
        """
        c1 = self.retrieve_collection(collection_one)
        c2 = self.retrieve_collection(collection_two)
        c1.add_relationship(relationship, c2)
        c2.add_relationship(relationship, c1)
        self.session.commit()

    def add_relationships(self, collection_one, collection_two, relationship_12, relationship_21):
        """
        Add a pair of relationships between <collection_one>  and <collection_two> such that
        collection_one has relationship_12 to collection_two and
        collection_two is a relationship_21 to collection_one.
        e.g. add_relationship('father_x','son_y','parent_of','child_of')
        (It is possible to add a one way relationship by passing relationship_21=None)
        """
        c1 = self.retrieve_collection(collection_one)
        c2 = self.retrieve_collection(collection_two)
        c1.add_relationship(relationship_12, c2)
        if relationship_21 is not None:
            c2.add_relationship(relationship_21, c1)
        self.session.commit()

    def add_variables_from_file(self, filename):
        """ Add all the variables found in a file to the database"""
        cfparse_file(self, filename)

    def create_collection(self, collection_name, description, kw={}):
        """
        Add a collection and any properties, and return instance
        """
        c = Collection(name=collection_name, volume=0, description=description)

        for k in kw:
            c[k] = kw[k]
            print(k.size)
            c.volume += k.size          
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

    def create_location(self, location, protocols=[],overwrite=False):
        """
        Create a storage <location>. The database is ignorant about what
        "location" means. Other layers of software care about that.
        However, it may have one or more protocols associated with it.
        """
        try:
            loc = self.session.query(StorageLocation).filter_by(name=location).one()
        except NoResultFound:
            loc = StorageLocation(name=location)
            
            if protocols:
                existing_protocols = self.retrieve_protocols()
                for p in protocols:
                    if p not in existing_protocols:
                        pdb = StorageProtocol(name=p)
                        self.session.add(pdb)
                    loc.protocols.append(pdb)
            self.session.add(loc)
            self.session.commit()
        else:
            if not overwrite:
                raise ValueError(f'{location} already exists')
            

    def create_tag(self, tagname):
        """
        Create a tag and insert into a database
        """
        t = Tag(name=tagname)
        self.session.add(t)
        self.session.commit()

    def locate_replicants(self, collection_name,
                          strip_base='',
                          match_full_path=False,
                          try_reverse_for_speed=False,
                          check="Both"):
        """
        Locate copies of a file across collections
        strip_base - remove given string from the file string
        match_full_path - find only if the full path matches if true, otherwise only filename
        try_reverse_for_speed - optimization approach that does not yet work and is not implemented
        check - check for "name", "size" or "both", checksum needs to be implemented
        """
        def strip(path, stem):
            """ If path starts with stem, return path without the stem, otherwise return the path"""
            if path.startswith(stem):
                return path[len(stem):]
            else:
                return path


        if try_reverse_for_speed:
            raise NotImplementedError
        else:
            # basic algorithm is to grab all the candidates, and then do a search on those.
            # a SQL wizard would do better.
            c = self.retrieve_collection(collection_name)
            candidates = self.retrieve_files_in_collection(collection_name)
            if check.lower() == "both":
                if strip_base:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [self.session.query(File).filter(
                            and_(File.name == f.name,
                                File.path == strip(f.path, strip_base),
                                File.size == f.size)).all()
                                    for f in candidates]
                    else:
                        possibles = [self.session.query(File).filter(
                                        and_(File.name == f.name, File.size == f.size)).all()
                                    for f in candidates]
                else:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [self.session.query(File).filter(
                            and_(File.name == f.name,
                                File.path == f.path,
                                File.size == f.size)).all()
                                    for f in candidates]
                                    
                    else:
                        possibles = [self.session.query(File).filter(
                            and_(File.name == f.name,
                                File.size == f.size)).all()
                                for f in candidates]
            if check.lower() == "name":
                if strip_base:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [self.session.query(File).filter(
                                        and_(File.name == f.name, File.path == strip(f.path, strip_base))).all()
                                    for f in candidates]
                    else:
                        possibles = [self.session.query(File).filter(
                            (File.name == strip(f.name, strip_base)))
                                    for f in candidates]
                else:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [self.session.query(File).filter(
                            and_(File.name == f.name,
                                File.path == f.path,
                                )).all()
                                    for f in candidates]
                    else:
                        possibles = [self.session.query(File).filter(
                                and_(File.name == f.name)).all()
                                for f in candidates]
            if check.lower() == "size":
                if strip_base:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [self.session.query(File).filter(
                                and_(File.size == f.size)).all()
                                for f in candidates]
                    else:
                        possibles = [self.session.query(File).filter(
                                and_(File.size == f.size)).all()
                                for f in candidates]
                else:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [self.session.query(File).filter(
                                and_(File.size == f.size, File.path == f.path)).all()
                                for f in candidates]
                    else:
                        possibles = [self.session.query(File).filter(
                                and_(File.size == f.size)).all()
                                for f in candidates]
        return candidates, possibles

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
            raise FileNotFoundError  #(f'File "{path}/{name}" not found')

    def retrieve_location(self, location_name):
        """
        Retrieve information about a specific location
        """
        try:
            x = self.session.query(StorageLocation).filter_by(name=location_name).one()
        except NoResultFound:
            raise ValueError(f'No such collection {location_name}')
        assert x.name == location_name
        return x


    def retrieve_locations(self):
        """
        Retrieve locations.
        Currently retrieves all known locations.
        """
        locs = self.session.query(StorageLocation).all()
        return locs

    def retrieve_protocols(self):
        """
        Retrieve protocols.
        """
        p = self.session.query(StorageProtocol).all()
        return p

    def retrieve_related(self, collection, relationship):
        """
        Find all related collections to <collection> which have
        <relationship> as the predicate.
        """
        c = self.session.query(Collection).filter_by(name=collection).one()
        try:
            r = c.related[relationship]
            return r
        except KeyError:
            return []

    def retrieve_files_which_match(self, match):
        """
        Retrieve files where <match> appears in either the path or the name.
        """
        m = f'%{match}%'
        return self.session.query(File).filter(or_(File.name.like(m), File.path.like(m))).all()

    def retrieve_files_in_collection(self, collection, match=None, replicants=False):
        """
        Return a list of files in a particular collection, possibly including those
        which match a particular string and/or are replicants.
        """
        # do all the query combinations separately, likely to be more efficient ...
        if match is None and replicants is False:
            return self.retrieve_collection(collection).holds_files
        elif match and replicants is False:
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
        elif replicants and match is None:
            files = self.session.query(File).filter(File.in_collections.any(
                                Collection.name == collection)).join(
                                File.replicas).group_by(File).having(
                                func.count(StorageLocation.id) > 1).all()
            return files
        else:
            m = f'%{match}%'
            files = self.session.query(File).filter(and_(
                File.in_collections.any(Collection.name == collection),
                or_(File.name.like(m), File.path.like(m)))).join(
                File.replicas).group_by(File).having(
                func.count(StorageLocation.id) > 1).all()
            #TODO add checksum here
            return files


    def delete_file_from_collection(self, collection, file):
        """
        Delete a file from a collection
        """

        path, filename = os.path.split(str(file))
        f = self.retrieve_file(path,filename)

        c = self.session.query(Collection).filter_by(name=collection).one()
        if not (str(file) in map(str,c.holds_files)):
            raise ValueError(f"Attempt to delete file {file} from {c} - but it's already not there")
        c.holds_files.remove(f)      
        c.volume -= f.size
        if not any([f in self.collections]):
            try:
                uc = self.session.query(Collection).filter_by(name="unlisted").one()
            except NoResultFound:
                uc = Collection(name="unlisted", volume=0, description="Holds unlisted files")
                self.session.add(uc)
            uc.holds_files.append(f)
            uc.volume += f.size
        self.session.commit()

        
    def retrieve_variable(self, key, value):
        """ Retrieve variable by arbitrary property"""
        queries = []
        if key in ['long_name','standard_name','cfdm_size','cfdm_domain','cell_methods']:
            queries.append(getattr(Variable,key) == value)
        else:
            queries.append(Variable.with_other_attributes(key,value))
        if key == 'in_files':
            queries.append([value == k for k in Variable.in_files])
        if len(queries) == 0:
            raise ValueError('No query received for retrieve variable')
        elif len(queries) == 1:
            results = self.session.query(Variable).filter(queries[0]).all()
        else:
            results = self.session.query(Variable).filter(and_(*queries)).all()
        if key == "all":
            results = self.session.query(Variable).all()
        return results

    def retrieve_variable_query(self, key, value, query):
        """ Retrieve variable by arbitrary property"""
        queries = query
        if key in ['long_name','standard_name','cfdm_size','cfdm_domain','cell_methods']:
            queries.append(getattr(Variable,key) == value)
        else:
            queries.append(Variable.with_other_attributes(key,value))
        if key == 'in_files':
            queries.append([value == k for k in Variable.in_files])
        if len(queries) == 0:
            raise ValueError('No query received for retrieve variable')
        elif len(queries) == 1:
            results = self.session.query(Variable).filter(queries[0]).all()
        else:
            results = self.session.query(Variable).filter(and_(*queries)).all()
        return results, queries

    def delete_collection(self, collection_name,force):
        """
        Remove a collection from the database, ensuring all files have already been removed first.
        """
        files = self.retrieve_files_in_collection(collection_name)
        if files and force:
            for f in files:
                self.delete_file_from_collection(collection_name, f)
            c = self.retrieve_collection(collection_name)
            self.session.delete(c)
            self.session.commit()
        elif files:
            raise CollectionError(collection_name, f'not empty (contains {len(files)} files)')
        else:
            c = self.retrieve_collection(collection_name)
            self.session.delete(c)
            self.session.commit()

    def delete_tag(self, tagname):
        """
        Delete a tag, from wherever it is used
        """
        t = self.session.query(Tag).filter_by(name=tagname)
        self.session.delete(t)
        self.session.commit()

    def add_file_to_collection(self, collection, file):
        """
        Add file to a collection
        """
        c = self.session.query(Collection).filter_by(name=collection).one()
        if file in c.holds_files:
            raise ValueError(f"Attempt to add file {file} to {c} - but it's already there")
        c.holds_files.append(file)
        c.volume += file.size        
        self.session.commit()

    def collection_info(self, name):
        """
        Return information about a collection
        """
        try:
            c = self.session.query(Collection).filter_by(name=name).first()
        except NoResultFound:
            raise ValueError(f'No such collection {name}')
        return c.md

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
            #c = self.create_collection(collection, description, {})
            c = Collection(name=collection, volume=0, description=description)
            self.session.add(c)
        missing = []
        for f in files:
            path, name = os.path.split(f)
            try:
                ff = self.retrieve_file(path, name)
                c.holds_files.append(ff)
            except FileNotFoundError:
                missing.append(f)
        if missing: 
            message = "ERROR: Operation not completed: The following files were not found in database:\n-> "
            message += '\n-> '.join(missing)
            raise FileNotFoundError(message)
               
        print([x for x in c.holds_files])
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

    def upload_file_to_collection(self, location, collection, f,  lazy=0, update=True):
        """
        Add a (potentially) new file <f> from <location> into the database, and add details to <collection>
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

        if 'checksum' not in f:
            f['checksum'] = 'None'
        name, path, size, checksum = f['name'], f['path'], f['size'], f['checksum']
        c.volume+=f["size"]
        try:
            if lazy == 0:
                check = self.retrieve_file(path, name)
            elif lazy == 1:
                check = self.retrieve_file(path, name, size=size)
            elif lazy == 2:
                check = self.retrieve_file(path, name, checksum=checksum)
            else:
                raise ValueError(f'Unexpected value of lazy {lazy}')
        except FileNotFoundError:
            check = False

        if check:
            if not update:
                raise ValueError(f'Cannot upload file {os.path.join(path, name)} as it already exists')
            else:
                check.replicas.append(loc)
                c.holds_files.append(check)
        else:
            try:
                fmt = f['format']
            except KeyError:
                fmt = os.path.splitext(name)[1]
            f = File(name=name, path=path, checksum=checksum, size=size, format=fmt, initial_collection=c.id)
            f.replicas.append(loc)
            c.holds_files.append(f)
            loc.holds_files.append(f)
            c.volume += f.size
            loc.volume += f.size
        self.session.commit()

    def upload_files_to_collection(self, location, collection, files):
        """
        Add new files which exist at <location> to a <collection>. Both
        location and collection must already exist.

        <files>: list of file dictionaries
            {name:..., path: ..., checksum: ..., size: ..., format: ...}

        """
        for f in files:
            self.upload_file_to_collection(location, collection, f)

    def remove_file_from_collection(self, collection, file_path, file_name, checksum=None):
        """
        Remove a file described by <path_name> and <file_name> (and optionally <checksum> from a particular
        <collection>. Raise an error if already removed from collection (or, I suppose, if it was never
        in that collection, the database wont know!)
        """
        f = self.retrieve_file(file_path, file_name)
        c = self.retrieve_collection(collection)
        c.volume -= f.size
        try:
            index = f.in_collections.index(c)
        except ValueError:
            raise CollectionError(collection, f' - file {file_path}/{file_name} not present!')
        del f.in_collections[index]


    @property
    def _tables(self):
        """
        List the names of all the tables in the database interface
        """
        return self.engine.table_names()

    def delete_collection_with_files(collection):
        pass

def chkeq(file1,file2,try_hash=False,return_hash=False):
    """
    Compare the equality of two files
    """

    a_file = open(file1, "rb")
    b_file = open(file2, "rb")

    
    filesize_equal= (os.path.getsize(a_file) ==os.path.getsize(b_file))
    if try_hash and not filesize_equal:
        sha256_hash = hashlib.sha256()    

        for byte_block in iter(lambda: a_file.read(4096),b""):
            sha256_hash.update(byte_block)
        a_hash = sha256_hash.hexdigest()
        
        sha256_hash = hashlib.sha256()   
     
        for byte_block in iter(lambda: b_file.read(4096),b""):    
            sha256_hash.update(byte_block)
        b_hash = sha256_hash.hexdigest()
       
        hash_equal = a_hash==b_hash
        
        if(return_hash):
            return(hash_equal,a_hash,b_hash)
            
        return (hash_equal)
    return(filesize_equal)
