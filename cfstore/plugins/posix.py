import os
import cf
import json
from cfstore import db
from cfstore.plugins.ssh import SSHlite
from cfstore.cfparse_file import manage_types
import re
from cfstore.db import Variable
from deepdiff import DeepDiff

class Posix:
    """

    Supports cfstore view of _part_ or _all_ of a posix storage path,
    using a "collection_head" for the entire thing, and "sub_collection_of"
    for all directories within it.

    """
    def __init__(self, db, location):
        """

        Initialise PosixGWS with database and location name. Will instantiate location
        in database if necessary.

        """
        self.db = db
        self.location = location
        try:
            loc = self.db.create_location(location)
        except ValueError:
            print(f'Using existing location ({location})')


    def add_collection(self, path_to_collection_head, collection_head_name, collection_head_description,
                       subcollections=False, checksum=None,regex=None):
        """

        Add a new collection with all files below <path_to_collection_head>,
        and call that collection <collection_head_name>, and decorate with <collection_head_description> text.

        Optionally (<subcollections=True>), create sub-collections for all internal directories
        (default = False = do not create sub-collections). (NOT YET IMPLEMENTED) Not Implemented

        If checksums required, provide a checksum method string.
        (NOT YET IMPLEMENTED) Not Implemented

        """
        c = self.db.create_collection(collection_head_name, collection_head_description)
        args=[path_to_collection_head, collection_head_name, collection_head_description,subcollections, checksum,regex]
        keys=["_path_to_collection_head", "_collection_head_name", "_collection_head_description","_subcollections", "_checksum","_regex"]
        
        for n in range(len(args)):
            c[keys[n]] = str(args[n])
        self._walk(path_to_collection_head, collection_head_name, subcollections, checksum,regex)

    def _walk(self, path_to_collection_head, collection_head_name, subcollections, checksum,regex):
        """ Walk local POSIX tree"""
        if subcollections:
            for dirName, directories, files in os.walk(path_to_collection_head):
                dbfiles = []
                for f in files:
                    if not regex or re.match(regex, f):
                        fp = dirName+"/"+f
                        dbfiles.append(self._file2dict(fp, os.stat(fp).st_size, checksum=checksum))
                self.db.upload_files_to_collection(self.location, collection_head_name, dbfiles)
        else:
            for dirName, directories, files in os.walk(path_to_collection_head):
                dbfiles = []
                for f in files:
                    if not regex or re.match(regex, f):    
                        if dirName == path_to_collection_head:
                            fp = path_to_collection_head+"/"+f
                            dbfiles.append(self._file2dict(fp, os.stat(fp).st_size, checksum=checksum))
                    self.db.upload_files_to_collection(self.location, collection_head_name, dbfiles)

    def getBMetadata(self,remotepath, collection, localpath, subcollections, checksum,regex):
        """
        Currently runs a remote script
        Used in conjunction with "getBMetadata" script to aquire BMetadata from remote files
        #FIXME This just needs to be massively cleaned up
        """
        print("Getting b metadata")
        #files = self.ssh.get_files_and_sizes(path_to_collection_head, subcollections)
        #self.ssh.get_b_metadata(path_to_collection_head,self.db)
        self.ssh.run_script(remotepath, collection, localpath)

    def _file2dict(self, path_to_file, size,  checksum=None):
        """
        Build the dictionary of file information needed for cfstore
        """
        p, n = os.path.split(path_to_file)
        if checksum is not None:
            raise NotImplementedError

        f = {'size': size,
             'path': p,
             'name': n
             }
        return f

    def check_collection(self, collection, update=True, processes=4):
        """
        Used to check AND update status of files in a collection. If files already have checksums, will return
        those files which have changed checksums (user has to decide whether that is corruption or a
        deliberate change). If update is True, the checksums will be updated, if not, they will only be
        updated if there was no previous checksum. (Files with no previous checksum will not be returned as changed,
        come what may. By default, use <processes> subprocesses to do the checksumming.
        """
        raise NotImplementedError

    def aggregation_files_to_collection(self, aggfile):
        """
        Uses a cf python aggregation file to add metadata variables to the appropriate files
        """
        #open file
        #read file into list of variables
        print("Adding variables from",aggfile)
        aggfileobject = open(aggfile,'r')
        variables = json.load(aggfileobject)
        dbfiles=[]
        #for each variable
        
        for v in variables:
            print(v)
            v = Variable(standard_name=v['standard_name'],long_name=v['long_name'],cfdm_size=v['cfdm_size'],cfdm_domain=['cfdm_domain'])
            properties = v.properties()

            if ('standard_name' not in properties and 'long_name' not in properties):
                properties['long_name'] = v.identity
            name, long_name = v.get_property('standard_name', None), v.get_property('long_name', None)

            domain = v.domain._one_line_description()
            size = v.size

            var = Variable(standard_name=name, long_name=long_name, cfdm_size=size, cfdm_domain=domain)
            for k,p in properties.items():
                if k not in ['standard_name','long_name']:
                    var[k] = manage_types(p) 

            for file in v.get_filenames():
                for f in db.retrieve_files_which_match(os.path.basename(file)):
                    var.in_files.append(f)

            #there is a more pythonic way of doing this
            #if db.retrieve_variable("long_name",var.long_name) should check emptiness but something is going wrong
            #I'm just leaving this working before I go mad but #FIXME later
            #Post-fixme update - comparisons are now checking for exactness. Two things are missing - 
            #   first should we be checking everything? Probably not, there will be some very similar variables we can group
            #   second these only included ordered lists which definitely needs to be changed - those are at least one example of similar variables we can group
            querylist = []
            duplicate = True
            if var.long_name:
                querylist = db.retrieve_variable("long_name",var.long_name)
            if var.standard_name:
                querylist = db.retrieve_variable("standard_name",var.standard_name)
            if var.long_name and var.standard_name:
                querylist = db.retrieve_variable("long_name",var.long_name)+db.retrieve_variable("standard_name",var.standard_name)
            if querylist:
                for queryvar in querylist:
                    if var.cfdm_domain == queryvar.cfdm_domain and var.cfdm_size == queryvar.cfdm_size:
                        if DeepDiff(var.get_properties(verbosity=2)[1:],queryvar.get_properties(verbosity=2)[1:]):
                            duplicate=True
                        else:
                            duplicate = False
                    else:
                        duplicate = False
            else:   
                duplicate = False
            
            if not duplicate:
                db.session.add(var)

            for m, cm in v.cell_methods().items():
                for a in cm.get_axes(): 
                    method = cm.get_method()
                    dbmethod = db.cell_method_get_or_make(axis=a, method=method)
                    dbmethod.used_in.append(var)
            db.session.commit()
        print(dbfiles)

        #store a variable in collection


class RemotePosix(Posix):
    """
    Stores a view of a remote POSIX file system using pysftp
    """
    def configure(self, hostname, username, **kw):
        """
        Configure RemotePosix backend with hostname, username, and anything else needed by the
        SSH backend. Currently the ssh backend uses SSHlite, and expects to use a running SSH Agent,
        and so now keyword arguments are expected or used.
        """
        self.ssh = SSHlite(hostname, username)


    def _walk(self, path_to_collection_head, collection_head_name, subcollections, checksum, regex):
        """
        Walk a remote directory and populate the collection
        """
        # Useful: https://stackoverflow.com/questions/45653213/parallel-downloads-with-multiprocessing-and-pysftp
        if not hasattr(self, 'ssh'):
            raise ConnectionError('Posix has not been initialised')
        if checksum:
            raise ValueError('Cannot (ok, really we mean, will not) checksum remote files')
        if subcollections:
            raise NotImplementedError('No support for sub-collections as yet')
        if regex:
            raise NotImplementedError('No support for remote regex yet')

        files = self.ssh.get_files_and_sizes(path_to_collection_head, subcollections)
        dbfiles = [self._file2dict(f[0], f[1]) for f in files]
        self.db.upload_files_to_collection(self.location, collection_head_name, dbfiles)


