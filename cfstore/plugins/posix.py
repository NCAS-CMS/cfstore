import os
import re

import cf
import numpy as np

from cfstore import db
from cfstore.cfparse_file import cfparse_file
from cfstore.plugins.ssh import SSHlite


def manage_types(value):
    """
    The database only supports variable values which are boolean, int, string, and float.
    """
    if isinstance(value, str):
        return value
    elif isinstance(value, bool):
        return value
    elif isinstance(value, np.int32):
        return int(value)
    elif isinstance(value, int):
        return value
    elif isinstance(value, np.floating):
        return float(value)
    else:
        raise ValueError("Unrecognised type for database ", type(value))


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
            print(f"Using existing location ({location})")

    def add_collection(
        self,
        path_to_collection_head,
        collection_head_name,
        collection_head_description,
        subcollections=False,
        checksum=None,
        regex=None,
    ):
        """

        Add a new collection with all files below <path_to_collection_head>,
        and call that collection <collection_head_name>, and decorate with <collection_head_description> text.

        Optionally (<subcollections=True>), create sub-collections for all internal directories
        (default = False = do not create sub-collections). (NOT YET IMPLEMENTED) Not Implemented

        If checksums required, provide a checksum method string.
        (NOT YET IMPLEMENTED) Not Implemented

        """
        c = self.db.create_collection(collection_head_name, collection_head_description)
        args = [
            path_to_collection_head,
            collection_head_name,
            collection_head_description,
            subcollections,
            checksum,
            regex,
        ]
        keys = [
            "_path_to_collection_head",
            "_collection_head_name",
            "_collection_head_description",
            "_subcollections",
            "_checksum",
            "_regex",
        ]

        for n in range(len(args)):
            print(c._proxied, type(c._proxied))
            c[keys[n]] = args[n]
        self._walk(
            path_to_collection_head,
            collection_head_name,
            subcollections,
            checksum,
            regex,
        )
        return c

    def _walk(
        self,
        path_to_collection_head,
        collection_head_name,
        subcollections,
        checksum,
        regex,
    ):
        """Walk local POSIX tree"""
        if subcollections:
            for dirName, directories, files in os.walk(path_to_collection_head):
                dbfiles = []
                for f in files:
                    if not regex or re.match(regex, f):
                        fp = dirName + "/" + f
                        dbfiles.append(
                            self._file2dict(fp, os.stat(fp).st_size, checksum=checksum)
                        )
                self.db.upload_files_to_collection(
                    self.location, collection_head_name, dbfiles
                )
        else:
            dbfiles = []
            for file in os.walklevel(path_to_collection_head):
                if not regex or re.match(regex, file):
                    if dirName == path_to_collection_head:
                        fp = path_to_collection_head + "/" + file
                        dbfiles.append(
                            self._file2dict(
                                fp, os.stat(fp).st_size, checksum=checksum
                            )
                        )
                self.db.upload_files_to_collection(
                    self.location, collection_head_name, dbfiles
                )

    def walklevel(some_dir, level=1):
        some_dir = some_dir.rstrip(os.path.sep)
        assert os.path.isdir(some_dir)
        num_sep = some_dir.count(os.path.sep)
        for root, dirs, files in os.walk(some_dir):
            yield root, dirs, files
            num_sep_this = root.count(os.path.sep)
            if num_sep + level <= num_sep_this:
                del dirs[:]

    def getBMetadata(
        self, remotepath, collection, localpath, subcollections, checksum, regex
    ):
        """
        Currently runs a remote script
        Used in conjunction with "getBMetadata" script to aquire BMetadata from remote files
        #FIXME This just needs to be massively cleaned up
        """
        print("Getting b metadata")
        # files = self.ssh.get_files_and_sizes(path_to_collection_head, subcollections)
        # self.ssh.get_b_metadata(path_to_collection_head,self.db)
        self.ssh.run_script(remotepath, collection, localpath)

    def _file2dict(self, path_to_file, size, checksum=None):
        """
        Build the dictionary of file information needed for cfstore
        """
        p, n = os.path.split(path_to_file)
        if checksum is not None:
            raise NotImplementedError

        f = {"size": size, "path": p, "name": n}
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

    def aggregation_files_to_collection(self, aggfile, collection):
        """
        Uses a cf python aggregation file to add metadata variables to the appropriate files
        """
        # open file
        # read file into list of variables
        print("Adding variables from", aggfile)
        # aggfileobject = open(aggfile,'r')
        cff = cf.read(aggfile)
        print("Read!")
        # for each variable
        c = self.db.retrieve_collection(collection)

        # loop over fields in file (not the same as netcdf variables)
        for v in cff:
            properties = v.properties()
            cell_methods = v.cell_methods()
            cell_methods_unpacked = []
            if cell_methods:
                for cmethod in cell_methods.values():
                    axes = cmethod.axes
                    methods = cmethod.method
                    cell_method_dict = {"axes": axes, "methods": methods}
                    print(cell_method_dict)
                    cell_methods_unpacked.append(cell_method_dict)

            print("Starting")
            if "standard_name" not in properties and "long_name" not in properties:
                properties["long_name"] = v.identity()
            name, long_name = v.get_property("standard_name", None), v.get_property(
                "long_name", None
            )
            identity = v.identity()
            domain = v.domain._one_line_description()
            size = v.size
            # Maybe use shape? Would require back-end update

            managed_properties = {}
            for k, p in properties.items():
                if k not in ["standard_name", "long_name"]:
                    managed_properties[k] = manage_types(p)

                if "frequency" in managed_properties.keys():
                    if managed_properties["frequency"] == cf.D:
                        managed_properties["frequency"] = "Daily"
                    if managed_properties["frequency"] == cf.M:
                        managed_properties["frequency"] = "Monthly"
                    if managed_properties["frequency"] == cf.Y:
                        managed_properties["frequency"] = "Yearly"

            var, created = db.Variable.objects.get_or_create(
                identity=identity,
                standard_name=name,
                long_name=long_name,
                cfdm_size=size,
                cfdm_domain=domain,
                _proxied=managed_properties,
                _cell_methods=cell_methods_unpacked,
            )
            print(created)
            if not created:
                print("Variable already exists! Updating files")
            if c not in var.in_collection.all():
                var.in_collection.add(c)

            files = list(v.get_filenames())
            for file in files:
                (path, file) = os.path.split(file)
                try:
                    file = self.db.retrieve_file(path, file)
                    if file not in var.in_files.all():
                        var.in_files.add(file)
                    file.save()
                except FileNotFoundError:
                    try:
                        uc = (
                            self.session.query(db.Collection)
                            .filter_by(name="unlisted")
                            .one()
                        )
                    except:
                        uc = db.Collection(
                            name="unlisted",
                            volume=0,
                            description="Holds unlisted files",
                        )
                    uf = db.File(
                        name=file, path=path, checksum=0, size=0, format="unknown"
                    )
                    uf.save()
                    var.in_files.add(uf)

            print(var.id)
            print(var.keys())
            var.save()


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

    def _walk(
        self,
        path_to_collection_head,
        collection_head_name,
        subcollections,
        checksum,
        regex,
    ):
        """
        Walk a remote directory and populate the collection
        """
        # Useful: https://stackoverflow.com/questions/45653213/parallel-downloads-with-multiprocessing-and-pysftp
        if not hasattr(self, "ssh"):
            raise ConnectionError("Posix has not been initialised")
        elif checksum:
            raise ValueError(
                "Cannot (ok, really we mean, will not) checksum remote files"
            )
        elif subcollections:
            files = self.ssh.get_files_and_sizes(path_to_collection_head, subcollections)
            dbfiles = [self._file2dict(f[0], f[1]) for f in files]
            self.db.upload_files_to_collection(self.location, collection_head_name, dbfiles)
        elif regex:
            raise NotImplementedError("No support for remote regex yet")
        else:
            files = self.ssh.get_files_and_sizes(path_to_collection_head, subcollections)
            dbfiles = [self._file2dict(f[0], f[1]) for f in files]
            self.db.upload_files_to_collection(self.location, collection_head_name, dbfiles)
