import configparser, os, io
from pathlib import Path
from cfstore.interface import CollectionDB


class CFSconfig:
    """
    Handles interaction with configuration
    """
    def __init__(self, filename=None):
        """
        Initialise configuration from file name provided, or from configuration file, or
        from file found in default location (~/.csfstore/cfstore.ini). If it doesn't exist
        create a default configuration.
        """
        if not filename:
            filename = os.getenv('CFS_CONFIG_FILE', None)
            if not filename:
                cfdir = Path.home()/'.cfstore'
                print(cfdir)
                if not cfdir.exists():
                    os.mkdir(cfdir)
                self.filepath = cfdir/'cfstore.ini'
            else:
                self.filepath = Path(filename)
        else:
            self.filepath = Path(filename)
        self.config = configparser.ConfigParser()
        if not self.filepath.exists():
            self.config.read_file(io.StringIO(self._default()))
            with open(self.filepath,'w') as fp:
                self.config.write(fp)
        else:
            self.config.read(self.filepath)

        self._db = CollectionDB()
        self._db.init(self.conn_string)

    @property
    def conn_string(self):
        """ Return what the current configuration thinks is the connection string"""
        return self.config['_DB']['db_protocol'] + self.config['_DB']['db']

    @property
    def interfaces(self):
        return [x for x in self.config.sections() if x.find('template') == -1]

    def get_template(self, fstype):
        """
        Return the list of types understood by the configuration file
        """
        try:
            return self.config[f'template_{fstype}']
        except KeyError:
            raise ValueError('Unknown value of location fstype - ', fstype)

    @property
    def collection(self):
        return self.config['_DB']['last_collection']

    @property
    def name(self):
        return self.config['_DB']['db']

    @property
    def db(self):
        if self._db:
            if not self.conn_string == self._db.conn_string:
                self._db = CollectionDB()
                self._db.init(self.conn_string)
        else:
            self._db = CollectionDB()
            self._db.init(self.conn_string)
        return self._db

    def __setitem__(self, key, value ):
        if key in self.config['_DB']:
            self.config['_DB'][key] = value
        else:
            raise ValueError(f'Attempt to set non existent configuration option {key}')

    def __getitem__(self, key):
        return self.config['_DB'][key]

    def add_location(self, fstype, location, **kw):
        """
        Add new interface details (expressed using keywords) for a given <location> of type <fstype>.
        <fstype> must be understood by the configuration.
        Will overwrite existing location if it exists!
        """
        if location in self.interfaces:
            raise ValueError(f'WARNING: Interface {location} already exists')
        template = self.get_template(fstype)
        try:
            assert set(kw.keys()) == set(template.keys()) - set(['fstype',])
        except AssertionError:
            raise ValueError(f'Improper configuration for fstype {fstype}')
        self.config[location] = kw
        self.config[location]['fstype'] = fstype

    def get_location(self, location):
        return self.config[location]

    def save(self):
        with open(self.filepath,'w') as fp:
            self.config.write(fp)

    def _default(self):
        """ Return a default configuration file"""
        return '''
# cfstore configuration file
# do not edit anything except the _DB section

[_DB]
db = cfstoresqlalchemy.db
db_protocol = sqlite:///
last_collection =  
last_location = 

[local]
fstype = Posix

[et]
fstype = ElasticTape
gws = 

[template_rp]
fstype = RemotePosix
host = 
user = 

[template_S3]
fstype: S3
tbd: 
'''








