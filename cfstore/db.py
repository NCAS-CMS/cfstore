from ast import For, In
from xml.etree.ElementTree import canonicalize
from sqlalchemy import Column, Integer, String, Unicode, Boolean, ForeignKey, Table, UnicodeText, MetaData, Float, BigInteger
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, Session, backref
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import event, cast, case, null
from sqlalchemy import literal_column
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.interfaces import PropComparator

import os, re
from cfstore.parse_cell_methods import parse_cell_methods

from sqlalchemy.ext.declarative import declarative_base

# sqlalchemy relationships etc:
# https://docs.sqlalchemy.org/en/13/orm/basic_relationships.html
# see also https://docs.sqlalchemy.org/en/13/_modules/examples/vertical/dictlike.html
# https://docs.sqlalchemy.org/en/13/orm/extensions/associationproxy.html

# Would like to add "ubercollection" to be associated with a GWS and a user.
# Need to be able to total the unique volumes associated with eash user and GWS.
# This will need new tables and data counting on addinng and subtracting files.


Base = declarative_base()

collection_files_associations = Table(
    'collection_files_associations',
    Base.metadata,
    Column('collections_id', Integer, ForeignKey('collections.id'), primary_key=True),
    Column('files_id', Integer, ForeignKey('files.id'), primary_key=True)
)

collection_tags_associations = Table(
    'collection_tags_associations',
    Base.metadata,
    Column('collections_id', Integer, ForeignKey('collections.id'), primary_key=True),
    Column('tags_id',Integer, ForeignKey('tags.id'), primary_key=True)
)

storage_files_associations = Table(
    'storage_files_associations',
    Base.metadata,
    Column('locations_id', Integer, ForeignKey('locations.id'), primary_key=True),
    Column('files_id', Integer, ForeignKey('files.id'), primary_key=True)
)

relationship_associations = Table(
    'related_objects',
    Base.metadata,
    Column('relationship_id', Integer, ForeignKey('relationship.id'), primary_key=True),
    Column('objects_id', Integer, ForeignKey('collections.id'), primary_key=True)
)

location_protocol_associations = Table(
    'location_protocol_associations',
    Base.metadata,
    Column('locations_id', Integer, ForeignKey('locations.id'), primary_key=True),
    Column('protocol_id', Integer, ForeignKey('protocol.id'), primary_key=True)
)

var_files_associations = Table(
    'var_files_associations',
    Base.metadata,
    Column('files_id', Integer, ForeignKey('files.id'), primary_key=True),
    Column('variable_id', Integer, ForeignKey('variable.id'), primary_key=True)
)

var_cell_associations = Table(
    'var_cell_assocations',
    Base.metadata,
    Column('variable_id', Integer, ForeignKey('variable.id'), primary_key=True),
    Column('cell_method_id', Integer, ForeignKey('cell_method.id'), primary_key=True)
)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


##
## A piece of sqlalchemy magic for subclassing the ability 
## for simple vertical tables
##

class ProxiedDictMixin:
    """Adds obj[key] access to a mapped class.

    This class basically proxies dictionary access to an attribute
    called ``_proxied``.  The class which inherits this class
    should have an attribute called ``_proxied`` which points to a dictionary.
    This is a vertical table pattern where where we have one such vertical table 
    and there is only one type of attribute stored in that table.
    """

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]


##
## Following Magic is from sqllachmy docs
## Basic idea is we want to have typed attributes in a 
## sparse matrix as many attributes will be in common but
## that number is small per file ... and the number of
## possible values is large and they are typed.
##

class PolymorphicVerticalProperty(object):
    """A key/value pair with polymorphic value storage.

    The class which is mapped should indicate typing information
    within the "info" dictionary of mapped Column objects,
    See example at: https://docs.sqlalchemy.org/en/14/_modules/examples/vertical/dictlike-polymorphic.html
    See also concept "Entity-Attribute-Value" pattern discussion at h
    ttps://en.wikipedia.org/wiki/Entity%E2%80%93attribute%E2%80%93value_model

    """

    def __init__(self, key, value=None):
        self.key = key
        self.value = value

    @hybrid_property
    def value(self):
        fieldname, discriminator = self.type_map[self.type]
        if fieldname is None:
            return None
        else:
            return getattr(self, fieldname)

    @value.setter
    def value(self, value):
        py_type = type(value)
        fieldname, discriminator = self.type_map[py_type]

        self.type = discriminator
        if fieldname is not None:
            setattr(self, fieldname, value)

    @value.deleter
    def value(self):
        self._set_value(None)

    @value.comparator
    class value(PropComparator):
        """A comparator for .value, builds a polymorphic comparison
        via CASE."""

        def __init__(self, cls):
            self.cls = cls

        def _case(self):
            pairs = set(self.cls.type_map.values())
            whens = [
                (
                    literal_column("'%s'" % discriminator),
                    cast(getattr(self.cls, attribute), String),
                )
                for attribute, discriminator in pairs
                if attribute is not None
            ]
            return case(whens, value=self.cls.type, else_=null())

        def __eq__(self, other):
            return self._case() == cast(other, String)

        def __ne__(self, other):
            return self._case() != cast(other, String)

    def __repr__(self):
        return "<%s %r=%r>" % (self.__class__.__name__, self.key, self.value)

@event.listens_for(
    PolymorphicVerticalProperty, "mapper_configured", propagate=True
)

def on_new_class(mapper, cls_):
    """Look for Column objects with type info in them, and work up
    a lookup table."""

    info_dict = {}
    info_dict[type(None)] = (None, "none")
    info_dict["none"] = (None, "none")

    for k in mapper.c.keys():
        col = mapper.c[k]
        if "type" in col.info:
            python_type, discriminator = col.info["type"]
            info_dict[python_type] = (k, discriminator)
            info_dict[discriminator] = (k, discriminator)
    cls_.type_map = info_dict

###
### Sqlalchemy magic concludes
###


#
# Variable Tables:
#

class VariableMetadata(PolymorphicVerticalProperty, Base):
    """
    Arbitrary key value pair associated with variables
    (B-Metadata in Lawrence et al 2008 terminology) ... 
    Following CF conventions, all variable metadata is both variable
    specific and inherited from file globals.
    """
     # see https://docs.sqlalchemy.org/en/14/_modules/examples/vertical/dictlike-polymorphic.html for heritage
    __tablename__ = "var_metadata"
    collection_id = Column(ForeignKey('variable.id'), primary_key=True)

    # 128 characters would seem to allow plenty of room for "interesting" keys
    # could serialise json into value if necessary
    key = Column(Unicode(128), primary_key=True)
    type = Column(Unicode(16))
    json = Column(Boolean)

    # add information about storage for different types
    # in the info dictionary of Columns. We expect we will do
    # our own external serialisation for JSON into the char_value
    boolean_value = Column(Boolean, info={"type": (bool, "boolean") })
    int_value = Column(BigInteger, info={"type": (int, "integer")})
    real_value = Column(Float, info={"type": (float, "float")})
    char_value = Column(UnicodeText, info={"type": (str, "string")})

    def __repr__(self):
        if self.char_value:
            return f'{self.char_value}'
        if self.real_value:
            return f'{self.real_value}'
        if self.int_value:
            return f'{self.int_value}'
        if self.boolean_value:
            return f'{self.boolean_value}'

class CellMethod(Base):
    """ 
    Collection of possible cell methods
    """
    __tablename__ = "cell_method"
    id = Column(Integer, primary_key=True)
    axis = Column(String)
    method = Column(String)

    used_in = relationship(
        "Variable",
        secondary=var_cell_associations,
        back_populates="_cell_methods"
    )

    def __repr__(self):
        return f'{self.axis} : {self.method}'

class Variable(ProxiedDictMixin, Base):
    """
    Representation of a Variable in the database.

    Note that the various attributes are handled differently, via different interfaces,
    which is probably going to be horrible to comprehend higher up the stack.
    It is most likely we will want to have another class which handles all the 
    attributes in the same way, so that the differnce between, say X.cell_methods = 'adsf'
    and X['file_count'] = 1 which is necessary to the database layer is hidden
    from the user higher up (we should handle both the same way in user facing classes).

    """
    __tablename__="variable"

    id = Column(Integer, primary_key=True)
    standard_name = Column(String)
    long_name = Column(String)
    cfdm_size = Column(BigInteger)
    cfdm_domain = Column(String)

    in_files = relationship(
        "File",
        secondary=var_files_associations,
        back_populates="variables"
    )

    _cell_methods = relationship(
        "CellMethod",
        secondary=var_cell_associations,
        back_populates="used_in"
    )

    other_attributes = relationship(
        "VariableMetadata", collection_class=attribute_mapped_collection("key")
    )
    
    _proxied = association_proxy(
        "other_attributes",
        "value",
        creator = lambda key, value: VariableMetadata(key=key, value=value),
    )

    def __init__(self, standard_name=None, long_name=None, cfdm_size=0, cfdm_domain=''):
        """ Ensure either longname or cf_name is provided"""
        if standard_name is None and long_name is None:
            print("This_variable_was_not_assigned_either_a_long_or_standard_name")
            long_name = "This_variable_was_not_assigned_either_a_long_or_standard_name"
        super(Variable, self).__init__(standard_name=standard_name,long_name=long_name, cfdm_size=cfdm_size, cfdm_domain=cfdm_domain)

    def __repr__(self,verbosity=0):
        if verbosity==0:
            if self.standard_name:
                return self.standard_name
            else:
                return self.long_name
    
    def get_properties(self,verbosity=0):
        if verbosity==0:
            if self.standard_name:
                return self.standard_name
            else:
                return self.long_name

        if verbosity==1:
            if self.standard_name:
                name = self.standard_name
            else:
                name =  self.long_name
            return [self.id,name,self.cfdm_size,self.cfdm_domain,self.in_files]

        if verbosity==2:    
            if self.standard_name:
                name = self.standard_name
            else:
                name =  self.long_name
            return [self.id,name,self.cfdm_size,self.cfdm_domain,self.in_files,self.other_attributes]



    def __setattr__(self, key, value):
        if key == 'cell_methods':
            mdict = parse_cell_methods(self,value)
            for m in mdict:
                for a in m['axes']:
                    cm = CellMethod(axis=a, method=m['method'])
                    self._cell_methods.append(cm)
        else:
            super().__setattr__(key, value)

    @classmethod
    def with_other_attributes(self, key, value):
        return self.other_attributes.any(key=key,value=value)

    @classmethod
    def is_equals(self,var):
        if self.standard_name==var.standard_name or self.long_name or var.long_name:
            if self.cfdm_domain==var.cfdm_domain and self.cfdm_size == var.cfdm_size:
                if [prop in self.get_properties for prop in var.get_properties]:
                    return True
        return False

#
# Collection Tables
#

class CollectionProperty(Base):
    """
    Arbitrary key value pair associated with collections.
    """
    # see https://docs.sqlalchemy.org/en/13/_modules/examples/vertical/dictlike.html for heritage
    __tablename__ = "collection_properties"
    collection_id = Column(ForeignKey('collections.id'), primary_key=True)

    # 128 characters would seem to allow plenty of room for "interesting" keys
    # could serialise json into value if necessary
    key = Column(Unicode(128), primary_key=True)
    value = Column(UnicodeText)


class Collection(ProxiedDictMixin, Base):
    """
    The key concept of cftape is that files are organised into one or more collections.
    """
    __tablename__ = 'collections'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, unique=True)
    description = Column(UnicodeText)
    volume = Column(Integer)

    # Use the triple mechanism to hold a relationship (the method which creates
    # relationships is responsible for sorting out the backward relationships, if any.)
    related = association_proxy(
        'collection_relationships', 'objects',
        creator=lambda p, o: Relationship(predicate=p, objects=o))

    holds_files = relationship(
        "File",
        secondary=collection_files_associations,
        back_populates="in_collections")
    #association_proxy('holds_files', 'files')

    # collections which correspond to #ed batches, and which cannot be
    # deleted unless there are no references to the files within it elsewhere
    batch = Column(Boolean)

    tags = relationship(
        "Tag",
        secondary=lambda: collection_tags_associations,
        back_populates="in_collections")

    # vertical table properties
    properties = relationship("CollectionProperty",
                              collection_class=attribute_mapped_collection("key"))
    _proxied = association_proxy(
        "properties",
        "value",
        creator=lambda key, value: CollectionProperty(key=key, value=value))

    def __repr__(self):
        if not self.volume:
            self.volume = 0
        return f'Collection <{self.name}> has  {sizeof_fmt(self.volume)} in {self.filecount} files'

    def add_relationship(self, predicate, object):
        if predicate in self.related:
            self.related[predicate].append(object)
        else:
            self.related[predicate] = [object]

    @property
    def filecount(self):
        return len(self.holds_files)


    @classmethod
    def with_property(self, key, value):
        return self.properties.any(key=key, value=value)

    def serialise(self, target='dict'):
        """
        Serialise to a particular target format. Currently the only target understood is "dict"
        which is suitable for use in json.
        """
        assert target == 'dict', "Collection can only be serialised to a python dictionary"
        blob = {x: getattr(self, x) for x in ['name', 'description', 'volume', 'filecount']}
        blob['tags'] = [str(k) for k in self.tags]
        #blob['related'] = self.related
        #blob['holds_files'] = self.holds_files
        return blob

    @property
    def md(self):
        """
        Extended string view in markdown. The assumption is that the existing
        collection description is either text or markdown. We simply decorate it
        to ensure titles for the bits of information.
        """
        blob = self.serialise(target='dict')
        template = f"## Collection {blob['name']}\n{self}\n"
        result = template + ''.join(
            [f'\n__{x.capitalize()}__\n\n{blob[x]}\n\n' for x in ['description', 'tags', 'related']])
        return result

class Relationship(Base):
    __tablename__ = "relationship"
    id = Column(Integer, primary_key=True)
    # one relationship
    predicate = Column(String(50))
    subject_id = Column(Integer, ForeignKey('collections.id'))
    subject = relationship(Collection,
                           backref=backref("collection_relationships",
                                           collection_class=attribute_mapped_collection('predicate')),
                           foreign_keys="Relationship.subject_id")
    # many possible objects (targets)
    objects = relationship(Collection, secondary=relationship_associations)

    def __repr__(self):
        return f'{self.subject}-{self.predicate}-{self.objects}'


class Tag(Base):
    """
    User defined tags for collections
    """
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode(64), nullable=False, unique=True)

    in_collections = relationship('Collection',
                                  secondary=collection_tags_associations,
                                  back_populates='tags')
    def __repr__(self):
        return self.name


class StorageProtocol(Base):
    """
    Holds the list of available protocols
    """
    # Resisted the temptation to use an enum, as otherwise new enums (storage interfaces) 
    # would require client-side database upgrades and that sounds like a recipe for trouble.
    __tablename__ = "protocol"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    used_by = relationship('StorageLocation',
                           secondary=location_protocol_associations,
                           back_populates='protocols')
                           
    association_proxy('used_by','locations')

class StorageLocation(Base):
    """
    Holds the list of available storage locations.
    """
    __tablename__ = "locations"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    volume = Column(Integer, default=0)
    
    protocols = relationship('StorageProtocol',
                             secondary=location_protocol_associations,
                             back_populates='used_by')

    holds_files = relationship('File',
                               secondary=storage_files_associations,
                               back_populates='replicas')

    association_proxy('holds_files', 'files')

    def __repr__(self):
        return self.name

    @property
    def info(self):
        if not self.volume:
            self.volume = 0
        return f'Location <{self.name}> has  {sizeof_fmt(self.volume)} in {len(self.holds_files)} files'


class File(Base):
    """
    Representation of a file
    """
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    path = Column(String)
    checksum = Column(String)
    checksum_method = Column(String)
    size = Column(Integer)
    initial_collection = Column(Integer, ForeignKey('collections.id'))
    format = Column(String)

    replicas = relationship(
        'StorageLocation',
        secondary=storage_files_associations,
        back_populates='holds_files')

    in_collections = relationship(
        "Collection",
        secondary=collection_files_associations,
        back_populates="holds_files")

    variables = relationship(
        "Variable",
        secondary=var_files_associations,
        back_populates="in_files"
    )

    def __repr__(self):
        return os.path.join(self.path, self.name)




class CoreDB:
    """ Provides the interface to these tables """
    connection = None
    engine = None
    conn_string = None
    collections = []
    metadata = MetaData()
    session = None

    def init(self, conn_string):
        self.engine = create_engine(conn_string)
        Base.metadata.create_all(self.engine)
        self.connection = self.engine.connect()
        self.session = Session(bind=self.connection)
        self.conn_string = conn_string

    def save(self):
        """
        Commit any changes to the db
        """
        self.session.commit()


if __name__=="__main__":
    from eralchemy import render_er
    # NB, to run this, we need a patched version of eralchemy
    # I have 1.2.30 which needed a patch to work with sqlalchemy 1.4, see 
    # https://github.com/Alexis-benoist/eralchemy/issues/80
    # I have patched my library copy accordingly!
    render_er(Base,'cfstore-db-dbview.png')