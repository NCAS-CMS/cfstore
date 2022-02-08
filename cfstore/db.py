from ast import For, In
from sqlalchemy import Column, Integer, String, Unicode, Boolean, ForeignKey, Table, UnicodeText, MetaData, Float
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, Session, backref
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import event
from sqlalchemy import literal_column
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.interfaces import PropComparator

import os

from sqlalchemy.ext.declarative import declarative_base

# sqlalchemy relationships etc:
# https://docs.sqlalchemy.org/en/13/orm/basic_relationships.html
# see also https://docs.sqlalchemy.org/en/13/_modules/examples/vertical/dictlike.html
# https://docs.sqlalchemy.org/en/13/orm/extensions/associationproxy.html

#FIXME: Does not yet include B metadata support, although that could probably be done via collection properties.

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

class FileMetadata(PolymorphicVerticalProperty, Base):
    """
    Arbitrary key value pair associated with files 
    (B-Metadata in Lawrence et al 2008 terminology) ... 
    """
     # see https://docs.sqlalchemy.org/en/14/_modules/examples/vertical/dictlike-polymorphic.htmlfor heritage
    __tablename__ = "file_metadata"
    collection_id = Column(ForeignKey('File.id'), primary_key=True)

    # 128 characters would seem to allow plenty of room for "interesting" keys
    # could serialise json into value if necessary
    key = Column(Unicode(128), primary_key=True)
    type = Column(Unicode(16))
    json = Column(Boolean)

    # add information about storage for different types
    # in the info dictionary of Columns. We expect we will do
    # our own external serialisation for JSON into the char_value
    int_value = Column(Integer, info={"type": (int, "integer")})
    real_value = Column(Float, info={"type": (int, "integer")})
    char_value = Column(UnicodeText, info={"type": (str, "string")})
    

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

    # collections which correspond to uploaded batches, and which cannot be
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


class File(ProxiedDictMixin, Base):
    """
    Representation of a file
    """
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    path = Column(String)
    checksum = Column(String)
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

    # vertical table properties
    nc_attrs = relationship("FileMetadata",
                              collection_class=attribute_mapped_collection("key"))
    
    _proxied = association_proxy(
        "nc_attrs",
        "value",
        creator=lambda key, value: FileMetadata(key=key, value=value))    

    def __repr__(self):
        return os.path.join(self.path, self.name)

    @classmethod
    def with_nc_attr(self, key, value):
        return self.nc_attrs.any(key=key, value=value)


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

if __name__=="__main__":
    from eralchemy import render_er
    # NB, to run this, we need a patched version of eralchemy
    # I have 1.2.30 which needed a patch to work with sqlalchemy 1.4, see 
    # https://github.com/Alexis-benoist/eralchemy/issues/80
    # I have patched my library copy accordingly!
    render_er(Base,'cfstore-db-dbview.png')
