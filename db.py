from sqlalchemy import Column, Integer, String, Unicode, Boolean, ForeignKey, Table, UnicodeText, MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, Session
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.ext.associationproxy import association_proxy

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


class ProxiedDictMixin:
    """Adds obj[key] access to a mapped class.

    This class basically proxies dictionary access to an attribute
    called ``_proxied``.  The class which inherits this class
    should have an attribute called ``_proxied`` which points to a dictionary.

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

    holds_files = relationship(
        "File",
        secondary=collection_files_associations,
        back_populates="in_collections")
    association_proxy('holds_files', 'files')

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
        return f'Collection <{self.name}> has  {self.volume/9e6}GB in {self.filecount} files'

    @property
    def filecount(self):
        return len(self.holds_files)

    @classmethod
    def with_property(self, key, value):
        return self.properties.any(key=key, value=value)


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


class File(Base):
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
    # V0.2 format = Column(String)
    in_collections = relationship(
        "Collection",
        secondary=collection_files_associations,
        back_populates="holds_files")

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
        self.engine = create_engine(conn_string or self.conn_string)
        Base.metadata.create_all(self.engine)
        self.connection = self.engine.connect()
        self.session = Session(bind=self.connection)


