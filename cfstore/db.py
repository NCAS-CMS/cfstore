from sqlalchemy import Column, Integer, String, Unicode, Boolean, ForeignKey, Table, UnicodeText, MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, Session, backref
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


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


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
    # could serialise json into value if necessaryprint(
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

    def serialise(self, target='dict'):
        """
        Serialise to a particular target format. Currently the only target understood is "dict"
        which is suitable for use in json.
        """
        assert target == 'dict', "Collection can only be serialised to a python dictionary"
        blob = {x: getattr(self, x) for x in ['name', 'description', 'volume', 'filecount']}
        blob['tags'] = [str(k) for k in self.tags]
        blob['related'] = "[relationships not yet serialised]"
        return blob

    @property
    def view(self):
        """ Extended string view suitable for printing """
        blob = self.serialise(target='dict')
        result = str(self) + ''.join(
            [f'\n__{x.capitalize()}__ \n{blob[x]}' for x in ['description', 'tags', 'related']])
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


class StorageLocation(Base):
    """
    Holds the list of available storage locations.
    """
    __tablename__ = "locations"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    volume = Column(Integer)

    holds_files = relationship('File',
                               secondary=storage_files_associations,
                               back_populates='replicas')
    association_proxy('holds_files', 'files')

    def __repr__(self):
        return self.name

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

if __name__=="__main__":
    from eralchemy import render_er
    render_er(Base,'cfstore-db-dbview.png')
