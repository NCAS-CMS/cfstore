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
    name = Column(Unicode)
    description = Column(UnicodeText)
    filecount = Column(Integer)
    volume = Column(Integer)

    holds_files = relationship(
        "File",
        secondary=collection_files_associations,
        back_populates="in_collections")

    # collections which correspond to uploaded batches, and which cannot be
    # deleted unless there are no references to the files within it elsewhere
    batch = Column(Boolean)

    tags = relationship("Tag",
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
        return f'<Collection {self.name} {self.volume/9e6}GB  in {self.filecount} files'

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


class File(Base):
    """
    Representation of a file
    """
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    path = Column(String)
    checksum = Column(String)
    initial_collection = Column(Integer, ForeignKey('collections.id'))
    in_collections = relationship(
        "Collection",
        secondary=collection_files_associations,
        back_populates="holds_files")


class CollectionsDB:
    """ Provides the interface to these tables """
    connection = None
    engine = None
    conn_string = None
    collections = []
    metadata = MetaData()

    def init(self, conn_string):
        self.engine = create_engine(conn_string or self.conn_string)
        Base.metadata.create_all(self.engine)
        self.connection = self.engine.connect()
        self.session = Session(bind=self.connection)

    def add_collection(self, collection_name, description, keyvalues={}):
        """ Add a collection and any properties, and return instance """
        c = Collection(name=collection_name, description=description)
        for k in kw:
            c[k] = kw[k]
        self.session.add(c)
        self.session.commit()
        return c

    def upload_file_to_collection(self, collection , name, path, checksum):
        """ Add to file details to collection """
        f = File(name=name, path=path, checksum=checksum, initial_collection=collection.id)
        self.session.add(f)
        self.session.commit()

    @property
    def tables(self):
        return self.engine.table_names()


def upload_files_to_collection(db, collection, files):
    """
    Add new files to a collection
    :param session:
    :param collection:
    :param files: list of file tuples [(name, checksum),...]
    :return: None
    """
    for f in files:
        # FIXME: Haven't properly added it into collections I don't think
        full_name = f[0]
        try:
            checksum = f[1]
        except IndexError:
            checksum = ''
        path, name = os.path.split(f)
        db.upload_file_to_collection(collection, name, path, checksum)


if __name__ == "__main__":

    db = CollectionsDB()
    db.init('sqlite:///memory:')
    print(db.tables)

    kw = {'resolution': 'N512', 'workspace': 'testws'}
    c = db.add_collection('mrun1', 'no real description', kw)

    files = [f'/somewhere/in/unix_land/file{i}' for i in range(10)]
    upload_files_to_collection(db, c, files)