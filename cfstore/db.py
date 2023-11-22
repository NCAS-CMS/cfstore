from ast import For, In
from xml.etree.ElementTree import canonicalize
import django
from django.db import models
from cfstoreviewer.models import (
    Collection,
    File,
    Tag,
    Location,
    Protocol,
    Cell_Method,
    Variable,
    Relationship,
)
from cfstore.parse_cell_methods import parse_cell_methods
from django import template
# sqlalchemy relationships etc:
# https://docs.sqlalchemy.org/en/13/orm/basic_relationships.html
# see also https://docs.sqlalchemy.org/en/13/_modules/examples/vertical/dictlike.html
# https://docs.sqlalchemy.org/en/13/orm/extensions/associationproxy.html

# Would like to add "ubercollection" to be associated with a GWS and a user.
# Need to be able to total the unique volumes associated with eash user and GWS.
# This will need new tables and data counting on addinng and subtracting files.



def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


##
## A piece of sqlalchemy magic for subclassing the ability
## for simple vertical tables
##


register = template.Library()

@register.filter
def get_obj_field(obj, key):
    return obj[key]

class CoreDB:
    """Provides the interface to these tables"""

    connection = None
    engine = None
    conn_string = None
    collections = []
    metadata = None #MetaData()
    session = None

    def init(self, conn_string):
        pass
        """self.engine = create_engine(conn_string)
        Base.metadata.create_all(self.engine)
        self.connection = self.engine.connect()
        self.session = Session(bind=self.connection)
        self.conn_string = conn_string"""

    def save(self):
        """
        Commit any changes to the db
        """
        self.session.commit()


if __name__ == "__main__":
    django.setup()
    pass