Database Structure
------------------

The database is constructed using the `sqlalchemy <https://sqlalchemy.org>`_
object relational mapper.

The database structure is:

.. image:: img/cfstore-db-dbview.png
    :width: 800
    :alt: Database Entity Relationship Diagram


The database should only be manipulated via the
interface provided, the structural information is only provided for
software maintenance purposes.

Note that we are following the CF conventions here, and assigning all
file metadata to variables (fields). This would cause redundnacy, but
we avoid that (we hope) by utilising variable metadata which is 
itself in a table, and that's a special sort of table which utilises
the notion of `polymorphic values <https://docs.sqlalchemy.org/en/14/_modules/examples/vertical/dictlike-polymorphic.html>`_.
The basic idea is that have typed attributes in a sparse matrix where many attributes will be in common but
that number is small per file ... and the number of possible values is large and they are typed.

See also the "Entity-Attribute-Value" pattern discussion in `wikipedia <https://en.wikipedia.org/wiki/Entity%E2%80%93attribute%E2%80%93value_model>`_
