import cfdm
from cfstore.interface import CollectionDB
from cfstore.db import Variable, CellMethod
from pathlib import Path

def cfparse_file(db, filename):
    """  
    Parse a file and load cf metadata into the database 
    :Parameters:
        db: `CollectionDB` 
            an instance of a collection database
        filename: `str`
            filename which will be parsed
    :Returns:
        None
    **Examples:**
    >>> cfparse_file(db, 'my_model_file.nc')
    """

    cff = cfdm.read(filename)
    # loop over fields in file (not the same as netcdf variables)
    for v in cff:
        properties = v.properties()

        if ('standard_name' not in properties and 'long_name' not in properties):
            properties['long_name'] = v.identity
        name, long_name = v.get_property('standard_name', None), v.get_property('long_name', None)
        var = Variable(standard_name=name, long_name=long_name)
        for k,p  in properties.items():
            if k not in ['standard_name','long_name']:
                var[k] = p 
        db.session.add(var)
        for m, cm in v.cell_methods().items():
            for a in cm.get_axes(): 
                method = cm.get_method()
                dbmethod = db.cell_method_get_or_make(axis=a, method=method)
                dbmethod.used_in.append(var)
        db.session.commit()

if __name__=="__main__":
    filename = Path(__file__).parent /  'data/file.nc'
    db = CollectionDB()  
    db.init('sqlite://')
    cfparse_file(db, filename)
