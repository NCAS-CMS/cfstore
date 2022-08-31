import cfdm
from cfstore.db import Variable
import numpy as np


def manage_types(value):
    """ 
    The database only supports variable values which are boolean, int, string, and float. 
    """
    if isinstance(value, str):
        return value
    elif isinstance(value,bool):
        return value
    elif isinstance(value, np.int32):
        return int(value)
    elif isinstance(value, int):
        return value
    elif isinstance(value, np.floating):
        return float(value)
    else:
        raise ValueError('Unrecognised type for database ',type(value))



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
    print("Running cfparse_file")
    cff = cfdm.read(filename)
    # loop over fields in file (not the same as netcdf variables)
    for v in cff:
        properties = v.properties()

        if ('standard_name' not in properties and 'long_name' not in properties):
            properties['long_name'] = v.identity
        name, long_name = v.get_property('standard_name', None), v.get_property('long_name', None)

        domain = v.domain._one_line_description()
        size = v.size

        var = Variable(standard_name=name, long_name=long_name, cfdm_size=size, cfdm_domain=domain)
        for k,p in properties.items():
            if k not in ['standard_name','long_name']:
                var[k] = manage_types(p) 
        

        db.session.add(var)
        for m, cm in v.cell_methods().items():
            for a in cm.get_axes(): 
                method = cm.get_method()
                dbmethod = db.cell_method_get_or_make(axis=a, method=method)
                dbmethod.used_in.append(var)
        db.session.commit()
