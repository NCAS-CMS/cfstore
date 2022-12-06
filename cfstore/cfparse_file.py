import cfdm
from cfstore.db import Variable
import numpy as np
import os
from deepdiff import DeepDiff
import time


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

        for file in v.get_filenames():
            for f in db.retrieve_files_which_match(os.path.basename(file)):
                var.in_files.append(f)

        #there is a more pythonic way of doing this
        #if db.retrieve_variable("long_name",var.long_name) should check emptiness but something is going wrong
        #I'm just leaving this working before I go mad but #FIXME later
        #Post-fixme update - comparisons are now checking for exactness. Two things are missing - 
        #   first should we be checking everything? Probably not, there will be some very similar variables we can group
        #   second these only included ordered lists which definitely needs to be changed - those are at least one example of similar variables we can group
        querylist = []
        duplicate = True
        if var.long_name:
            querylist = db.retrieve_variable("long_name",var.long_name)
        if var.standard_name:
            querylist = db.retrieve_variable("standard_name",var.standard_name)
        if var.long_name and var.standard_name:
            querylist = db.retrieve_variable("long_name",var.long_name)+db.retrieve_variable("standard_name",var.standard_name)
        if querylist:
            for queryvar in querylist:
                if var.cfdm_domain == queryvar.cfdm_domain and var.cfdm_size == queryvar.cfdm_size:
                    if DeepDiff(var.get_properties(verbosity=2)[1:],queryvar.get_properties(verbosity=2)[1:]):
                        duplicate=True
                    else:
                        duplicate = False
                else:
                    duplicate = False
        else:   
            duplicate = False
        
        if not duplicate:
            db.session.add(var)

        for m, cm in v.cell_methods().items():
            for a in cm.get_axes(): 
                method = cm.get_method()
                dbmethod = db.cell_method_get_or_make(axis=a, method=method)
                dbmethod.used_in.append(var)
        db.session.commit()
