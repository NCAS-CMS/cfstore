import os
import cfdm
import json
import numpy as np
from cfstore.db import Variable
from cfstore.config import CFSconfig

#I know this doesn't work
#It doesn't fail in a way that makes things annoying either

def as_dict(self):
    return {c.name: getattr(self, c.name) for c in self.__table__.columns}

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

if __name__ == '__main__':
    for filename in os.listdir(os.curdir):
        print(filename)
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
        variable_output_list = []
        if filename.split(".")[1]=="nc":
            with open(filename+"bmetadata.json","w") as writepath:
                print("")
            with open(filename+"_variables_bmetadata.json","w") as writepath:
                print("")
            cff = cfdm.read(filename)
            # loop over fields in file (not the same as netcdf variables)
            variables=[]
            for v in cff:
                with open(filename+"_variables_bmetadata.json","a") as writepath:
                    json.dump(str(v),writepath)
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
                var_dict = as_dict(var)
                variable_output_list.append(var_dict)
            print(len(variable_output_list))
            with open(filename+"bmetadata.json","a") as writepath:
                #json.dump("Standard name \'=\'"+str(var.standard_name)+"\n",writepath) 
                #json.dump("Long name \'=\'"+str(var.long_name)+"\n",writepath) 
                #json.dump("Size \'=\'"+str(var.cfdm_size)+"\n",writepath) 
                #json.dump("Domain \'=\'"+str(var.cfdm_domain)+"\n",writepath) 

                #for v in var.__dict__:
                #json.dump(str(v.cell_methods())+"\n",writepath)  
                json.dump(variable_output_list,writepath)

            