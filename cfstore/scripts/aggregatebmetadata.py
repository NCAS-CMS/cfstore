from asyncore import write
import os
import json
import numpy as np
import cf
from cfstore.db import Variable
from cfstore.config import CFSconfig


 
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
    outputdicts=[]

    for filename in os.listdir(os.curdir):
        variable_output_list = []
        #If should probably be a glob of some kind?
        if filename.endswith(".nca"):
            #Opens and empties matching files
            with open(filename+"bmetadata.json","w") as writepath:
                print("")
            with open(filename+"_variables_bmetadata.json","w") as writepath:
                print("")
            
            #Reads the fields from the file with cf
            #Alternatively cfdm can be used as such:
            #   cff = cfdm.read(filename)
            cff=(cf.read(filename))   
            x = cf.read(filename)
            # loop over fields in file (not the same as netcdf variables)
            variables=[]
    #cff = cf.aggregate(cff, verbose=0)
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
                print("Variable:")
                print(v)
                print("Properties:")
                print(v.data.dumpd())
                print("Filenames")
                print(v.get_filenames())
                print("____________________________________________")
                variable_output_list.append(var_dict)
            with open(filename.split(".")[0]+"bmetadata.json","a") as writepath:
                print(filename.split(".")[0]+"bmetadata.json")
                json.dump(variable_output_list,writepath)