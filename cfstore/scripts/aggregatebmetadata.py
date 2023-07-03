import os
import json
import numpy as np
import cf

 
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

    for filename in os.listdir("{{fileinput}}"):
        #If should probably be a glob of some kind?
        if filename.endswith(".nca"):
            #Opens and empties matching files
            with open("{{homedir}}"+filename+"bmetadata.json","w") as writepath:
                print("")
            with open("{{homedir}}"+filename+"_variables_bmetadata.json","w") as writepath:
                print("")
            
            #Reads the fields from the file with cf
            #Alternatively cfdm can be used as such:
            #   cff = cfdm.read(filename)
        cff=cf.read("{{fileinput}}cn134o_1_mon__grid_T_195003-195003.nc",ignore_read_error=True,fmt="NETCDF",aggregate={"relaxed_units":True,"relaxed_identities":True})  
        # loop over fields in file (not the same as netcdf variables)
        variables=[]
        for v in cff:
            properties = v.properties()

            if ('standard_name' not in properties and 'long_name' not in properties):
                properties['long_name'] = v.identity
            name, long_name = v.get_property('standard_name', None), v.get_property('long_name', None)
            identity = v.identity()
            domain = v.domain._one_line_description()
            size = v.size
            files = list(v.get_filenames())
            var = {"identity":identity,"standard_name":name, "long_name":long_name, "cfdm_size":size, "cfdm_domain":domain, "in_files":files}
            for k,p in properties.items():
                if k not in ['standard_name','long_name']:
                    var[k] = manage_types(p) 
            
            variables.append(var)
        with open("{{homedir}}/"+"tempfile.json","w") as writepath:
            json.dump(variables,writepath)