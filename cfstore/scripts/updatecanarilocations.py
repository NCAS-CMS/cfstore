from netCDF4 import Dataset
import json
import re
import numpy as np

growncfa = Dataset("/home/george/Documents/cfs/cfstore/cfstore/json/19500101T0000Z_i.cfa", "a", format="NETCDF4")

replacementfiles = {"${unavailable}cn134i_999_mon_20141101-20141201.nc":"${tarpe}"}

def back_prop(a, i, s):
    if i == 0:
        varval[i] = varval[i].replace("${unavailable}",s)
    elif "${unavailable}" in a[i]:
        varval[i] = varval[i].replace("${unavailable}",s)
        print(varval[-10:])
        back_prop(a,i-1,s)

with open("cfstore/json/tapefiles.json","w") as f:
    json.dump(replacementfiles,f)

with open("cfstore/json/tapefiles.json") as f:
    tapefiles = json.load(f)

for varkey,varval in (growncfa.variables.items()):
    if varkey.startswith("cfa_file"):
        varshape = (varval[...,0].shape)
        varval=varval[...,0].flatten().tolist()
        for v in range(len(varval)-1):
            val = varval[v]
            if val in tapefiles:
                back_prop(varval,v,tapefiles[val])
        varval = np.reshape(varval,varshape)
        print(varval[-10:])

            
# cfa_file_1 =
#  "${unavailable}cn134i_999_mon_19500101-19500201.nc", _ ;