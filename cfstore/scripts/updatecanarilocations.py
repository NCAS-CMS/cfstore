import json
import re

import numpy as np
from netCDF4 import Dataset

growncfa = Dataset(
    "/home/george/Documents/cfs/cfstore/cfstore/json/CANARI_2_cw342_atmos.nc",
    "a",
    format="NETCDF4",
)

replacementfiles = {"${tape}cw342_2_6hr_pt__195310-195310.nc": "${disc}"}


def back_prop(a, i, s):
    if i == 0:
        varval[i] = varval[i].replace("${tape}",s)
    elif "${tape}" in a[i]:
        varval[i] = varval[i].replace("${tape}",s)
        back_prop(a,i-1,s)

with open("cfstore/json/tapefiles.json", "w") as f:
    json.dump(replacementfiles, f)

with open("cfstore/json/tapefiles.json") as f:
    tapefiles = json.load(f)

k = 1
c = 1
for varkey, varval in growncfa.variables.items():
    if varkey.startswith("cfa_file"):
        varshape = varval[..., 0].shape
        varval = varval[..., 0].flatten().tolist()
        for v in range(len(varval) - 1):
            k += 1
            val = varval[v]
            if val in tapefiles:
                back_prop(varval, v, tapefiles[val])
        varval = np.reshape(varval, varshape)
        growncfa.variables[varkey][...,0] = varval

for varkey, varval in growncfa.variables.items():
    if varkey.startswith("cfa_file"):
        varshape = varval[..., 0].shape
        varval = varval[..., 0].flatten().tolist()
        print(varval[:-5])
