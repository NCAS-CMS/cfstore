import os

import cf

if __name__ == "__main__":
    outputdicts = []

    # Reads the fields from the file with cf
    # Alternatively cfdm can be used as such:
    #   cff = cfdm.read(filename)

    # If you want to configure the aggregation change this variable
    aggregate = {
        # "relaxed_units": True, (not needed if we're only looking at NetCDF files)
        "relaxed_identities": True,
        "exclude": False,
        "concatenate": False,
        "cells": cf.climatology_cells(),
        "contiguous": True,
    }

    cff = cf.read(
        "{{fileinput}}",
        ignore_read_error=True,
        fmt="NETCDF",
        aggregate=aggregate,
        recursive=True,
        followlinks=True,
        chunks=None,
    )

    writepath = "{{homedir}}/canaricfas{{fileinput}}.cfa"
    os.umask(0)
    os.makedirs("{{homedir}}/canaricfas{{filepath}}", mode=0o777, exist_ok=True) 
    os.chmod("{{homedir}}/canaricfas{{filepath}}", mode=0o777)
    os.umask(0)
    cf.write(cff, cfa={"strict": False}, filename=writepath)
