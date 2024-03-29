import os

import cf

if __name__ == "__main__":
    outputdicts = []

    for filename in os.listdir("{{fileinput}}"):
        # If should probably be a glob of some kind?
        if filename.endswith(".nca"):
            # Opens and empties matching files
            with open("{{homedir}}" + filename + "bmetadata.json", "w") as writepath:
                print("")
            with open(
                "{{homedir}}" + filename + "_variables_bmetadata.json", "w"
            ) as writepath:
                print("")

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
        chunks=None,
    )

    print('Ran cf.read("{{fileinput}}",**{{aggregate}})')
    print("With the following settings for aggregate:")
    print("aggregate=**{{aggregate}}")
    print("cff length:", len(cff))
    writepath = "{{homedir}}/" + "tempfile.cfa"

    cf.write(cff, cfa={"strict": False}, filename=writepath)