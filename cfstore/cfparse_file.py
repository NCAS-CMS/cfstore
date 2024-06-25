import os
import time

import cf
import numpy as np
from deepdiff import DeepDiff
from django.db import transaction

from cfstore.db import Collection, File, Variable


def manage_types(value):
    """
    The database only supports variable values which are boolean, int, string, and float.
    """
    if isinstance(value, str):
        return value
    elif isinstance(value, bool):
        return value
    elif isinstance(value, np.int32):
        return int(value)
    elif isinstance(value, int):
        return value
    elif isinstance(value, np.floating):
        return float(value)
    else:
        raise ValueError("Unrecognised type for database ", type(value))


def cfparse_file_to_collection(db, filename, collection,cffilelocation):
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
    print("reading ",filename)
    fullstart = time.time()
    cff = cf.read(filename)
    # loop over fields in file (not the same as netcdf variables)
    collection = db.retrieve_collection(collection)
    for v in cff:
        properties = v.properties()
        if "standard_name" not in properties and "long_name" not in properties:
            properties["long_name"] = v.identity
        name, long_name = v.get_property("standard_name", None), v.get_property(
            "long_name", None
        )
        realm = v.get_property("realm",None)

        print(realm,cffilelocation,filename, v.identity())
        domain = v.domain._one_line_description()
        size = v.size
        var = db.retrieve_or_make_variable(
            standard_name=name,
            long_name=long_name,
            identity=v.identity(),
            cfdm_size=size,
            cfdm_domain=domain,
            realm=realm,
            location=cffilelocation
        )
        var.save()

        var[properties["variant_id"]] = {}

        for k, p in properties.items():
            if k not in ["standard_name", "long_name"]:
                var[properties["variant_id"]][k] = manage_types(p)
        with transaction.atomic():
            through_files = File.objects.bulk_create(
                [
                    File(name=os.path.basename(bulkfilename), path=os.path.abspath(bulkfilename), size=0)
                    for bulkfilename in (v.get_filenames())
                ],
                ignore_conflicts=True,
            )
        var[properties["variant_id"]]["filenames"] = list(v.get_filenames())
        with transaction.atomic():
            Collection.files.through.objects.bulk_create(
                [
                    Collection.files.through(file_id=tf.pk, collection_id=collection.id)
                    for tf in through_files
                ],
                ignore_conflicts=True,
            )

        with transaction.atomic():
            Variable.in_files.through.objects.bulk_create(
                [
                    Variable.in_files.through(file_id=tf.pk, variable_id=var.id)
                    for tf in through_files
                ],
                ignore_conflicts=True,
            )

        # there is a more pythonic way of doing this
        # if db.retrieve_variable("long_name",var.long_name) should check emptiness but something is going wrong
        # I'm just leaving this working before I go mad but #FIXME later
        # Post-fixme update - comparisons are now checking for exactness. Two things are missing -
        #   first should we be checking everything? Probably not, there will be some very similar variables we can group
        #   second these only included ordered lists which definitely needs to be changed - those are at least one example of similar variables we can group
        querylist = []
        duplicate = False
        if var.long_name:
            querylist = db.retrieve_all_variables("long_name", var.long_name)
        if var.standard_name:
            querylist = db.retrieve_all_variables("standard_name", var.standard_name)
        if querylist:
            for queryvar in querylist:
                if not var.id == queryvar.id:
                    if var.cfdm_size == queryvar.cfdm_size:
                        if var.cfdm_domain == queryvar.cfdm_domain:
                            if DeepDiff(var._proxied, queryvar._proxied):
                                duplicate = True
                                continue
        var.save()
        db.add_variable_to_collection(collection.name, var)

        for m, cm in v.cell_methods().items():
            for a in cm.get_axes():
                method = cm.get_method()
                dbmethod = db.cell_method_get_or_make(axis=a, method=method)
                var._cell_methods[method] = (a, method)
        var.save()
    collection.save()
    print("LOOP", time.time() - fullstart)


def cfparse_file(db, filename, cffilelocation):
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
    cff = cf.read(filename)
    # loop over fields in file (not the same as netcdf variables)
    for v in cff:
        properties = v.properties()
        if "standard_name" not in properties and "long_name" not in properties:
            properties["long_name"] = v.identity
        name, long_name = v.get_property("standard_name", None), v.get_property(
            "long_name", None
        )
        realm = v.get_property("realm",None)

        print(realm,cffilelocation,filename, v.identity())
        domain = v.domain._one_line_description()
        size = v.size
        var = db.retrieve_or_make_variable(
            standard_name=name,
            long_name=long_name,
            identity=v.identity(),
            cfdm_size=size,
            cfdm_domain=domain,
            realm=realm,
            location=cffilelocation
        )
        var.save()

        var[properties["variant_id"]] = {}

        for k, p in properties.items():
            if k not in ["standard_name", "long_name"]:
                var[properties["variant_id"]][k] = manage_types(p)
        with transaction.atomic():
            through_files = File.objects.bulk_create(
                [
                    File(name=os.path.basename(bulkfilename), path=os.path.abspath(bulkfilename), size=0)
                    for bulkfilename in (v.get_filenames())
                ],
                ignore_conflicts=True,
            )
        var[properties["variant_id"]]["filenames"] = list(v.get_filenames())


        with transaction.atomic():
            Variable.in_files.through.objects.bulk_create(
                [
                    Variable.in_files.through(file_id=tf.pk, variable_id=var.id)
                    for tf in through_files
                ],
                ignore_conflicts=True,
            )

        # there is a more pythonic way of doing this
        # if db.retrieve_variable("long_name",var.long_name) should check emptiness but something is going wrong
        # I'm just leaving this working before I go mad but #FIXME later
        # Post-fixme update - comparisons are now checking for exactness. Two things are missing -
        #   first should we be checking everything? Probably not, there will be some very similar variables we can group
        #   second these only included ordered lists which definitely needs to be changed - those are at least one example of similar variables we can group
        querylist = []
        duplicate = False
        if var.long_name:
            querylist = db.retrieve_all_variables("long_name", var.long_name)
        if var.standard_name:
            querylist = db.retrieve_all_variables("standard_name", var.standard_name)
        if querylist:
            for queryvar in querylist:
                if not var.id == queryvar.id:
                    if var.cfdm_size == queryvar.cfdm_size:
                        if var.cfdm_domain == queryvar.cfdm_domain:
                            if DeepDiff(var._proxied, queryvar._proxied):
                                duplicate = True
                                continue
        var.save()

        for m, cm in v.cell_methods().items():
            for a in cm.get_axes():
                method = cm.get_method()
                dbmethod = db.cell_method_get_or_make(axis=a, method=method)
                var._cell_methods[method] = (a, method)
        var.save()
