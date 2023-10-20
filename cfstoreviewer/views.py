from django.http import FileResponse, HttpResponse, HttpResponseRedirect
from django.shortcuts import render

from cfstore.config import CFSconfig

from .forms import (CollectionSearchForm, SaveAsCollectionForm,
                    VariableSearchForm, VariableBrowseForm)


def outputvar(var):
    """Can't use the django built in coz not everything is a float.
    But we can use this to suppress 0.0 in a nice way."""

    return var.identity


def index(request):
    return HttpResponse("Welcome to CFstore. You're at the polls index.")


def confirmdelete(request, collection):
    return render(request, "confirmdelete_view.html")


def deletecol(request, collection):
    db = CFSconfig().db
    db.delete_collection(collection_name=collection, force=True)
    collections = db.retrieve_collections()
    variable_search_form = VariableSearchForm()
    return HttpResponseRedirect("..")
    return render(
        request,
        "index_view.html",
        {"collections": collections, "variable_search_form": variable_search_form},
    )


def ls(request):
    db = CFSconfig().db
    collections = db.retrieve_collections()
    variable_search_form = VariableSearchForm()
    collection_search_form = CollectionSearchForm()
    variable_browse_form = VariableBrowseForm()
    return render(
        request,
        "index_view.html",
        {
            "collections": collections,
            "variable_search_form": variable_search_form,
            "collection_search_form": collection_search_form,
            "variable_browse_form": variable_browse_form,
        },
    )


def lsbrowse(request):
    db = CFSconfig().db
    search_method = ""
    collection_save_name = ""
    varsearch = []
    colsearch = []
    checks = {}

    if request.method == "POST":
        print(request.POST.keys())
        variable_search_form = VariableSearchForm(request.POST)
        collection_search_form = CollectionSearchForm(request.POST)
        collection_save_form = SaveAsCollectionForm(request.POST)
        checks = [ v for k, v in request.POST.items() if 'check' in k]

        if "variablename" in request.POST:
            varsearch.append(request.POST["variablename"])
        else:
            variable_search_form = VariableSearchForm()
        if "collection_searchname" in request.POST:
            colsearch.append(request.POST["collection_searchname"])
        else:
            collection_search_form = CollectionSearchForm()
        if "collectionname" in request.POST:
            collection_save_name = request.POST["collectionname"]
        else:
            collection_save_form = SaveAsCollectionForm
    else:
        variable_search_form = VariableSearchForm()
        collection_search_form = CollectionSearchForm()
        collection_save_form = SaveAsCollectionForm()

    collections = db.retrieve_collections()

    if varsearch:
        for s in varsearch:
            if collections:
                var = db.retrieve_variable("standard_name", s)
                if var.exists():
                    collections = collections.filter(files__in=var.in_files.all())
                else:
                    var = db.retrieve_variable("long_name", s)
                if var.exists():
                    collections = collections.filter(files__in=var.in_files.all())
                    search_method = "Exact match found"
                else:
                    var = db.search_variable("standard_name", s)
                    if var.exists():
                        collections = collections.filter(files__in=var.in_files.all())
                    else:
                        var = db.search_variable("long_name", s)
                    if var.exists():
                        collections = collections.filter(files__in=var.in_files.all())
                        search_method = "Partial match found"
                    else:
                        return render(request, "no_result_view.html")
            else:
                return render(request, "no_result_view.html")

    if colsearch:
        for s in colsearch:
            if collections:
                collections = collections.filter(name__contains=s)
            else:
                return render(request, "no_result_view.html")
        if not collections:
            return render(request, "no_result_view.html")

    collections = collections.distinct()
    if collection_save_name:
        db.save_as_collection(collections, collection_save_name)

    if search_method:
        return render(
            request,
            "browse_view.html",
            {
                "collections": collections,
                "variable_search_form": variable_search_form,
                "collection_search_form": collection_search_form,
                "collection_save_form": collection_save_form,
                "search_method": search_method,
            },
        )

    if request.method == "POST":
        print(request.POST)

    print(checks)

    return render(
        request,
        "index_view.html",
        {
            "collections": collections,
            "variable_search_form": variable_search_form,
            "collection_search_form": collection_search_form,
            "collection_save_form": collection_save_form,
            "checks": checks,
        },
    )


def lscol(request, page="all"):
    db = CFSconfig().db
    variables = db.retrieve_variables_in_collection(page)
    collection = db.retrieve_collection(page)
    files = db.retrieve_files_in_collection(page)
    locations = {}
    for f in files:
        for loc in f.location_set.distinct():
            if loc.name not in locations:
                locations[loc.name] = 1
            else:
                locations[loc.name] += 1
    if len(files) > 10:
        displayfiles = files[0:10]
    else:
        displayfiles = files
    return render(
        request,
        "collections_view.html",
        {
            "variables": variables,
            "collection": collection,
            "filecount": len(files),
            "varcount": len(variables),
            "files": displayfiles,
            "displayed": len(displayfiles),
            "locations": locations,
        },
    )


def downloadcol(request, page="all"):
    db = CFSconfig().db
    files = db.retrieve_files_in_collection(page)
    filenames = (f.path + "/" + f.name + "\n" for f in files if f.name.endswith(".nc"))
    return FileResponse(filenames, as_attachment=True, filename="Export.txt")


def lsvar(request, var="all"):
    db = CFSconfig().db
    var = var.replace(" ", "_")
    variables = db.retrieve_all_variables("identity", var)
    print(variables)
    return render(
        request,
        "variables_view.html",
        {
            "variables": variables
        },
    )
