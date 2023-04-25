from django.urls import path
from . import views

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render

import os
from pathlib import Path

from cfstore import interface, cfdb
from cfstore.config import CFSconfig

from .forms import VariableSearchForm, SaveAsCollectionForm

def outputvar(var):
    """ Can't use the django built in coz not everything is a float.
    But we can use this to suppress 0.0 in a nice way."""

    if var.standard_name:
        return_string = var.standard_name
    elif var.long_name:
        return_string = var.long_name
    else:
        return_string = "id "+str(var.id)+"(which has no name for some reason)"+"<br>"
    return return_string

def index(request):
    return HttpResponse("Welcome to CFstore. You're at the polls index.")

def confirmdelete(request,collection):
    return render(request, "confirmdelete_view.html")

def deletecol(request,collection):
    db = CFSconfig().db
    db.delete_collection(collection_name=collection,force=True)
    collections = db.retrieve_collections()
    variable_search_form = VariableSearchForm()
    return HttpResponseRedirect("..")
    return render(request, "index_view.html",{"collections":collections,"variable_search_form":variable_search_form})

def ls(request):
    db = CFSconfig().db
    collections = db.retrieve_collections()
    variable_search_form = VariableSearchForm()
    return render(request, "index_view.html",{"collections":collections,"variable_search_form":variable_search_form})

def lsbrowse(request):
    db = CFSconfig().db
    search=""
    search_method="fsdgs"
    collection_save_name=""
    if request.method == "POST":
        variable_search_form = VariableSearchForm(request.POST)
        collection_save_form = SaveAsCollectionForm(request.POST)
        if "variablename" in request.POST:
            search = request.POST["variablename"]
        else:
            variable_search_form = VariableSearchForm()
        if "collectionname" in request.POST:
            collection_save_name = request.POST["collectionname"]
        else:
            collection_save_form = SaveAsCollectionForm
    else:
        variable_search_form = VariableSearchForm()
        collection_save_form = SaveAsCollectionForm()
       

    collections = db.retrieve_collections()

    if search:
        search = search.split(":")
        for s in search:
            try:
                try:
                    try:
                        var = db.retrieve_variable("standard_name",s)
                        collections = collections.filter(files__in=var.in_files.all())
                    except:
                        var = db.retrieve_variable("long_name",s)
                        collections = collections.filter(files__in=var.in_files.all())

                    search_method = "Exact match found"
                except:
                    try:
                        var = db.search_variable("standard_name",s)
                        collections = collections.filter(files__in=var.in_files.all())
                    except:
                        var = db.search_variable("long_name",s)
                        collections = collections.filter(files__in=var.in_files.all())

                    search_method = "Partial match found"
            except:
                return render(request, "no_result_view.html")
            
    collections = collections.distinct()
    if collection_save_name:
        db.save_as_collection(collections,collection_save_name)

    print("search",search_method)
    if search_method:
        return render(request, "index_view.html",{"collections":collections,"variable_search_form":variable_search_form,"collection_save_form":collection_save_form,"search_method":search_method})

    return render(request, "index_view.html",{"collections":collections,"variable_search_form":variable_search_form,"collection_save_form":collection_save_form})


def lscol(request,page="all"):
    db = CFSconfig().db
    variables = db.retrieve_variables_in_collection(page)
    collection = db.retrieve_collection(page)
    files = db.retrieve_files_in_collection(page)
    if len(files)>10:
        displayfiles=files[0:10]
    else:
        displayfiles=files
    return render(request,"collections_view.html",{'variables':variables,'collection':collection,'filecount':len(files),'varcount':len(variables),'files':displayfiles, 'displayed':len(displayfiles)})


def lsvar(request,var="all"):
    db = CFSconfig().db
    variable = db.retrieve_variable("standard_name",var)
    if not variable:
        variable = db.retrieve_variable("long_name",var)
    collections = db.show_collections_with_variable(variable)
    return render(request, "variables_view.html",{'variable':variable,'collections':collections,'colcount':len(collections)})
