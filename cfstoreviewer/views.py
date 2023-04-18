from django.urls import path
from . import views

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render

import os
from pathlib import Path

from cfstore import interface, cfdb
from cfstore.config import CFSconfig

from .forms import SearchForm

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

def ls(request):
    db = CFSconfig().db
    collections = db.retrieve_collections()
    form = SearchForm()
    return render(request, "index_view.html",{"collections":collections,"form":form})

def lsbrowse(request):
    db = CFSconfig().db
    if request.method == "POST":
        form = SearchForm(request.POST)
        search = request.POST["srch"]
    else:
        form = SearchForm()
        search=""
    
    collections = db.retrieve_collections()

    if search:
        search = search.split(":")
        for s in search:

            var = db.retrieve_variable("standard_name",s)
            if not var:
                var = db.retrieve_variable("long_name",s)
            if var.in_files:
                collections = collections.filter(files__in=var.in_files.all())
            if not collections:
                return render(request, "no_result_view.html")
    collections = collections.distinct()
    return render(request, "index_view.html",{"collections":collections,"form":form})


def lscol(request,page="all"):
    db = CFSconfig().db
    variables = db.retrieve_variables_in_collection(page)
    collection = db.retrieve_collection(page)
    files = db.retrieve_files_in_collection(page)
    
    return render(request,"collections_view.html",{'variables':variables,'collection':collection,'filecount':len(files),'varcount':len(variables)})


def lsvar(request,var="all"):
    db = CFSconfig().db
    try:
        variable = db.retrieve_variable("standard_name",var)
    except:
        variable = db.retrieve_variable("long_name",var)
    collections = db.show_collections_with_variable(variable)
    return render(request, "variables_view.html",{'variable':variable,'collections':collections,'colcount':len(collections)})

