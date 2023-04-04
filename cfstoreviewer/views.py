from django.urls import path
from . import views

from django.http import HttpResponse
from django.shortcuts import render

import os
from pathlib import Path

from cfstore import interface, cfdb
from cfstore.config import CFSconfig

def index(request):
    return HttpResponse("Welcome to CFstore. You're at the polls index.")

def outputvar(var):
    return_string = ""
    if var.standard_name:
        return_string = return_string+(var.standard_name)
    elif var.long_name:
        return_string = return_string+(var.long_name)
    else:
        return_string = return_string+("id "+str(var.id)+"(which has no name for some reason)"+"<br>")
    return return_string

def ls(request,page="all"):
    db = CFSconfig().db
    variables = db.retrieve_variables_in_collection(page)
    return_string = "Collection contains:<br>"
    for r in variables:
        return_string = return_string+("&nbsp"+outputvar(r) + "<br>")
    
    return render(request,"collections_view.html",{'variables':variables})

def lsvar(request,col="all"):
    db = CFSconfig().db
    print("_____________",col)
    variables = db.retrieve_variable(col,"")
    variablesnames = [outputvar(v) for v in variables]
    return render(request, "variables_view.html",{'variables':variables,'variablesnames':variablesnames})

urlpatterns = [
    path('', views.index, name='index'),
    path('/viewcollections',views.ls),
    path('/viewcollections/<str:page>',views.ls)
]
