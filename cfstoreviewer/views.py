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
        return_string = return_string+(var.standard_name+"<br>")
    elif var.long_name:
        return_string = return_string+(var.long_name+"<br>")
    else:
        return_string = return_string+("id "+str(var.id)+"(which has no name for some reason)"+"<br>")
    return return_string

def ls(request,page="all"):
    db = CFSconfig().db
    return_list = db.retrieve_variables_in_collection(page)
    return_string = "Collection contains:<br>"
    for r in return_list:
        return_string = return_string+("&nbsp"+outputvar(r) + "<br>")
    
    return HttpResponse(return_string)

def lsvar(request,col="all"):
    db = CFSconfig().db
    return_list = db.retrieve_variable("all","")
    return_string=""
    for r in return_list:
        return_string = return_string+("<br><br>Variable:<br>")
        return_string = return_string + outputvar(r)
        return_string = return_string + "is in<br>"
        for f in r.in_collection.all():
            return_string = return_string+("&nbsp "+f.name+"<br>")
    return HttpResponse(return_string)

urlpatterns = [
    path('', views.index, name='index'),
    path('/viewcollections',views.ls),
    path('/viewcollections/<str:page>',views.ls)
]
