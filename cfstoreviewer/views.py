from django.urls import path

from . import views

from django.http import HttpResponse
from django.shortcuts import render

import os
from pathlib import Path

from cfstore import interface, cfdb

def index(request):
    return HttpResponse("Welcome to CFstore. You're at the polls index.")

def ls(request,page="index.html"):
    return render(request,page)

urlpatterns = [
    path('', views.index, name='index'),
    path('/viewcollections',views.ls, name='allcollections'),
    path('/viewcollections/<str:page>',views.ls)
]
