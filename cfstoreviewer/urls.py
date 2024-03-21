"""cfstoreapp URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path

from . import views

urlpatterns = [
    path("", views.ls, name="index"),
    path("viewcollections/", views.ls, name="allcollections"),
    path("viewcollections/search/", views.lsbrowse, name="allcollections"),
    path(
        "viewcollections/search/download", views.downloadsearch, name="allcollections"
    ),
    path("viewcollections/<str:page>/", views.lscol),
    path("viewcollections/<str:collection>/delete", views.deletecol),
    path("viewcollections/<str:collection>/confirmdelete", views.confirmdelete),
    path("viewcollections/<str:page>/download/", views.downloadcol),
    path("<str:page>/download/", views.downloadcol),
    path("viewcollections/<str:page>/search/download/", views.downloadcol),
    path("viewcollections/variables/", views.lsvar, name="variableindex"),
    path("viewcollections/variables/<str:var>", views.lsvar),
    path("viewcollections/variables/<str:var>/<str:prop>/download/", views.downloadvar),
    path("demo", views.demo),
]
