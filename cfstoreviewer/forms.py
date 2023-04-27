from django import forms

class VariableSearchForm(forms.Form):
    variablename = forms.CharField(label="Variable Search",max_length=100)

class CollectionSearchForm(forms.Form):
    collection_searchname = forms.CharField(label="Collection Search",max_length=100)

class SaveAsCollectionForm(forms.Form):
    collectionname = forms.CharField(label="Save as Collection",max_length=100)
