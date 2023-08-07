from django import forms
from cfstore import db

class VariableSearchForm(forms.Form):
    variablename = forms.CharField(label="Variable Search",max_length=100)

class CollectionSearchForm(forms.Form):
    collection_searchname = forms.CharField(label="Collection Search",max_length=100)

class SaveAsCollectionForm(forms.Form):
    collectionname = forms.CharField(label="Save as Collection",max_length=100)

class FrequencyForm(forms.Form):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        brands = set(db.Variables.objects.all()._proxied.values)
        for brand in brands:
            self.fields[f'{brand}'] = forms.BooleanField(label=f'{brand}')