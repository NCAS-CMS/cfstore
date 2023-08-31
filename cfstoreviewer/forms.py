from django import forms

from cfstore.config import CFSconfig

class VariableSearchForm(forms.Form):
    variablename = forms.CharField(label="Variable Search", max_length=100)

class VariableBrowseForm(forms.Form):
    my_object = forms.MultipleChoiceField(
        widget=forms.CheckboxSelectMultiple,
    )

class CollectionSearchForm(forms.Form):
    collection_searchname = forms.CharField(label="Collection Search", max_length=100)


class SaveAsCollectionForm(forms.Form):
    collectionname = forms.CharField(label="Save as Collection", max_length=100)