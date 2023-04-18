from django import forms

class SearchForm(forms.Form):
    srch = forms.CharField(label="search_input",max_length=100)