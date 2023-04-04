from django import template

register = template.Library()

@register.simple_tag
def active(request, pattern):
    """ to help with navigation CSS """
    import re
    print(pattern,request.path)
    if re.search(pattern, request.path):
        return 'active'
    return ''

@template.defaulttags.register.filter
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