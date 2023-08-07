from django import template

from cfstore.config import CFSconfig

register = template.Library()


@register.simple_tag
def active(request, pattern):
    """to help with navigation CSS"""
    import re

    print(pattern, request.path)
    if re.search(pattern, request.path):
        return "active"
    return ""


@template.defaulttags.register.filter
def outputvar(var):
    """Can't use the django built in coz not everything is a float.
    But we can use this to suppress 0.0 in a nice way."""
    iden = var.identity
    iden = iden.replace("_", " ")
    iden = iden.replace("long name=", "(Long Name) ")
    return iden


@template.defaulttags.register.filter
def sizeoffmt(num):
    suffix = "B"
    print(num)
    num = int(num)
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


@template.defaulttags.register.filter
def getvariables(collection):
    db = CFSconfig().db
    variables = db.retrieve_variables_in_collection(collection.name)
    return variables


@template.defaulttags.register.filter
def getvariableproperties(collection):
    db = CFSconfig().db
    print(collection)
    variables = db.retrieve_variable("all", "")
    properties = {}
    for var in variables:
        for prop, value in var._proxied.items():
            if prop not in properties:
                properties[prop] = [value]
            elif value not in properties[prop]:
                properties[prop].append(value)
    for p in properties:
        properties[p] = len(properties[p])
    return properties.items()


@template.defaulttags.register.filter
def getpropertyvalues(propname):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    output = []
    for var in variables:
        if propname[0] in var._proxied and var[propname[0]] not in output:
            output.append(var[propname[0]])
    print(output)
    return output


@template.defaulttags.register.filter
def displayproperty(prop):
    output = prop[0] + " (" + str(prop[1]) + ")"
    return output


@template.defaulttags.register.filter
def length(list):
    return len(list)
