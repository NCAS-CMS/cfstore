from django import template

from cfstore.config import CFSconfig

register = template.Library()


@register.simple_tag
def active(request, pattern):
    """to help with navigation CSS"""
    import re

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
def getcollectionsfromvariable(variable):
    db = CFSconfig().db
    collection = variable.in_collection.all()
    return collection

@template.defaulttags.register.filter
def getuniquevariables(collection):
    db = CFSconfig().db
    variables = db.retrieve_variables_in_collection(collection.name)
    variablenames = [v.identity for v in variables]
    uniquevariables = {}
    for v in variablenames:
        if v not in uniquevariables:
            uniquevariables[v] = 1
        else:
            uniquevariables[v] += 1
    return uniquevariables.items()

@template.defaulttags.register.filter
def getallvariableproperties(collection):
    db = CFSconfig().db
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
    properties = {k: v for k, v in sorted(properties.items(), key=lambda item: item[1], reverse=True)}
    return properties.items()

@template.defaulttags.register.filter
def checkvar(variable,properties):
    print(variable)
    print(properties)
    db = CFSconfig().db
    variable = db.retrieve_variable("identity",variable)
    check = True
    for p in properties:
        print(p, properties)
        if p not in variable._proxied.values():
            print("NOT FOUND")
            check = False
    print(check)
    return check

@template.defaulttags.register.filter
def getvariableproperty(variable):
    db = CFSconfig().db
    variable = db.retrieve_variable("identity",variable)
    properties = variable._proxied.values()
    return properties

@template.defaulttags.register.filter
def getvariableproperties(variable):
    properties = variable._proxied.items()
    return properties


@template.defaulttags.register.filter
def getallvariablecellmethods(collection):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    allcellmethods = {}
    for var in variables:
        cellmethods = var._cell_methods
        for cellmethod in cellmethods:
            if isinstance(cellmethod, dict):
                print(cellmethods)
                method = cellmethod["methods"]
                axes = cellmethod["axes"]
                if method not in allcellmethods:
                    allcellmethods[method] = [method]
                elif method not in allcellmethods[method]:
                    allcellmethods[method].append(method)
    for cm in allcellmethods:
        allcellmethods[cm] = len(allcellmethods[cm])  
    return allcellmethods

@template.defaulttags.register.filter
def getallvariablecellaxes(collection):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    allcellmethods = {}
    for var in variables:
        cellmethods = var._cell_methods
        for cellmethod in cellmethods:
            if isinstance(cellmethod, dict):
                print(cellmethods)
                axes = cellmethod["axes"]
                print("AXES",axes)
                for a in axes:
                    if a not in allcellmethods:
                        allcellmethods[a] = [a]
                    elif a not in allcellmethods[a]:
                        allcellmethods[a].append(a)
    for cm in allcellmethods:
        allcellmethods[cm] = len(allcellmethods[cm])  
    return allcellmethods

@template.defaulttags.register.filter
def getcellmethods(variable):
    print("VARIABLE",variable)
    return variable

@template.defaulttags.register.filter
def getcellmethodaxes(variable):
    print(variable)
    axes = variable._cell_methods["axes"]
    return axes

@template.defaulttags.register.filter
def getpropertyvalues(propname):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    output = []
    for var in variables:
        if propname[0] in var._proxied and var[propname[0]] not in output:
            output.append(var[propname[0]])
    return output


@template.defaulttags.register.filter
def displayproperty(prop):
    output = prop[0] + " (" + str(prop[1]) + ")"
    return output


@template.defaulttags.register.filter
def length(list):
    return len(list)
