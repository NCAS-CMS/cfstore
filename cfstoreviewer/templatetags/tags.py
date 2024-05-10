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
    iden = var.identity
    iden = iden.replace("long_name=", "")
    iden = iden.replace(" ","_")
    return iden

@template.defaulttags.register.filter
def outputcellmethods(var):
    cm = var._cell_methods
    output= ""
    for key in cm.keys():
        output = output + key + " "
    return output[:-1]

@template.defaulttags.register.filter
def vardrsdisplay(var):
    drs = var["experiment"] + "." + var["variant_id"] + "." + var["realm"] + "."
    return drs


@template.defaulttags.register.filter
def getproxied(var):
    return var._proxied.values()


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
def getallvariables(collection):
    db = CFSconfig().db
    variables = db.retrieve_all_variables("all", "_")
    variables = [outputvar(var) for var in variables]
    return variables


@template.defaulttags.register.filter
def getcollectionsfromvariable(variable):
    db = CFSconfig().db
    collection = variable.in_collection.all()
    return collection


@template.defaulttags.register.filter
def getuniquevariables(collection, check):
    db = CFSconfig().db
    variables = db.retrieve_variables_in_collection(collection)
    uniquevariables = {}
    for v in variables:
        uniquevariables[v.identity] = len(v._proxied)
    return uniquevariables.items()


@template.defaulttags.register.filter
def getallvariableproperties(collection):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    properydict = {}
    for var in variables:
        for simulation, properties in var._proxied.items():
            properydict[simulation] = {}
            for prop, value in properties.items():
                if prop not in properydict[simulation].keys():
                    properydict[simulation][prop] = [value]
                elif value not in properydict[simulation][prop]:
                    properydict[simulation][prop].append(value)
    return properydict.items()


@template.defaulttags.register.filter
def demogetallvariableproperties(collection):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    properydict = {}
    for var in variables:
        for _, properties in var._proxied.items():
            for prop, value in properties.items():
                if prop not in properydict.keys():
                    properydict[prop] = [value]
                elif value not in properydict[prop]:
                    properydict[prop].append(value)
    return properydict.items()


@template.defaulttags.register.filter
def getpropertyforsearchbar(collection, property):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    experiments = []
    for variable in variables:
        for var in variable._proxied.values():
            value = var[property]
            if value not in experiments:
                experiments.append(value)
    return experiments


@template.defaulttags.register.filter
def getpropertyforautocompletesearchbar(property):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    experiments = []
    for variable in variables:
        for var in variable._proxied.values():
            value = var[property]
            if value not in experiments:
                experiments.append(value)
    print(experiments)
    return experiments


@template.defaulttags.register.filter
def getcellmethodsforautocompletesearchbar(property):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    output = []
    for variable in variables:
        for var in variable._cell_methods:
            if var not in output:
                output.append(var)
    return output

@template.defaulttags.register.filter
def getfrequencyforautocompletesearchbar(property):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    output = ['hourly','daily','monthly']
    return output

@template.defaulttags.register.filter
def getLocationAutosearchallvariables(property):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    output = ['unavailable','tape','monthly']
    return output

@register.inclusion_tag("demo_page.html", takes_context=True)
def jump_link(context):
    return {
        "realm": getpropertyforautocompletesearchbar("realm"),
    }


@template.defaulttags.register.filter
def checkvar(variable, properties):
    db = CFSconfig().db
    variable = db.retrieve_variable("identity", variable)
    check = True
    for p in properties:
        if p not in variable._proxied.values():
            check = False
    return check


@template.defaulttags.register.filter
def checkcol(collections, check):
    db = CFSconfig().db
    returncollections = []
    for collection in collections:
        variables = db.retrieve_variables_in_collection(collection)
        variablenames = [v.identity for v in variables]
        uniquevariables = {}
        for v in variablenames:
            if v not in uniquevariables:
                uniquevariables[v] = 1
            else:
                uniquevariables[v] += 1
        if uniquevariables:
            returncollections.append(collection)
    return returncollections


@template.defaulttags.register.filter
def getvariablepropertyvalues(variable):
    db = CFSconfig().db
    variable = db.retrieve_variable("identity", variable)
    properties = variable._proxied.values()
    return properties


@template.defaulttags.register.filter
def getvariablepropertykeys(variable):
    properties = variable._proxied.keys()
    return properties


@template.defaulttags.register.filter
def getvariablepropertyitems(variable):
    properties = variable._proxied.items()
    return properties


@template.defaulttags.register.filter
def unpackverttable(verttable):
    return verttable


@template.defaulttags.register.filter
def getallvariablecellmethods(collection):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    allcellmethods = {}
    for var in variables:
        cellmethods = var._cell_methods
        for cellmethod in cellmethods:
            if isinstance(cellmethod, dict):
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
                axes = cellmethod["axes"]
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
    return variable


@template.defaulttags.register.filter
def getcellmethodaxes(variable):
    axes = variable._cell_methods["axes"]
    return axes


@template.defaulttags.register.filter
def getpropertyvalues(propname):
    db = CFSconfig().db
    variables = db.retrieve_variable("all", "")
    output = propname
    return output.items()


@template.defaulttags.register.filter
def displayproperty(prop):
    output = prop[0] + " (" + str(prop[1]) + ")"
    return output


@template.defaulttags.register.filter
def getproperty(prop):
    output = prop[0]
    return output


@template.defaulttags.register.filter
def length(list):
    return len(list)


@template.defaulttags.register.filter
def format_long_name(name):
    name = name.replace("long_name=", "")
    name = name.replace("/", "")
    return name


@template.defaulttags.register.filter
def get_percentage(col):
    return 0
