from django.db import models


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


class VDM(models.Model):
    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]

    class Meta:
        app_label = "cfstoreviewer"


class Protocol(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256)


class Location(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256)
    volume = models.IntegerField()
    protocols = models.ManyToManyField(Protocol)
    holds_files = models.ManyToManyField("File")


class File(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    path = models.CharField(max_length=256)
    checksum = models.CharField(max_length=1024)
    checksum_method = models.CharField(max_length=256)
    size = models.IntegerField()
    format = models.CharField(max_length=256, default="Unknown format")
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256)
    locations = models.ManyToManyField(Location, related_name="filelocations")
    replicas = models.ManyToManyField(Location)


class Tag(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=64)


#    collection_id = models.ForeignKey(Collection.id)
#    collections = models.ManyToManyField(Collection)


class CollectionProperty(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    id = models.AutoField(primary_key=True)
    value = models.TextField()
    key = models.CharField(max_length=128)
    # Collection_id = models.ForeignKey(Collection)


class Collection(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]

    def __repr__(self):
        return self.name + ":" + str(self.volume)

    _proxied = models.JSONField()
    name = models.CharField(max_length=256, unique=True)
    volume = models.IntegerField()
    description = models.TextField()
    id = models.AutoField(primary_key=True)
    batch = models.BooleanField()
    files = models.ManyToManyField(File)
    properties = models.ManyToManyField(CollectionProperty)
    tags = models.ManyToManyField(Tag)


class Relationship(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    predicate = models.CharField(max_length=50)
    subject_collection = models.ManyToManyField(Collection, related_name="subject")
    related_collection = models.ManyToManyField(Collection, related_name="related")


class Variable(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]

    def keys(self):
        return self._proxied.keys()

    def exists(self):
        return True

    def __name__(self):
        return self.identity

    _cell_methods = models.JSONField(default=dict)
    _proxied = models.JSONField(default=dict)
    cfdm_size = models.BigIntegerField()
    long_name = models.CharField(max_length=1024, null=True)
    id = models.AutoField(primary_key=True)
    cfdm_domain = models.CharField(max_length=1024)
    standard_name = models.CharField(max_length=1024, null=True)
    in_collection = models.ManyToManyField(Collection)
    in_files = models.ManyToManyField(File)
    identity = models.CharField(max_length=1024)


class Var_Metadata(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    type = models.CharField(max_length=16)
    collection_id = models.ForeignKey(Collection, on_delete=models.CASCADE)
    json = models.BooleanField()
    boolean_value = models.BooleanField()
    char_value = models.TextField()
    int_value = models.BigIntegerField()
    real_value = models.FloatField()
    key = models.CharField(max_length=128)


class Cell_Method(models.Model):
    class Meta:
        app_label = "cfstoreviewer"

    id = models.AutoField(primary_key=True)
    method = models.CharField(max_length=1024)
    axis = models.CharField(max_length=256)
