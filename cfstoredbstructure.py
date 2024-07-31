# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class AuthGroup(models.Model):
    name = models.CharField(unique=True, max_length=150)

    class Meta:
        managed = False
        db_table = "auth_group"


class AuthGroupPermissions(models.Model):
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)
    permission = models.ForeignKey("AuthPermission", models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "auth_group_permissions"
        unique_together = (("group", "permission"),)


class AuthPermission(models.Model):
    content_type = models.ForeignKey("DjangoContentType", models.DO_NOTHING)
    codename = models.CharField(max_length=100)
    name = models.CharField(max_length=255)

    class Meta:
        managed = False
        db_table = "auth_permission"
        unique_together = (("content_type", "codename"),)


class AuthUser(models.Model):
    password = models.CharField(max_length=128)
    last_login = models.DateTimeField(blank=True, null=True)
    is_superuser = models.BooleanField()
    username = models.CharField(unique=True, max_length=150)
    last_name = models.CharField(max_length=150)
    email = models.CharField(max_length=254)
    is_staff = models.BooleanField()
    is_active = models.BooleanField()
    date_joined = models.DateTimeField()
    first_name = models.CharField(max_length=150)

    class Meta:
        managed = False
        db_table = "auth_user"


class AuthUserGroups(models.Model):
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "auth_user_groups"
        unique_together = (("user", "group"),)


class AuthUserUserPermissions(models.Model):
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "auth_user_user_permissions"
        unique_together = (("user", "permission"),)


class CfstoreviewerCellMethod(models.Model):
    method = models.CharField(max_length=1024)
    axis = models.CharField(max_length=256)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_cell_method"


class CfstoreviewerCollection(models.Model):
    field_proxied = models.JSONField(
        db_column="_proxied"
    )  # Field renamed because it started with '_'.
    name = models.CharField(unique=True, max_length=256)
    volume = models.IntegerField()
    description = models.TextField()
    batch = models.BooleanField()

    class Meta:
        managed = False
        db_table = "cfstoreviewer_collection"


class CfstoreviewerCollectionFiles(models.Model):
    collection = models.ForeignKey(CfstoreviewerCollection, models.DO_NOTHING)
    file = models.ForeignKey("CfstoreviewerFile", models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_collection_files"
        unique_together = (("collection", "file"),)


class CfstoreviewerCollectionProperties(models.Model):
    collection = models.ForeignKey(CfstoreviewerCollection, models.DO_NOTHING)
    collectionproperty = models.ForeignKey(
        "CfstoreviewerCollectionproperty", models.DO_NOTHING
    )

    class Meta:
        managed = False
        db_table = "cfstoreviewer_collection_properties"
        unique_together = (("collection", "collectionproperty"),)


class CfstoreviewerCollectionTags(models.Model):
    collection = models.ForeignKey(CfstoreviewerCollection, models.DO_NOTHING)
    tag = models.ForeignKey("CfstoreviewerTag", models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_collection_tags"
        unique_together = (("collection", "tag"),)


class CfstoreviewerCollectionproperty(models.Model):
    value = models.TextField()
    key = models.CharField(max_length=128)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_collectionproperty"


class CfstoreviewerDirectory(models.Model):
    path = models.CharField(max_length=1024)
    cfa = models.BooleanField(db_column="CFA")  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = "cfstoreviewer_directory"


class CfstoreviewerDirectoryLocation(models.Model):
    directory = models.ForeignKey(CfstoreviewerDirectory, models.DO_NOTHING)
    location = models.ForeignKey("CfstoreviewerLocation", models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_directory_location"
        unique_together = (("directory", "location"),)


class CfstoreviewerFile(models.Model):
    path = models.CharField(max_length=256)
    checksum = models.CharField(max_length=1024)
    checksum_method = models.CharField(max_length=256)
    size = models.IntegerField()
    format = models.CharField(max_length=256)
    name = models.CharField(primary_key=True, max_length=256)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_file"


class CfstoreviewerFileLocations(models.Model):
    file = models.ForeignKey(CfstoreviewerFile, models.DO_NOTHING)
    location = models.ForeignKey("CfstoreviewerLocation", models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_file_locations"
        unique_together = (("file", "location"),)


class CfstoreviewerFileReplicas(models.Model):
    file = models.ForeignKey(CfstoreviewerFile, models.DO_NOTHING)
    location = models.ForeignKey("CfstoreviewerLocation", models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_file_replicas"
        unique_together = (("file", "location"),)


class CfstoreviewerLocation(models.Model):
    name = models.CharField(max_length=256)
    volume = models.IntegerField()

    class Meta:
        managed = False
        db_table = "cfstoreviewer_location"


class CfstoreviewerLocationHoldsFiles(models.Model):
    location = models.ForeignKey(CfstoreviewerLocation, models.DO_NOTHING)
    file = models.ForeignKey(CfstoreviewerFile, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_location_holds_files"
        unique_together = (("location", "file"),)


class CfstoreviewerLocationProtocols(models.Model):
    location = models.ForeignKey(CfstoreviewerLocation, models.DO_NOTHING)
    protocol = models.ForeignKey("CfstoreviewerProtocol", models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_location_protocols"
        unique_together = (("location", "protocol"),)


class CfstoreviewerProtocol(models.Model):
    name = models.CharField(max_length=256)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_protocol"


class CfstoreviewerRelationship(models.Model):
    predicate = models.CharField(max_length=50)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_relationship"


class CfstoreviewerRelationshipRelatedCollection(models.Model):
    relationship = models.ForeignKey(CfstoreviewerRelationship, models.DO_NOTHING)
    collection = models.ForeignKey(CfstoreviewerCollection, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_relationship_related_collection"
        unique_together = (("relationship", "collection"),)


class CfstoreviewerRelationshipSubjectCollection(models.Model):
    relationship = models.ForeignKey(CfstoreviewerRelationship, models.DO_NOTHING)
    collection = models.ForeignKey(CfstoreviewerCollection, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_relationship_subject_collection"
        unique_together = (("relationship", "collection"),)


class CfstoreviewerTag(models.Model):
    name = models.CharField(max_length=64)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_tag"


class CfstoreviewerVarMetadata(models.Model):
    type = models.CharField(max_length=16)
    json = models.BooleanField()
    boolean_value = models.BooleanField()
    char_value = models.TextField()
    int_value = models.BigIntegerField()
    real_value = models.FloatField()
    key = models.CharField(max_length=128)
    collection_id = models.ForeignKey(CfstoreviewerCollection, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_var_metadata"


class CfstoreviewerVariable(models.Model):
    field_cell_methods = models.JSONField(
        db_column="_cell_methods"
    )  # Field renamed because it started with '_'.
    field_proxied = models.JSONField(
        db_column="_proxied"
    )  # Field renamed because it started with '_'.
    cfdm_size = models.BigIntegerField()
    long_name = models.CharField(max_length=1024, blank=True, null=True)
    cfdm_domain = models.CharField(max_length=1024)
    standard_name = models.CharField(max_length=1024, blank=True, null=True)
    identity = models.CharField(max_length=1024)
    realm = models.CharField(max_length=1024)
    location = models.CharField(max_length=1024)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_variable"


class CfstoreviewerVariableInCollection(models.Model):
    variable = models.ForeignKey(CfstoreviewerVariable, models.DO_NOTHING)
    collection = models.ForeignKey(CfstoreviewerCollection, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_variable_in_collection"
        unique_together = (("variable", "collection"),)


class CfstoreviewerVariableInFiles(models.Model):
    variable = models.ForeignKey(CfstoreviewerVariable, models.DO_NOTHING)
    file = models.ForeignKey(CfstoreviewerFile, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "cfstoreviewer_variable_in_files"
        unique_together = (("variable", "file"),)


class CfstoreviewerVdm(models.Model):
    class Meta:
        managed = False
        db_table = "cfstoreviewer_vdm"


class DjangoAdminLog(models.Model):
    object_id = models.TextField(blank=True, null=True)
    object_repr = models.CharField(max_length=200)
    action_flag = models.PositiveSmallIntegerField()
    change_message = models.TextField()
    content_type = models.ForeignKey(
        "DjangoContentType", models.DO_NOTHING, blank=True, null=True
    )
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    action_time = models.DateTimeField()

    class Meta:
        managed = False
        db_table = "django_admin_log"


class DjangoContentType(models.Model):
    app_label = models.CharField(max_length=100)
    model = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = "django_content_type"
        unique_together = (("app_label", "model"),)


class DjangoMigrations(models.Model):
    app = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    applied = models.DateTimeField()

    class Meta:
        managed = False
        db_table = "django_migrations"


class DjangoSession(models.Model):
    session_key = models.CharField(primary_key=True, max_length=40)
    session_data = models.TextField()
    expire_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = "django_session"
