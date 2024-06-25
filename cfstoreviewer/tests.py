import os
from unittest import mock

import django
from click.testing import CliRunner
from django.test import TestCase

from cfstore.cfdb import cli
from cfstore.cfin import cli as incli
from cfstore.interface import CollectionDB, CollectionError
from cfstore.plugins.ssh import SSHlite
from cfstoreviewer.models import Collection, File, Location, Variable


def _dummy(db, location="testing", collection_stem="dummy", files_per_collection=10):
    """Set up a dummy dataset in db with accessiblepy
    structure for testing
    """
    db.create_location(location)
    for i in range(5):
        c = f"{collection_stem}{i}"
        db.create_collection(c, "no description", {})
        files = [
            {"path": "/somewhere/in/unix_land", "name": f"file{j}{i}", "size": 10}
            for j in range(files_per_collection)
        ]
        db.upload_files_to_collection(location, c, files)


class CollectionTestCase(TestCase):
    def setUp(self):
        self.db = CollectionDB()

    def test_create_collection(self):
        """Collection can be created"""
        col = self.db.create_collection("colcreate", "Test Collection Number 1")
        self.assertEqual(col.name, "colcreate")

    def test_retrieve_collection(self):
        ol = self.db.create_collection("colcreatedd", "aaaaaaaa")
        ol = self.db.create_collection("colcrebte", "bbbbbbbbbdd")
        a = self.db.retrieve_collections(name_contains="a")
        self.assertEqual(len(a), 1)
        b = self.db.retrieve_collections(description_contains="b")
        self.assertEqual(len(b), 1)
        c = self.db.retrieve_collections(contains="dd")
        self.assertEqual(len(c), 2)

    def test_collection_functions(self):
        """Collection functions work"""
        colfun = self.db.create_collection("colfun", "Test Collection Number 1")

        colfun["test"] = "yes"
        self.assertEqual(colfun["test"], "yes")
        self.assertEqual("test" in colfun, True)
        self.assertEqual(len(colfun), 1)

    def test_add_relationship_between_collections(self):
        """Add relationship between two collections"""
        ted = self.db.create_collection("ted", "A man called ted")
        ed = self.db.create_collection("ed", "his brother, ed")
        rel = self.db.add_relationship(ted.name, ed.name, "brother")

        self.assertEqual(
            self.db.retrieve_related(ted.name, "brother")[0].related_collection.all()[
                0
            ],
            ed,
        )

        fred = self.db.create_collection("fred", "his ther brother, fred")
        rel = self.db.add_relationships(ted.name, fred.name, "brother", "brother")

        self.assertEqual(
            self.db.retrieve_related(fred.name, "brother")[0].related_collection.all()[
                0
            ],
            ted,
        )
        self.assertEqual(
            self.db.retrieve_related(ted.name, "brother")[1].related_collection.all()[
                0
            ],
            fred,
        )
        self.assertEqual(
            self.db.retrieve_relationships("fred").all()[0].predicate, "brother"
        )

    def test_add_files(self):
        loc = self.db.create_location("TestLocation", protocols=["test"])
        c = self.db.create_collection("FileTestCollection", "no description", {})
        c.save()
        self.db.upload_file_to_collection(
            loc.name,
            c.name,
            {"path": "/somewhere/in/unix_land", "name": "lonefile", "size": 1},
        )
        self.db.remove_file_from_collection(
            c.name, "/somewhere/in/unix_land", "lonefile"
        )
        files = [
            {"path": "/somewhere/in/unix_land", "name": f"file{j}", "size": 1}
            for j in range(2)
        ]
        self.db.upload_files_to_collection(loc.name, c.name, files)
        files_in_c = self.db.retrieve_files_in_collection("FileTestCollection")
        files_in_c = self.db.retrieve_files_in_collection(
            "FileTestCollection", replicants=True
        )
        filenames = [f.name for f in files_in_c]
        self.assertEqual(filenames, ["file0", "file1"])

        self.db.delete_file_from_collection(c.name, files_in_c[0].__address__())

    def test_delete_collection(self):
        loc = self.db.create_location("TestLocation")
        col = self.db.create_collection(
            "EvilCollection",
            "A truly villainous collection. Delete him before he gets you!",
            {},
        )
        self.db.delete_collection("EvilCollection")
        col = self.db.create_collection(
            "EvilCollection", "Another one from the shadows. Delete him too!", {}
        )
        files = [
            {"path": "/somewhere/in/unix_land", "name": f"file{j}", "size": 1}
            for j in range(2)
        ]
        self.db.upload_files_to_collection(loc.name, col.name, files)
        # self.db.delete_collection("EvilCollection")
        self.db.delete_collection("EvilCollection", force=True)

    def test_add_to_collection(self):
        col = self.db.create_collection("Empty", "A vessel, waiting to be filled", {})
        var = Variable.objects.create(
            standard_name="fake", long_name="faaaaake", cfdm_size=0
        )
        file = File.objects.create(name="addfile", path="home/path", size=4)
        self.db.add_file_to_collection(col.name, file)
        self.db.add_variable_to_collection(col.name, var)

    def test_tag_collection(self):
        col = self.db.create_collection("Tag", "For tagging")
        col2 = self.db.create_collection("Tag2", "Also for tagging")
        self.db.tag_collection(col.name, "tagged")
        self.db.tag_collection(col2.name, "tagged")
        self.db.remove_tag_from_collection(col.name, "tagged")


class FileTestCase(TestCase):
    def setUp(self):
        self.db = CollectionDB()

    def test_retrieve_files(self):
        files = [
            {"path": "/somewhere/in/unix_land", "name": f"file{j}", "size": 1}
            for j in range(10)
        ]
        for file in files:
            File.objects.create(name=file["name"], path=file["path"], size=file["size"])
        self.db.retrieve_files_by_name("file3")
        self.db.retrieve_files_by_name("file")
        self.db.retrieve_files_by_name("fdsgdsfge")
        self.db.retrieve_file_if_present("/somewhere/in/unix_land/file0")
        self.db.retrieve_files_which_match("file")
        self.db.retrieve_or_make_file("newfile")

    def test_locate_replicants(self):
        loc1 = self.db.create_location("TestLocation", protocols=["test"])
        loc2 = self.db.create_location("TestLocation2", protocols=["test"])
        col1 = self.db.create_collection(
            "ReplicantCollection", "Like tears in the rain", {}
        )
        files1 = [
            {"path": "/somewhere/in/unix_land", "name": f"file{j}", "size": 1}
            for j in range(10)
        ]
        col2 = self.db.create_collection(
            "ReplicantCollection2", "Like tears in the rain", {}
        )
        files2 = [
            {"path": "/somewhere/in/unix_land", "name": f"file{j+5}", "size": 1}
            for j in range(10)
        ]
        self.db.upload_files_to_collection(loc1.name, col1.name, files1)
        self.db.upload_files_to_collection(loc2.name, col2.name, files2)
        self.db.locate_replicants(col1.name, match_full_path=True)
        self.db.locate_replicants(
            col1.name, match_full_path=True, strip_base="/somewhere"
        )
        self.db.locate_replicants(col1.name, strip_base="/somewhere")
        c, r = self.db.locate_replicants(col1.name)


class VariableTestCase(TestCase):
    def setUp(self):
        self.db = CollectionDB()

    def test_create_variable(self):
        self.db.retrieve_or_make_variable(
            "sname", "lname", "sname", 6, "domain", "test", "loc"
        )

    def test_retrieve_variable(self):
        var = self.db.retrieve_or_make_variable(
            "sname", "lname", "sname", 6, "domain", "test", "loc"
        )
        col = self.db.create_collection("varcol", "for the vars")
        var.save()
        col.save()
        self.db.add_variable_to_collection(col.name, var)
        self.db.retrieve_all_variables("standard_name", "sname")
        self.db.retrieve_variable("standard_name", "sname")
        self.db.retrieve_variable_query(
            ["standard_name", "long_name"], ["sname", "lname"]
        )
        self.db.retrieve_variables_in_collection(col.name)
        self.db.search_variables("standard_name", "sname")
        self.db.search_variable("standard_name", "sname")

    def test_show_collections_from_variable(self):
        var = self.db.retrieve_or_make_variable(
            "sname", "lname", "sname", 6, "domain", "test", "loc"
        )
        col = self.db.create_collection("varcol", "for the vars")
        var.save()
        col.save()
        self.db.add_variable_to_collection(col.name, var)
        self.db.show_collections_with_variable(var)

    def test_delete_all_var(self):
        var1 = self.db.retrieve_or_make_variable(
            "sn1ame", "lnam5e", "1sname", 6, "domain", "test", "loc"
        )
        var2 = self.db.retrieve_or_make_variable(
            "sna2me", "lna4me", "s2name", 6, "domain", "test", "loc"
        )
        var3 = self.db.retrieve_or_make_variable(
            "snam3e", "ln3ame", "sn3ame", 6, "domain", "test", "loc"
        )
        var4 = self.db.retrieve_or_make_variable(
            "sname4", "l2name", "sna4me", 6, "domain", "test", "loc"
        )
        var5 = self.db.retrieve_or_make_variable(
            "sname5", "1lname", "snam5e", 6, "domain", "test", "loc"
        )


class LocationTestCase(TestCase):
    def setUp(self):
        self.db = CollectionDB()

    def test_retrieve_location(self):
        self.db.create_location("Test", "Tester")
        self.db.retrieve_location("Test")
        self.db.retrieve_locations()


class UploadTestCase(TestCase):
    def setUp(self):
        self.db = CollectionDB()

    def test_add_variables_from_file_to_collection(self):
        """Read a files variables into a collection - essentially tests cfparsefile"""
        filecol = self.db.create_collection("19500101", "A file for variables")
        self.db.add_variables_from_file_to_collection(
            "/home/george/Documents/cfs/cfstore/cfstore/json/19500101T0000Z_i.cfa",
            "19500101",
            "TestLocation",
        )
        ice_area_vars = self.db.search_variables("long_name", "ice_area")
        self.db.save_as_collection(
            ice_area_vars,
            "ice area",
            "contains ice area variables (areables)",
            grouping_id="variables",
        )

    def test_add_variables_from_file(self):
        """Read a files variables and add them noware - essentially tests cfparsefile"""
        self.db.add_variables_from_file(
            "/home/george/Documents/cfs/cfstore/cfstore/json/19500101T0000Z_i.cfa",
            "TestLocation",
        )
        ice_area_vars = self.db.search_variables("long_name", "ice_area")
        self.db.save_as_collection(
            ice_area_vars,
            "ice area",
            "contains ice area variables (areables)",
            grouping_id="variables",
        )


class SSHTestCase(TestCase):
    @mock.patch.dict(
        os.environ,
        {
            "TEST_RP_HOST": "xfer1",
            "TEST_RP_PATH": "hiresgw/cftest",
            "TEST_RP_USER": "gobncas",
            "TEST_RP_EXPECTED_DIR": "subdir",
        },
    )
    def setup_ssh(self):
        rhost = os.getenv("TEST_RP_HOST", default="NONE")
        rpath = os.getenv("TEST_RP_PATH", default="NONE")
        ruser = os.getenv("TEST_RP_USER", default="NONE")
        expected = os.getenv("TEST_RP_EXPECTED_DIR", default="NONE")

        s = SSHlite(rhost, ruser)
        assert s.isalive(), "SSH test configuration does not work"
        return rhost, rpath, ruser, expected

    def test_ssh(self):
        """
        test the remote path includes an expected subdirectory
        """
        rhost, rpath, ruser, expected = self.setup_ssh()
        s = SSHlite(rhost, ruser)
        dlist = s.globish(rpath, "*")
        assert expected in dlist


class Test_cfin(TestCase):
    """
    Test the cfin command line interface
    """

    @mock.patch.dict(
        os.environ,
        {
            "TEST_RP_HOST": "xfer1",
            "TEST_RP_PATH": "/home/users/gobncas/canaricfas",
            "TEST_RP_USER": "gobncas",
            "TEST_RP_EXPECTED_DIR": "gws",
        },
    )
    def setup_ssh(self):
        rhost = os.getenv("TEST_RP_HOST", default="NONE")
        rpath = os.getenv("TEST_RP_PATH", default="NONE")
        ruser = os.getenv("TEST_RP_USER", default="NONE")
        expected = os.getenv("TEST_RP_EXPECTED_DIR", default="NONE")

        s = SSHlite(rhost, ruser)
        assert s.isalive(), "SSH test configuration does not work"
        return rhost, rpath, ruser, expected

    def test_create_location1(self):
        """Test creating location which we know doesn't exist"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                incli, ["rp", "setup", "a_location", "host_does_not_exist", "user"]
            )

    def test_create_location2(self):
        """Test creating location which we know does exist"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                incli, ["rp", "setup", "location1", "host_does_not_exist", "user"]
            )
            # this should raise a ValueError given we already have this location
            # it looks like it does, but somehow the test environment is removing it ...
            result = runner.invoke(
                incli, ["rp", "setup", "location1", "host_does_not_exist", "user"]
            )
            self.assertEqual(result.exit_code, 1)
            self.assertFalse(str(result.exception).find("already exists in") == -1)

    def test_add_remote_posix(self):
        """
        This test requires you to have an ssh host and location set in environment variables.
        They are mocked here in setup_ssh, you might need to do the same.
        You also need to sort out what is in the test directory.
        """
        rhost, rpath, ruser, expected = self.setup_ssh()
        print(rhost, rpath, ruser, expected)
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(incli, ["rp", "setup", "testremloc", rhost, ruser])
            print(result)
            result, stderr, stdout = runner.invoke(
                incli,
                [
                    "rp",
                    "add",
                    "testremloc",
                    rpath,
                    "testrem_collection",
                    "--description=testfile",
                ],
            )
            print("RESULT", result, stderr, stdout)
            result = runner.invoke(
                cli, ["ls", "--collection=testrem_collection", "--output=locations"]
            )
            print(result)
