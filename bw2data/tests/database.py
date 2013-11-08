# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import Database, databases
from ..errors import UnknownObject
import copy
from fixtures import food, biosphere


class DatabaseTest(BW2DataTest):
    def test_setup(self):
        d = Database("biosphere")
        d.register("Tests", [], len(biosphere))
        d.write(biosphere)
        d = Database("food")
        d.register("Tests", ["biosphere"], len(food))
        d.write(food)
        self.assertEqual(len(databases), 2)

    def test_copy(self):
        d = Database("food")
        d.register("Tests", ["biosphere"], len(food))
        d.write(food)
        with self.assertRaises(AssertionError):
            d.copy("food")
        d.copy("repas")
        self.assertTrue("repas" in databases.list)

    def test_relabel_data(self):
        old_data = {
            ("old and boring", 1): {
                "exchanges": [{"input": ("old and boring", 42), "amount": 1.0}]
            },
            ("old and boring", 2): {
                "exchanges": [{"input": ("old and boring", 1), "amount": 4.0}]
            }
        }
        shiny_new = {
            ("shiny new", 1): {
                "exchanges": [{"input": ("old and boring", 42), "amount": 1.0}]
            },
            ("shiny new", 2): {
                "exchanges": [{"input": ("shiny new", 1), "amount": 4.0}]
            }
        }
        db = Database("foo")
        self.assertEqual(shiny_new, db.relabel_data(old_data, "shiny new"))

    def test_revert(self):
        d = Database("biosphere")
        d.register("Tests", [], len(biosphere))
        d.write(biosphere)
        d = Database("food")
        d.register("Tests", ["biosphere"], len(food))
        d.write(food)
        d.write({})
        self.assertEqual(databases["food"]["version"], 2)
        self.assertEqual(Database("food").load(), {})
        d.revert(1)
        self.assertEqual(databases["food"]["version"], 1)
        self.assertEqual(Database("food").load(), food)
        with self.assertRaises(AssertionError):
            d.revert(10)

    def test_register(self):
        pass

    def test_deregister(self):
        d = Database("food")
        d.register("Tests", ["biosphere"], len(food))
        self.assertTrue("food" in databases)
        d.deregister()
        self.assertTrue("food" not in databases)

    def test_load(self):
        d = Database("food")
        d.register("Tests", ["biosphere"], len(food))
        d.write(food)
        data = Database("food").load()
        self.assertEqual(food, data)

    def test_write_bumps_version_number(self):
        d = Database("food")
        d.register("Tests", ["biosphere"], len(food))
        d.write(food)
        self.assertEqual(databases["food"]["version"], 1)
        d.write(food)
        self.assertEqual(databases["food"]["version"], 2)

    def test_write_unregistered_database_raises_error(self):
        d = Database("food")
        with self.assertRaises(UnknownObject):
            d.write(food)

    def test_repr(self):
        d = Database("food")
        self.assertTrue(isinstance(str(d), str))
        self.assertTrue(isinstance(unicode(d), unicode))

    def test_rename(self):
        d = Database("biosphere")
        d.register("Tests", [], len(biosphere))
        d.write(biosphere)
        d = Database("food")
        d.register("Tests", ["biosphere"], len(food))
        d.write(copy.deepcopy(food))
        ndb = d.rename("buildings")
        ndb_data = ndb.load()
        self.assertEqual(ndb.database, "buildings")
        self.assertEqual(len(ndb_data), len(food))
        for key in ndb_data:
            self.assertEqual(key[0], "buildings")
            for exc in ndb_data[key]['exchanges']:
                self.assertTrue(exc['input'][0] in ('biosphere', 'buildings'))
