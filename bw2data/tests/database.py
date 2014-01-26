# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import Database, databases, mapping, geomapping, config
from ..errors import UnknownObject
from .fixtures import food, biosphere
import copy
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle


class DatabaseTest(BW2DataTest):
    def test_setup(self):
        d = Database("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        d = Database("food")
        d.register(depends=["biosphere"])
        d.write(food)
        self.assertEqual(len(databases), 2)

    def test_copy(self):
        d = Database("food")
        d.register(depends=["biosphere"])
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
        d.register(depends=[])
        d.write(biosphere)
        d = Database("food")
        d.register(depends=["biosphere"])
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
        database = Database("testy")
        database.register()
        self.assertTrue("testy" in databases)
        self.assertTrue('version' in databases['testy'])
        self.assertTrue('depends' in databases['testy'])

    def test_deregister(self):
        d = Database("food")
        d.register(depends=["biosphere"])
        self.assertTrue("food" in databases)
        d.deregister()
        self.assertTrue("food" not in databases)

    def test_load(self):
        d = Database("food")
        d.register(depends=["biosphere"])
        d.write(food)
        data = Database("food").load()
        self.assertEqual(food, data)

    def test_write_bumps_version_number(self):
        d = Database("food")
        d.register(depends=["biosphere"])
        d.write(food)
        self.assertEqual(databases["food"]["version"], 1)
        d.write(food)
        self.assertEqual(databases["food"]["version"], 2)

    def test_write_unregistered_database_raises_error(self):
        d = Database("food")
        with self.assertRaises(UnknownObject):
            d.write(food)

    def test_rename(self):
        d = Database("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        d = Database("food")
        d.register(depends=["biosphere"])
        d.write(copy.deepcopy(food))
        ndb = d.rename("buildings")
        ndb_data = ndb.load()
        self.assertEqual(ndb.name, "buildings")
        self.assertEqual(len(ndb_data), len(food))
        for key in ndb_data:
            self.assertEqual(key[0], "buildings")
            for exc in ndb_data[key]['exchanges']:
                self.assertTrue(exc['input'][0] in ('biosphere', 'buildings'))

    def test_process_adds_to_mappings(self):
        database = Database("testy")
        database.register()
        database_data = {
            ("testy", "A"): {'location': 'CH'},
            ("testy", "B"): {'location': 'DE'},
        }
        database.write(database_data)
        self.assertTrue(
            ("testy", "A") in mapping and ("testy", "B") in mapping
        )
        self.assertTrue(
            "CH" in geomapping and "DE" in geomapping
        )

    def test_process_geomapping_array(self):
        database = Database("a database")
        database.register()
        database.write({})
        database.process()
        fp = os.path.join(
            config.dir,
            u"processed",
            database.name + u".geomapping.pickle"
        )
        array = pickle.load(open(fp, "rb"))
        fieldnames = {'activity', 'geo', 'row', 'col'}
        self.assertFalse(fieldnames.difference(set(array.dtype.names)))

    def test_processed_array(self):
        database = Database("a database")
        database.register()
        database.write({})
        database.process()
        fp = os.path.join(
            config.dir,
            u"processed",
            database.name + u".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        fieldnames = {'input', 'output', 'row', 'col', 'type'}
        self.assertFalse(fieldnames.difference(set(array.dtype.names)))

