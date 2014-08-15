# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import config
from ..database import DatabaseChooser
from ..backends.default.database import SingleFileDatabase
from ..errors import UnknownObject, MissingIntermediateData, UntypedExchange, \
    InvalidExchange
from ..meta import mapping, geomapping, databases
from ..validate import db_validator
from .fixtures import food, biosphere
import copy
import os
import pickle


class DatabaseTest(BW2DataTest):
    def test_setup(self):
        d = DatabaseChooser("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        d = DatabaseChooser("food")
        d.register(depends=["biosphere"])
        d.write(food)
        self.assertEqual(len(databases), 2)

    def test_copy(self):
        d = DatabaseChooser("food")
        d.register(depends=["biosphere"])
        d.write(food)
        with self.assertRaises(AssertionError):
            d.copy("food")
        d.copy("repas")
        self.assertTrue("repas" in databases.list)

    def test_copy_does_deepcopy(self):
        data = {
            ("old name", 1): {
                "exchanges": [{"input": ("old name", 1), "amount": 1.0}]
            }
        }
        d = DatabaseChooser("old name")
        d.register()
        d.write(data)
        new_db = d.copy("new name")
        new_data = new_db.load()
        self.assertEqual(
            new_data.values()[0]['exchanges'][0]['input'],
            ('new name', 1)
        )
        self.assertEqual(
            data.values()[0]['exchanges'][0]['input'],
            ('old name', 1)
        )
        self.assertEqual(
            d.load().values()[0]['exchanges'][0]['input'],
            ('old name', 1)
        )

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
        db = DatabaseChooser("foo")
        self.assertEqual(shiny_new, db.relabel_data(old_data, "shiny new"))

    def test_register(self):
        database = DatabaseChooser("testy")
        database.register()
        self.assertTrue("testy" in databases)
        self.assertTrue('depends' in databases['testy'])

    def test_deregister(self):
        d = DatabaseChooser("food")
        d.register(depends=["biosphere"])
        self.assertTrue("food" in databases)
        d.deregister()
        self.assertTrue("food" not in databases)

    def test_write_unregistered_database_raises_error(self):
        d = DatabaseChooser("food")
        with self.assertRaises(UnknownObject):
            d.write(food)

    def test_rename(self):
        d = DatabaseChooser("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        d = DatabaseChooser("food")
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

    def test_write_sets_databases_number_attribute(self):
        d = DatabaseChooser("biosphere")
        d.register()
        d.write(biosphere)
        self.assertEqual(databases["biosphere"]["number"], len(biosphere))

    def test_process_adds_to_mappings(self):
        database = DatabaseChooser("testy")
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

    def test_untyped_exchange_error(self):
        database = DatabaseChooser("testy")
        database.register()
        database_data = {
            ("testy", "A"): {'exchanges': [
                {'amount': 1, 'input': ('testy', 'A')}
            ]},
        }
        database.write(database_data)
        with self.assertRaises(UntypedExchange):
            database.process()

    def test_no_input_raises_invalid_exchange(self):
        database = DatabaseChooser("testy")
        database.register()
        database_data = {
            ("testy", "A"): {'exchanges': [
                {'amount': 1}
            ]},
        }
        database.write(database_data)
        with self.assertRaises(InvalidExchange):
            database.process()

    def test_no_amount_raises_invalid_exchange(self):
        database = DatabaseChooser("testy")
        database.register()
        database_data = {
            ("testy", "A"): {'exchanges': [
                {'input': ('testy', 'A')}
            ]},
        }
        database.write(database_data)
        with self.assertRaises(InvalidExchange):
            database.process()

    def test_process_geomapping_array(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({})
        database.process()
        fp = os.path.join(
            config.dir,
            u"processed",
            database.filename + u".geomapping.pickle"
        )
        array = pickle.load(open(fp, "rb"))
        fieldnames = {'activity', 'geo', 'row', 'col'}
        self.assertFalse(fieldnames.difference(set(array.dtype.names)))

    def test_process_checks_process_type(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({
            ("a database", "foo"): {
                'exchanges': [],
                'type': 'process'
            },
            ("a database", "bar"): {
                'type': 'definitely not a process'
            },
        })
        # This shouldn't raise an error
        database.process()

    def test_only_processes_in_geomapping(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({
            ("a database", "foo"): {
                'exchanges': [],
                'type': 'process'
            },
            ("a database", "bar"): {
                'exchanges': [],
                'type': 'process'
            },
            ("a database", "baz"): {
                'exchanges': [],
                'type': 'not a process'
            },
        })
        database.process()
        fp = os.path.join(
            config.dir,
            u"processed",
            database.filename + u".geomapping.pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array.shape, (2,))

    def test_geomapping_array_includes_only_processes(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({
            ("a database", "foo"): {
                'exchanges': [],
                'type': 'process',
                'location': 'bar'
            },
            ("a database", "baz"): {
                'exchanges': [],
                'type': 'emission'
            },
        })
        database.process()
        fp = os.path.join(
            config.dir,
            u"processed",
            database.filename + u".geomapping.pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array.shape, (1,))
        self.assertEqual(array[0]['geo'], geomapping['bar'])

    def test_processed_array(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({("a database", 2): {
            'type': 'process',
            'exchanges': [{
                'input': ("a database", 2),
                'amount': 42,
                'uncertainty type': 7,
                'type': 'production'
            }]
        }})
        database.process()
        fp = os.path.join(
            config.dir,
            u"processed",
            database.filename + u".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        fieldnames = {'input', 'output', 'row', 'col', 'type'}
        self.assertFalse(fieldnames.difference(set(array.dtype.names)))
        self.assertEqual(array.shape, (1,))
        self.assertEqual(array[0]['uncertainty_type'], 7)
        self.assertEqual(array[0]['amount'], 42)

    def test_loc_value_if_no_uncertainty(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({("a database", 2): {
            'type': 'process',
            'exchanges': [{
                'input': ("a database", 2),
                'amount': 42,
                'type': 'technosphere'
            }]
        }})
        database.process()
        fp = os.path.join(
            config.dir,
            u"processed",
            database.filename + u".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array['loc'][0], 42.)

    def test_base_class(self):
        database = DatabaseChooser("a database")
        self.assertEqual(database.metadata, databases)
        self.assertEqual(
            [x[0] for x in database.dtype_fields],
            ['input', 'output', 'row', 'col', 'type']
        )
        self.assertEqual(
            [x[0] for x in database.dtype_fields],
            ['input', 'output', 'row', 'col', 'type']
        )

    def test_find_dependents(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({
            ("a database", "foo"): {
                'exchanges': [
                    {'input': ("foo", "bar")},
                    {'input': ("biosphere", "bar")},
                    {'input': ("awkward", "silence")}, # Ignore becuase of 'ignore'
                    {'input': ("who", "am I?"), "type": "unknown"}, # Ignored because of 'unknown' type
                    {'input': ("biosphere", "bar")},
                ],
                'location': 'bar'
            },
            ("a database", "baz"): {
                'exchanges': [{'input': ("baz", "w00t")}],
                'type': 'emission' # Ignored because of type
            },
            ("a database", "nonce"): {},  # OK not to have 'exchanges'
        })
        self.assertEqual(
            database.find_dependents(ignore={"awkward"}),
            ["biosphere", "foo"]
        )

    def test_set_dependents(self):
        database = DatabaseChooser("a database")
        database.register()
        self.assertEqual(databases['a database']['depends'], [])
        keys = [("biosphere", "bar"), ("baz", "w00t"), ("foo", "bar")]
        mapping.add(keys)
        database.write({
            ("a database", "foo"): {
                'exchanges': [
                    {'input': ("foo", "bar"), 'type': 'technosphere', 'amount': 1},
                    {'input': ("biosphere", "bar"), 'type': 'biosphere', 'amount': 1}
                ],
                'type': 'process',
                'location': 'bar'
            },
            ("a database", "baz"): {
                'exchanges': [{'input': ("baz", "w00t"), 'type': 'technosphere', 'amount': 1}],
                'type': 'emission'
            },
        })
        database.process()
        # 'baz' ignored because not a process
        self.assertEqual(
            databases['a database']['depends'],
            ["biosphere", "foo"]
        )

    def test_process_without_exchanges_still_in_processed_array(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({("a database", "foo"): {}})
        database.process()
        fp = os.path.join(
            config.dir,
            u"processed",
            database.filename + u".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array['amount'][0], 1)
        self.assertEqual(array.shape, (1,))


class SingleFileDatabaseTest(BW2DataTest):
    # TODO: Better check .write?

    def test_revert(self):
        d = SingleFileDatabase("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        d = SingleFileDatabase("food")
        d.register(depends=["biosphere"])
        d.write(food)
        d.write({})
        self.assertEqual(databases["food"]["version"], 2)
        self.assertEqual(SingleFileDatabase("food").load(), {})
        d.revert(1)
        self.assertEqual(databases["food"]["version"], 1)
        self.assertEqual(SingleFileDatabase("food").load(), food)
        with self.assertRaises(AssertionError):
            d.revert(10)

    def test_versions(self):
        d = SingleFileDatabase("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        self.assertEqual(
            [x[0] for x in d.versions()], [1]
        )
        d.write(biosphere)
        self.assertEqual(
            [x[0] for x in d.versions()], [1, 2]
        )

    def test_wrong_version(self):
        d = SingleFileDatabase("biosphere")
        d.register()
        d.write(biosphere)
        with self.assertRaises(MissingIntermediateData):
            d.load(version=-1)

    def test_noninteger_version(self):
        d = SingleFileDatabase("biosphere")
        d.register()
        d.write(biosphere)
        with self.assertRaises(ValueError):
            d.load(version="foo")

    def test_register(self):
        database = SingleFileDatabase("testy")
        database.register()
        self.assertEqual(databases['testy']['version'], 0)

    def test_load(self):
        d = SingleFileDatabase("food")
        d.register(depends=["biosphere"])
        d.write(food)
        data = SingleFileDatabase("food").load()
        self.assertEqual(food, data)

    def test_write_bumps_version_number(self):
        d = SingleFileDatabase("food")
        d.register(depends=["biosphere"])
        d.write(food)
        self.assertEqual(databases["food"]["version"], 1)
        d.write(food)
        self.assertEqual(databases["food"]["version"], 2)

    def test_validator(self):
        database = SingleFileDatabase("a database")
        self.assertEqual(database.validator, db_validator)
        self.assertTrue(database.validate({}))
