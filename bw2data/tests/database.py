# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import config
from ..database import DatabaseChooser
from ..backends.peewee import Activity as PWActivity, Exchange as PWExchange
from ..backends.single_file.database import SingleFileDatabase
from ..errors import UnknownObject, MissingIntermediateData, UntypedExchange, \
    InvalidExchange
from ..proxies import Activity as SFActivity, Exchange as SFExchange
from ..meta import mapping, geomapping, databases
from ..serialization import JsonWrapper, JsonSanitizer
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

    def test_get(self):
        d = DatabaseChooser("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        activity = d.get('1')
        self.assertTrue(isinstance(activity, PWActivity))
        self.assertEqual(activity.name, 'an emission')

    def test_get_random(self):
        d = DatabaseChooser("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        activity = iter(d).next()
        self.assertTrue(isinstance(activity, PWActivity))
        self.assertTrue(activity.name in ('an emission', 'another emission'))

    def test_get_random(self):
        d = DatabaseChooser("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        activity = d.random()
        self.assertTrue(isinstance(activity, PWActivity))
        self.assertTrue(activity.name in ('an emission', 'another emission'))

    def test_copy(self):
        d = DatabaseChooser("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        d = DatabaseChooser("food")
        d.register(depends=["biosphere"])
        d.write(food)
        with self.assertRaises(AssertionError):
            d.copy("food")
        d.copy("repas")
        self.assertTrue("repas" in databases.list)

    def test_copy_does_deepcopy(self):
        data = {
            ("old name", '1'): {
                "exchanges": [{
                    "input": ("old name", '1'),
                    "amount": 1.0,
                    'type': 'technosphere'
                }]
            }
        }
        d = DatabaseChooser("old name")
        d.register()
        d.write(data)
        new_db = d.copy("new name")
        new_data = new_db.load()
        self.assertEqual(
            new_data.values()[0]['exchanges'][0]['input'],
            ('new name', '1')
        )
        self.assertEqual(
            data.values()[0]['exchanges'][0]['input'],
            ('old name', '1')
        )
        self.assertEqual(
            d.load().values()[0]['exchanges'][0]['input'],
            ('old name', '1')
        )

    def test_relabel_data(self):
        old_data = {
            ("old and boring", '1'): {
                "exchanges": [{"input": ("old and boring", '42'), "amount": 1.0}]
            },
            ("old and boring", '2'): {
                "exchanges": [{"input": ("old and boring", '1'), "amount": 4.0}]
            }
        }
        shiny_new = {
            ("shiny new", '1'): {
                "exchanges": [{"input": ("old and boring", '42'), "amount": 1.0}]
            },
            ("shiny new", '2'): {
                "exchanges": [{"input": ("shiny new", '1'), "amount": 4.0}]
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

    def test_process_unknown_object(self):
        database = DatabaseChooser("testy")
        database.register()
        data = {
            ("testy", "A"): {},
            ("testy", "B"): {'exchanges': [
                {'input': ("testy", "A"),
                 'amount': 1,
                 'type': 'technosphere'},
                {'input': ("testy", "C"),
                 'amount': 1,
                 'type': 'technosphere'},
            ]},
        }
        with self.assertRaises(UnknownObject):
            database.write(data)

    def test_process_unknown_object_singlefile(self):
        database = DatabaseChooser("testy", backend="singlefile")
        database.register()
        data = {
            ("testy", "A"): {},
            ("testy", "B"): {'exchanges': [
                {'input': ("testy", "A"),
                 'amount': 1,
                 'type': 'technosphere'},
                {'input': ("testy", "C"),
                 'amount': 1,
                 'type': 'technosphere'},
            ]},
        }
        with self.assertRaises(UnknownObject):
            database.write(data)

    def test_untyped_exchange_error(self):
        database = DatabaseChooser("testy")
        database.register()
        database_data = {
            ("testy", "A"): {'exchanges': [
                {'amount': 1, 'input': ('testy', 'A')}
            ]},
        }
        with self.assertRaises(UntypedExchange):
            database.write(database_data, process=False)

    def test_no_input_raises_invalid_exchange(self):
        database = DatabaseChooser("testy")
        database.register()
        database_data = {
            ("testy", "A"): {'exchanges': [
                {'amount': 1}
            ]},
        }
        with self.assertRaises(InvalidExchange):
            database.write(database_data, process=False)

    def test_no_amount_raises_invalid_exchange(self):
        database = DatabaseChooser("testy")
        database.register()
        database_data = {
            ("testy", "A"): {'exchanges': [
                {'input': ('testy', 'A'), 'type': 'technosphere'}
            ]},
        }
        with self.assertRaises(InvalidExchange):
            database.write(database_data, process=False)

    def test_process_geomapping_array(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({})
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
        }, process=True)
        # This shouldn't raise an error
        self.assertEqual(database.process(), None)

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
        database.write({("a database", '2'): {
            'type': 'process',
            'exchanges': [{
                'input': ("a database", '2'),
                'amount': 42,
                'uncertainty type': 7,
                'type': 'production'
            }]
        }})
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
        database.write({("a database", '2'): {
            'type': 'process',
            'exchanges': [{
                'input': ("a database", '2'),
                'amount': 42.,
                'type': 'technosphere'
            }]
        }})
        fp = os.path.join(
            config.dir,
            u"processed",
            database.filename + u".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array.shape, (2,))
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
                    {
                        'input': ("foo", "bar"),
                        'type': 'technosphere',
                        'amount': 0,
                    },
                    {
                        'input': ("biosphere", "bar"),
                        'type': 'technosphere',
                        'amount': 0,
                    },
                    # Ignore becuase of 'ignore'
                    {
                        'input': ("awkward", "silence"),
                        'type': 'technosphere',
                        'amount': 0,
                    },
                    # Ignored because of 'unknown' type
                    {
                        'input': ("who", "am I?"),
                        "type": "unknown",
                        'amount': 0,
                    },
                    {
                        'input': ("biosphere", "bar"),
                        'type': 'technosphere',
                        'amount': 0,
                    },
                ],
                'location': 'bar'
            },
            ("a database", "baz"): {
                'exchanges': [{
                    'input': ("baz", "w00t"),
                    'type': 'technosphere',
                    'amount': 0,
                }],
                'type': 'emission' # Ignored because of type
            },
            ("a database", "nonce"): {},  # OK not to have 'exchanges'
        }, process=False)
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
        self.assertEqual(
            databases['a database']['depends'],
            ["biosphere", "foo", "baz"]
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

    def test_can_split_processes_products(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({
            ("a database", "product"): {'type': 'product'},
            ("a database", "foo"): {
                'exchanges': [{
                        'input': ("a database", "product"),
                        'type': 'production',
                        'amount': 1
                }],
                'type': 'process',
            },
        })
        database.process()
        self.assertTrue(("a database", "product") in mapping)
        fp = os.path.join(
            config.dir,
            u"processed",
            database.filename + u".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array.shape, (1,))
        self.assertEqual(array['output'][0], mapping[("a database", "foo")])
        self.assertEqual(array['input'][0], mapping[("a database", "product")])

class SingleFileDatabaseTest(BW2DataTest):
    # TODO: Better check .write?

    def create_biosphere(self):
        d = SingleFileDatabase("biosphere")
        d.register()
        d.write(biosphere)
        return d

    def test_get(self):
        d = DatabaseChooser("biosphere", backend='singlefile')
        d.register(depends=[])
        d.write(biosphere)
        activity = d.get('1')
        self.assertTrue(isinstance(activity, SFActivity))
        self.assertEqual(activity.name, 'an emission')

    def test_get_random(self):
        d = DatabaseChooser("biosphere", backend='singlefile')
        d.register(depends=[])
        d.write(biosphere)
        activity = iter(d).next()
        self.assertTrue(isinstance(activity, SFActivity))
        self.assertTrue(activity.name in ('an emission', 'another emission'))

    def test_get_random(self):
        d = DatabaseChooser("biosphere", backend='singlefile')
        d.register(depends=[])
        d.write(biosphere)
        activity = d.random()
        self.assertTrue(isinstance(activity, SFActivity))
        self.assertTrue(activity.name in ('an emission', 'another emission'))

    def test_revert(self):
        self.create_biosphere()
        d = SingleFileDatabase("food")
        d.register()
        d.write(food)
        d.write({})
        self.assertEqual(databases["food"]["version"], 2)
        self.assertEqual(SingleFileDatabase("food").load(), {})
        d.revert(1)
        self.assertEqual(databases["food"]["version"], 1)
        self.assertEqual(SingleFileDatabase("food").load(), food)
        with self.assertRaises(AssertionError):
            d.revert(10)

    def test_make_latest_version(self):
        d = self.create_biosphere()
        biosphere2 = copy.deepcopy(biosphere)
        biosphere2[(u"biosphere", u"noodles")] = {}
        for x in range(10):
            d.write(biosphere2)
        self.assertEqual(len(d.versions()), 11)
        d.revert(1)
        d.make_latest_version()
        self.assertEqual(d.version, 12)
        self.assertEqual(d.load(), biosphere)

    def test_versions(self):
        d = self.create_biosphere()
        self.assertEqual(
            [x[0] for x in d.versions()], [1]
        )
        d.write(biosphere)
        self.assertEqual(
            [x[0] for x in d.versions()], [1, 2]
        )

    def test_wrong_version(self):
        d = self.create_biosphere()
        with self.assertRaises(MissingIntermediateData):
            d.load(version=-1)

    def test_noninteger_version(self):
        d = self.create_biosphere()
        with self.assertRaises(ValueError):
            d.load(version="foo")

    def test_register(self):
        database = SingleFileDatabase("testy")
        database.register()
        self.assertEqual(databases['testy']['version'], 0)

    def test_load(self):
        self.create_biosphere()
        d = SingleFileDatabase("food")
        d.register()
        d.write(food)
        data = SingleFileDatabase("food").load()
        self.assertEqual(food, data)

    def test_load_as_dict(self):
        self.create_biosphere()
        d = SingleFileDatabase("food")
        d.register()
        d.write(food)
        data = SingleFileDatabase("food").load(as_dict=True)
        self.assertTrue(isinstance(data, dict))

    def test_db_is_json_serializable(self):
        self.create_biosphere()
        d = SingleFileDatabase("food")
        d.register()
        d.write(food)
        data = SingleFileDatabase("food").load(as_dict=True)
        JsonWrapper.dumps(JsonSanitizer.sanitize(data))

    def test_write_bumps_version_number(self):
        self.create_biosphere()
        d = SingleFileDatabase("food")
        d.register()
        d.write(food)
        self.assertEqual(databases["food"]["version"], 1)
        d.write(food)
        self.assertEqual(databases["food"]["version"], 2)

    def test_validator(self):
        database = SingleFileDatabase("a database")
        self.assertEqual(database.validator, db_validator)
        self.assertTrue(database.validate({}))
