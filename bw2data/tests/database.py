# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest
from .. import config, projects
from ..database import DatabaseChooser
from ..backends.peewee import (
    Activity as PWActivity,
    ActivityDataset,
    Exchange as PWExchange,
    ExchangeDataset,
    sqlite3_lci_db,
)
from ..backends.utils import convert_backend
from ..backends.single_file.database import SingleFileDatabase
from ..errors import (
    InvalidExchange,
    MissingIntermediateData,
    UnknownObject,
    UntypedExchange,
    ValidityError,
)
from ..backends.single_file import (
    Activity as SFActivity,
    Exchange as SFExchange,
)
from ..errors import NotAllowed, WrongDatabase
from ..meta import mapping, geomapping, databases, methods
from ..serialization import JsonWrapper, JsonSanitizer
from ..utils import numpy_string, get_activity
from ..validate import db_validator
from .fixtures import food, biosphere, get_naughty
from peewee import DoesNotExist
import copy
import datetime
import numpy as np
import os
import pickle
import warnings


class PeeweeProxyTest(BW2DataTest):
    def get_activity(self):
        database = DatabaseChooser("a database")
        database.write({
            ("a database", "foo"): {
                'exchanges': [{
                    'input': ("a database", "foo"),
                    'amount': 1,
                    'type': 'production',
                }],
                'location': 'bar',
                'name': 'baz'
            },
        })
        return database.get('foo')

    def test_set_item(self):
        act = self.get_activity()
        act['foo'] = 'bar'
        act.save()
        act = DatabaseChooser("a database").get("foo")
        self.assertEqual(act['foo'], 'bar')

    def test_key(self):
        act = self.get_activity()
        self.assertEqual(act.key, ("a database", "foo"))

    def test_change_code(self):
        act = self.get_activity()
        db = DatabaseChooser("a database")
        self.assertEqual(len(db), 1)
        old_key = act.key[:]
        act['code'] = 'a new one'
        self.assertEqual(len(db), 1)
        self.assertTrue(get_activity(("a database", "a new one")))
        with self.assertRaises(DoesNotExist):
            get_activity(old_key)

    def test_change_code_same_code(self):
        act = self.get_activity()
        act['code'] = 'foo'

    def test_change_database(self):
        act = self.get_activity()
        db = DatabaseChooser("a database")
        db2 = DatabaseChooser("another database")
        db2.write({})
        self.assertEqual(len(db2), 0)
        self.assertEqual(len(db), 1)
        old_key = act.key[:]
        self.assertEqual(len(get_activity(old_key).production()), 1)
        act['database'] = "another database"
        self.assertEqual(len(db), 0)
        self.assertEqual(len(db2), 1)
        self.assertTrue(get_activity(("another database", "foo")))
        self.assertEqual(len(get_activity(("another database", "foo")).production()), 1)
        with self.assertRaises(DoesNotExist):
            get_activity(old_key)

    def test_change_database_not_exist(self):
        act = self.get_activity()
        with self.assertRaises(ValueError):
            act['database'] = "nope!"

    def test_database_same_database(self):
        act = self.get_activity()
        act['database'] = "a database"

    def test_change_code_not_unique(self):
        database = DatabaseChooser("a database")
        database.write({
            ("a database", "foo"): {
                'exchanges': [{
                    'input': ("a database", "foo"),
                    'amount': 1,
                    'type': 'production',
                }],
                'location': 'bar',
                'name': 'baz'
            },
            ("a database", "already there"): {
                'exchanges': [{
                    'input': ("a database", "already there"),
                    'amount': 1,
                    'type': 'production',
                }],
                'location': 'bar',
                'name': 'baz'
            },
        })
        act = database.get('foo')
        with self.assertRaises(ValueError):
            act['code'] = "already there"

    def test_delete(self):
        act = self.get_activity()
        self.assertEqual(ExchangeDataset.select().count(), 1)
        self.assertEqual(ActivityDataset.select().count(), 1)
        act.delete()
        self.assertEqual(ExchangeDataset.select().count(), 0)
        self.assertEqual(ActivityDataset.select().count(), 0)

    def test_save_invalid(self):
        db = DatabaseChooser("a database")
        db.register()
        act = db.new_activity("foo")
        with self.assertRaises(ValidityError):
            act.save()

    def test_copy(self):
        act = self.get_activity()
        self.assertEqual(ExchangeDataset.select().count(), 1)
        self.assertEqual(ActivityDataset.select().count(), 1)
        cp = act.copy("baz")
        self.assertFalse(cp['code'] == act['code'])
        self.assertEqual(cp['name'], 'baz')
        self.assertEqual(cp['location'], 'bar')
        self.assertEqual(ExchangeDataset.select().count(), 2)
        self.assertEqual(ActivityDataset.select().count(), 2)
        self.assertEqual(ActivityDataset.select().where(
            ActivityDataset.code == cp['code'],
            ActivityDataset.database == cp['database'],
        ).count(), 1)
        self.assertEqual(ActivityDataset.select().where(
            ActivityDataset.code == act['code'],
            ActivityDataset.database == act['database'],
        ).count(), 1)
        self.assertEqual(ExchangeDataset.select().where(
            ExchangeDataset.input_code == cp['code'],
            ExchangeDataset.input_database == cp['database'],
        ).count(), 1)
        self.assertEqual(ExchangeDataset.select().where(
            ExchangeDataset.input_database == act['database'],
            ExchangeDataset.input_code == act['code'],
        ).count(), 1)

    def test_find_graph_dependents(self):
        databases['one'] = {'depends': ['two', 'three']}
        databases['two'] = {'depends': ['four', 'five']}
        databases['three'] = {'depends': ['four']}
        databases['four'] = {'depends': ['six']}
        databases['five'] = {'depends': ['two']}
        databases['six'] = {'depends': []}
        self.assertEqual(
            DatabaseChooser('one').find_graph_dependents(),
            {'one', 'two', 'three', 'four', 'five', 'six'}
        )

class ExchangeTest(BW2DataTest):
    def extra_setup(self):
        self.database = DatabaseChooser("db")
        self.database.write({
            ("db", "a"): {
                'exchanges': [{
                    'input': ("db", "a"),
                    'amount': 2,
                    'type': 'production',
                }, {
                    'input': ("db", "b"),
                    'amount': 3,
                    'type': 'technosphere',
                }, {
                    'input': ("db", "c"),
                    'amount': 4,
                    'type': 'biosphere',
                }],
                'name': 'a'
            },
            ("db", "b"): {'name': 'b'},
            ("db", "c"): {'name': 'c', 'type': 'biosphere'},
            ("db", "d"): {
                'name': 'd',
                'exchanges': [{
                    'input': ("db", "a"),
                    'amount': 5,
                    'type': 'technosphere'
                }]
            },
        })
        self.act = self.database.get("a")

    def test_setup_clean(self):
        self.assertEqual(len(databases), 1)
        self.assertEqual(list(methods), [])
        self.assertEqual(len(mapping), 4)
        self.assertEqual(len(geomapping), 1)  # GLO
        self.assertTrue("GLO" in geomapping)
        self.assertEqual(len(projects), 1)  # Default project
        self.assertTrue("default" in projects)

    def test_production(self):
        self.assertEqual(
            len(list(self.act.production())),
            1
        )
        self.assertEqual(
            len(self.act.production()),
            1
        )
        exc = list(self.act.production())[0]
        self.assertEqual(exc['amount'], 2)

    def test_biosphere(self):
        self.assertEqual(
            len(list(self.act.biosphere())),
            1
        )
        self.assertEqual(
            len(self.act.biosphere()),
            1
        )
        exc = list(self.act.biosphere())[0]
        self.assertEqual(exc['amount'], 4)

    def test_technosphere(self):
        self.assertEqual(
            len(list(self.act.technosphere())),
            1
        )
        self.assertEqual(
            len(self.act.technosphere()),
            1
        )
        exc = list(self.act.technosphere())[0]
        self.assertEqual(exc['amount'], 3)

    def test_upstream(self):
        self.assertEqual(
            len(list(self.act.upstream())),
            1
        )
        self.assertEqual(
            len(self.act.upstream()),
            1
        )
        exc = list(self.act.upstream())[0]
        self.assertEqual(exc['amount'], 5)


class DatabaseTest(BW2DataTest):
    def test_setup(self):
        d = DatabaseChooser("biosphere")
        d.write(biosphere)
        d = DatabaseChooser("food")
        d.write(food)
        self.assertEqual(len(databases), 2)

    def test_naughty_names(self):
        db = DatabaseChooser("foo")
        data = {("foo", str(i)): {'name': x} for i, x in enumerate(get_naughty())}
        db.write(data)
        self.assertEqual(
            set(get_naughty()),
            set(x['name'] for x in db)
        )

    def test_get(self):
        d = DatabaseChooser("biosphere")
        d.write(biosphere)
        activity = d.get('1')
        self.assertTrue(isinstance(activity, PWActivity))
        self.assertEqual(activity['name'], 'an emission')

    def test_iter(self):
        d = DatabaseChooser("biosphere")
        d.write(biosphere)
        activity = next(iter(d))
        self.assertTrue(isinstance(activity, PWActivity))
        self.assertTrue(activity['name'] in ('an emission', 'another emission'))

    def test_get_random(self):
        d = DatabaseChooser("biosphere")
        d.write(biosphere)
        activity = d.random()
        self.assertTrue(isinstance(activity, PWActivity))
        self.assertTrue(activity['name'] in ('an emission', 'another emission'))

    def test_copy(self):
        d = DatabaseChooser("biosphere")
        d.write(biosphere)
        d = DatabaseChooser("food")
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
        d.write(data)
        new_db = d.copy("new name")
        new_data = new_db.load()
        self.assertEqual(
            list(new_data.values())[0]['exchanges'][0]['input'],
            ('new name', '1')
        )
        self.assertEqual(
            list(data.values())[0]['exchanges'][0]['input'],
            ('old name', '1')
        )
        self.assertEqual(
            list(d.load().values())[0]['exchanges'][0]['input'],
            ('old name', '1')
        )

    def test_raise_wrong_database(self):
        data = {
            ("foo", '1'): {}
        }
        d = DatabaseChooser("bar")
        with self.assertRaises(WrongDatabase):
            d.write(data)

    def test_deletes_from_database(self):
        d = DatabaseChooser("biosphere")
        d.write(biosphere)
        self.assertTrue("biosphere" in databases)
        del databases['biosphere']
        self.assertEqual(
            next(sqlite3_lci_db.execute_sql(
                "select count(*) from activitydataset where database = 'biosphere'"
            )),
            (0,)
        )
        self.assertEqual(
            next(sqlite3_lci_db.execute_sql(
                "select count(*) from exchangedataset where output_database = 'biosphere'"
            )),
            (0,)
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
        d.register()
        self.assertTrue("food" in databases)
        d.deregister()
        self.assertTrue("food" not in databases)

    def test_rename(self):
        d = DatabaseChooser("biosphere")
        d.write(biosphere)
        d = DatabaseChooser("food")
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
        d.write(biosphere)
        self.assertEqual(databases["biosphere"]["number"], len(biosphere))

    def test_process_adds_to_mappings(self):
        database = DatabaseChooser("testy")
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

    def test_exchange_save(self):
        database = DatabaseChooser("testy")
        data = {
            ("testy", "A"): {},
            ("testy", "C"): {'type': 'biosphere'},
            ("testy", "B"): {'exchanges': [
                {'input': ("testy", "A"),
                 'amount': 1,
                 'type': 'technosphere'},
                {'input': ("testy", "B"),
                 'amount': 1,
                 'type': 'production'},
                {'input': ("testy", "C"),
                 'amount': 1,
                 'type': 'biosphere'},
            ]},
        }
        then = datetime.datetime.now().isoformat()
        database.write(data)
        act = database.get("B")
        exc = [x for x in act.production()][0]
        exc['amount'] = 2
        exc.save()
        self.assertTrue(databases[database.name].get("dirty"))
        self.assertTrue(database.metadata.get("dirty"))
        self.assertTrue(database.metadata['modified'] > then)

        exc = [x for x in act.production()][0]
        self.assertEqual(exc['amount'], 2)

    def test_dirty_activities(self):
        database = DatabaseChooser("testy")
        data = {
            ("testy", "A"): {},
            ("testy", "C"): {'type': 'biosphere'},
            ("testy", "B"): {'exchanges': [
                {'input': ("testy", "A"),
                 'amount': 1,
                 'type': 'technosphere'},
                {'input': ("testy", "B"),
                 'amount': 1,
                 'type': 'production'},
                {'input': ("testy", "C"),
                 'amount': 1,
                 'type': 'biosphere'},
            ]},
        }
        database.write(data)
        act = database.get("B")
        exc = [x for x in act.production()][0]
        exc['amount'] = 2
        exc.save()
        self.assertTrue(databases['testy']['dirty'])
        lca = act.lca()
        self.assertFalse(databases['testy'].get('dirty'))
        self.assertEqual(
            lca.supply_array[lca.activity_dict[("testy", "A")]],
            0.5
        )

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

    def test_process_invalid_exchange_value(self):
        database = DatabaseChooser("testy")
        database.register()
        data = {
            ("testy", "A"): {},
            ("testy", "B"): {'exchanges': [
                {'input': ("testy", "A"),
                 'amount': np.nan,
                 'type': 'technosphere'},
                {'input': ("testy", "C"),
                 'amount': 1,
                 'type': 'technosphere'},
            ]},
        }
        with self.assertRaises(ValueError):
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

    def test_zero_amount_is_valid_exchange(self):
        database = DatabaseChooser("testy")
        database.register()
        database_data = {
            ("testy", "A"): {'exchanges': [
                {'input': ('testy', 'A'), 'type': 'technosphere', 'amount': 0.}
            ]},
        }
        database.write(database_data, process=False)

    def test_process_geomapping_array(self):
        database = DatabaseChooser("a database")
        database.register()
        database.write({})
        fp = os.path.join(
            projects.dir,
            "processed",
            database.filename + ".geomapping.pickle"
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
            projects.dir,
            "processed",
            database.filename + ".geomapping.pickle"
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
            projects.dir,
            "processed",
            database.filename + ".pickle"
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
            projects.dir,
            "processed",
            database.filename + ".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array.shape, (2,))
        self.assertEqual(array['loc'][0], 42.)

    def test_base_class(self):
        database = DatabaseChooser("a database")
        self.assertEqual(database._metadata, databases)
        self.assertEqual(
            [x[0] for x in database.dtype_fields],
            [numpy_string(x) for x in ('input', 'output', 'row', 'col', 'type')]
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
            ["baz", "biosphere", "foo"]
        )

    def test_process_without_exchanges_still_in_processed_array(self):
        database = DatabaseChooser("a database")
        database.write({("a database", "foo"): {}})
        fp = os.path.join(
            projects.dir,
            "processed",
            database.filename + ".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array['amount'][0], 1)
        self.assertEqual(array.shape, (1,))

    def test_random_empty(self):
        database = DatabaseChooser("a database")
        database.write({})
        with warnings.catch_warnings() as w:
            warnings.simplefilter("ignore")
            self.assertEqual(database.random(), None)

    def test_new_activity(self):
        database = DatabaseChooser("a database")
        database.register()
        act = database.new_activity('foo', this="that", name='something')
        act.save()

        act = database.get('foo')
        self.assertEqual(act['database'], 'a database')
        self.assertEqual(act['code'], 'foo')
        self.assertEqual(act['location'], 'GLO')
        self.assertEqual(act['this'], 'that')

    def test_can_split_processes_products(self):
        database = DatabaseChooser("a database")
        database.write({
            ("a database", "product"): {'type': 'product'},
            ("a database", "foo"): {
                'exchanges': [{
                        'input': ("a database", "product"),
                        'output': ("a database", "product"),
                        'type': 'production',
                        'amount': 1
                }],
                'type': 'process',
            },
        })
        self.assertTrue(("a database", "product") in mapping)
        fp = os.path.join(
            projects.dir,
            "processed",
            database.filename + ".pickle"
        )
        array = pickle.load(open(fp, "rb"))
        self.assertEqual(array.shape, (1,))
        self.assertEqual(array['output'][0], mapping[("a database", "foo")])
        self.assertEqual(array['input'][0], mapping[("a database", "product")])


class DatabaseQuerysetTest(BW2DataTest):
    def extra_setup(self):
        self.db = DatabaseChooser("Order!")
        self.db.write({
            ("Order!", "first"): {
                'name': 'a',
                'location': 'delaware',
                'reference product': 'widget',
                },
            ("Order!", "second"): {
                'name': 'b',
                'location': 'carolina',
                'reference product': 'wiggle',
                },
            ("Order!", "third"): {
                'name': 'c',
                'location': 'baseball',
                'reference product': 'lollipop',
                },
            ("Order!", "fourth"): {
                'name': 'd',
                'location': 'alabama',
                'reference product': 'widget',
                },
        })

    def test_setup_clean(self):
        self.assertEqual(len(databases), 1)
        self.assertTrue("Order!" in databases)
        self.assertEqual(list(methods), [])
        self.assertEqual(len(mapping), 4)
        self.assertTrue(("Order!", "fourth") in mapping)
        self.assertEqual(len(geomapping), 5)  # GLO
        self.assertTrue("GLO" in geomapping)
        self.assertTrue("carolina" in geomapping)
        self.assertEqual(len(projects), 1)  # Default project
        self.assertTrue("default" in projects)

    def test_random_respects_filters(self):
        self.db.filters = {'product': 'lollipop'}
        self.assertEqual(self.db.random()['name'], 'c')

    def test_get_ignores_filters(self):
        self.db.filters = {'product': 'giggles'}
        self.assertEqual(self.db.get('fourth')['name'], 'd')

    def test_filter(self):
        self.db.filters = {'product': 'widget'}
        self.assertEqual(len([x for x in self.db]), 2)

    def test_order_by(self):
        self.db.order_by = 'name'
        self.assertEqual(
            [x['name'] for x in self.db],
            ['a', 'b', 'c', 'd']
        )

    def test_order_by_bad_field(self):
        with self.assertRaises(AssertionError):
            self.db.order_by = 'poopy'

    def test_filter_bad_field(self):
        with self.assertRaises(AssertionError):
            self.db.filters = {'poopy': 'yuck'}

    def test_filter_not_dict(self):
        with self.assertRaises(AssertionError):
            self.db.filters = 'poopy'

    def test_reset_order_by(self):
        self.db.order_by = 'name'
        self.db.order_by = None
        self.assertFalse(
            [x['name'] for x in self.db] == \
            ['a', 'b', 'c', 'd']
        )

    def test_reset_filters(self):
        self.db.filters = {'product': 'widget'}
        self.assertEqual(len([x for x in self.db]), 2)
        self.db.filters = None
        self.assertEqual(len([x for x in self.db]), 4)

    def test_len_respects_filters(self):
        self.db.filters = {'product': 'widget'}
        self.assertEqual(len(self.db), 2)

    def test_make_searchable_unknown_object(self):
        db = DatabaseChooser("mysterious")
        with self.assertRaises(UnknownObject):
            db.make_searchable()

    def test_convert_same_backend(self):
        database = DatabaseChooser("a database")
        database.write({
            ("a database", "foo"): {
                'exchanges': [{
                    'input': ("a database", "foo"),
                    'amount': 1,
                    'type': 'production',
                }],
                'location': 'bar',
                'name': 'baz'
            },
        })
        self.assertFalse(convert_backend('a database', "sqlite"))

    def test_convert_backend(self):
        self.maxDiff = None
        database = DatabaseChooser("a database")
        database.write({
            ("a database", "foo"): {
                'exchanges': [{
                    'input': ("a database", "foo"),
                    'amount': 1,
                    'type': 'production',
                }],
                'location': 'bar',
                'name': 'baz'
            },
        })
        database = convert_backend('a database', "singlefile")
        self.assertEqual(databases['a database']['backend'], 'singlefile')
        self.assertEqual(databases['a database']['version'], 1)
        expected = {
            ("a database", "foo"): {
                'code': 'foo',
                'database': 'a database',
                'exchanges': [{
                    'input': ("a database", "foo"),
                    'output': ("a database", "foo"),
                    'amount': 1,
                    'type': 'production',
                }],
                'location': 'bar',
                'name': 'baz'
            },
        }
        self.assertEqual(database.load(), expected)


class SingleFileDatabaseTest(BW2DataTest):
    # TODO: Better check .write?

    def create_biosphere(self, process=True):
        d = SingleFileDatabase("biosphere")
        d.write(biosphere, process=process)
        return d

    def test_random_empty(self):
        database = SingleFileDatabase("a database")
        database.write({})
        with warnings.catch_warnings() as w:
            warnings.simplefilter("ignore")
            self.assertEqual(database.random(), None)

    def test_delete_cache(self):
        self.assertFalse("biosphere" in config.cache)
        d = self.create_biosphere(False)
        self.assertFalse(d.name in config.cache)
        d.load()
        self.assertTrue(d.name in config.cache)
        del databases[d.name]
        self.assertFalse(d.name in config.cache)

    def test_get(self):
        d = self.create_biosphere()
        activity = d.get('1')
        self.assertTrue(isinstance(activity, SFActivity))
        self.assertEqual(activity['name'], 'an emission')

    def test_get_random(self):
        d = self.create_biosphere()
        activity = next(iter(d))
        self.assertTrue(isinstance(activity, SFActivity))
        self.assertTrue(activity['name'] in ('an emission', 'another emission'))

    def test_get_random(self):
        d = self.create_biosphere()
        activity = d.random()
        self.assertTrue(isinstance(activity, SFActivity))
        self.assertTrue(activity['name'] in ('an emission', 'another emission'))

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
        biosphere2[("biosphere", "noodles")] = {}
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

    def test_process_invalid_exchange_value(self):
        database = SingleFileDatabase("testy")
        database.register()
        data = {
            ("testy", "A"): {},
            ("testy", "B"): {'exchanges': [
                {'input': ("testy", "A"),
                 'amount': np.nan,
                 'type': 'technosphere'},
                {'input': ("testy", "C"),
                 'amount': 1,
                 'type': 'technosphere'},
            ]},
        }
        with self.assertRaises(ValueError):
            database.write(data)

    def test_delete(self):
        d = SingleFileDatabase("biosphere")
        d.write(biosphere)
        fp = d.filepath_intermediate()
        self.assertTrue("biosphere" in databases)
        del databases['biosphere']
        self.assertFalse("biosphere" in databases)
        self.assertTrue(os.path.exists(fp))
