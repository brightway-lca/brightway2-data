# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import bw2test, BW2DataTest
from .fixtures import food, biosphere, get_naughty
from bw2data import config, projects
from bw2data.database import DatabaseChooser
from bw2data.backends.peewee import (
    Activity as PWActivity,
    ActivityDataset,
    Exchange as PWExchange,
    ExchangeDataset,
)
from bw2data.backends.utils import convert_backend
from bw2data.backends.single_file.database import SingleFileDatabase
from bw2data.errors import (
    InvalidExchange,
    MissingIntermediateData,
    UnknownObject,
    UntypedExchange,
    ValidityError,
)
from bw2data.backends.single_file import (
    Activity as SFActivity,
    Exchange as SFExchange,
)
from bw2data.errors import NotAllowed, WrongDatabase
from bw2data.meta import mapping, geomapping, databases, methods
from bw2data.serialization import JsonWrapper, JsonSanitizer
from bw2data.utils import numpy_string, get_activity
from bw2data.validate import db_validator
from peewee import DoesNotExist
import copy
import datetime
import numpy as np
import os
import pickle
import pytest
import warnings


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
