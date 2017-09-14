# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest, bw2test
from .fixtures import biosphere
from bw2data import Database, Method, methods, databases, mapping
from bw2data.backends.peewee import Activity as PWActivity
from bw2data.backends.single_file import Activity as SFActivity
from bw2data.database import Database
from bw2data.errors import ValidityError
from bw2data.utils import (
    combine_methods,
    get_activity,
    merge_databases,
    natural_sort,
    random_string,
    safe_filename,
    uncertainify,
)
import numpy as np
import pytest
import stats_arrays as sa


class UtilsTest(BW2DataTest):
    def test_natural_sort(self):
        data = ["s100", "s2", "s1"]
        self.assertEqual(["s1", "s2", "s100"], natural_sort(data))

    def test_random_string(self):
        s = random_string(10)
        self.assertEqual(len(s), 10)
        self.assertTrue(isinstance(s, str))

    def test_combine_methods(self):
        d = Database("biosphere")
        d.register(depends=[])
        d.write(biosphere)
        m1 = Method(("test method 1",))
        m1.register(unit="p")
        m1.write([
            (("biosphere", 1), 1, "GLO"),
            (("biosphere", 2), 2, "GLO")
        ])
        m2 = Method(("test method 2",))
        m2.register(unit="p")
        m2.write([
            (("biosphere", 2), 10, "GLO")
        ])
        combine_methods(("test method 3",), ("test method 1",),
            ("test method 2",))
        cm = Method(("test method 3",))
        self.assertEqual(sorted(cm.load()), [
            (("biosphere", 1), 1, "GLO"),
            (("biosphere", 2), 12, "GLO")
        ])
        self.assertEqual(methods[["test method 3"]]["unit"], "p")

    def test_safe_filename_unicode_input(self):
        self.assertTrue(safe_filename(u"Søren Kierkegaard"))


class UncertainifyTestCase(BW2DataTest):
    def test_wrong_distribution(self):
        with self.assertRaises(AssertionError):
            uncertainify({}, sa.LognormalUncertainty)

    def test_factors_valid(self):
        with self.assertRaises(AssertionError):
            uncertainify({}, bounds_factor=-1)
        with self.assertRaises(TypeError):
            uncertainify({}, bounds_factor="foo")
        with self.assertRaises(AssertionError):
            uncertainify({}, sd_factor=-1)
        with self.assertRaises(TypeError):
            uncertainify({}, sd_factor="foo")

    def test_bounds_factor_none_ok(self):
        uncertainify({}, bounds_factor=None)

    def test_skips(self):
        data = {1: {'exchanges': [
            {'type': 'production'},
            {'uncertainty type': sa.LognormalUncertainty.id}
        ]}}
        # Doesn't raise KeyError for 'amount'
        data = uncertainify(data)

    def test_uniform(self):
        data = {1: {'exchanges': [
            {'amount': 10.}
        ]}}
        data = uncertainify(data)
        new_dict = {
            'amount': 10.,
            'minimum': 9.,
            'maximum': 11.,
            'uncertainty type': sa.UniformUncertainty.id,
        }
        self.assertEqual(data[1]['exchanges'][0], new_dict)

    def test_normal_bounded(self):
        data = {1: {'exchanges': [
            {'amount': 10.}
        ]}}
        data = uncertainify(data, sa.NormalUncertainty)
        new_dict = {
            'amount': 10.,
            'loc': 10.,
            'scale': 1.,
            'minimum': 9.,
            'maximum': 11.,
            'uncertainty type': sa.NormalUncertainty.id,
        }
        self.assertEqual(data[1]['exchanges'][0], new_dict)

    def test_normal_unbounded(self):
        data = {1: {'exchanges': [
            {'amount': 10.}
        ]}}
        data = uncertainify(data, sa.NormalUncertainty, bounds_factor=None)
        new_dict = {
            'amount': 10.,
            'loc': 10.,
            'scale': 1.,
            'uncertainty type': sa.NormalUncertainty.id,
        }
        self.assertEqual(data[1]['exchanges'][0], new_dict)

    def test_normal_negative_amount(self):
        data = {1: {'exchanges': [
            {'amount': -10.}
        ]}}
        data = uncertainify(data, sa.NormalUncertainty)
        new_dict = {
            'amount': -10.,
            'loc': -10.,
            'scale': 1.,
            'minimum': -11.,
            'maximum': -9.,
            'uncertainty type': sa.NormalUncertainty.id,
        }
        self.assertEqual(data[1]['exchanges'][0], new_dict)

    def test_bounds_flipped_negative_amount(self):
        data = {1: {'exchanges': [
            {'amount': -10.}
        ]}}
        data = uncertainify(data)
        new_dict = {
            'amount': -10.,
            'minimum': -11.,
            'maximum': -9.,
            'uncertainty type': sa.UniformUncertainty.id,
        }
        self.assertEqual(data[1]['exchanges'][0], new_dict)

    def test_skip_zero_amounts(self):
        data = {1: {'exchanges': [
            {'amount': 0.}
        ]}}
        data = uncertainify(data)
        new_dict = {
            'amount': 0.,
        }
        self.assertEqual(data[1]['exchanges'][0], new_dict)

    def test_get_activity_peewee(self):
        database = Database("a database", "sqlite")
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
        self.assertTrue(isinstance(
            get_activity(("a database", "foo")),
            PWActivity
        ))
        self.assertTrue(isinstance(
            Database.get(("a database", "foo")),
            PWActivity
        ))

    def test_get_activity_singlefile(self):
        database = Database("a database", "singlefile")
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
        self.assertTrue(isinstance(
            get_activity(("a database", "foo")),
            SFActivity
        ))
        self.assertTrue(isinstance(
            Database.get(("a database", "foo")),
            SFActivity
        ))


@bw2test
def test_merge_databases_nonunique_activity_codes():
    first = Database("a database")
    first.write({
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
    second = Database("another database")
    second.write({
        ("another database", "foo"): {
            'exchanges': [{
                'input': ("another database", "foo"),
                'amount': 1,
                'type': 'production',
            }],
            'location': 'bar',
            'name': 'baz'
        },
    })
    with pytest.raises(ValidityError):
        merge_databases("a database", "another database")

@bw2test
def test_merge_databases_wrong_backend():
    first = Database("a database", "singlefile")
    first.write({
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
    second = Database("another database")
    second.write({
        ("another database", "bar"): {
            'exchanges': [{
                'input': ("another database", "bar"),
                'amount': 1,
                'type': 'production',
            }],
            'location': 'bar',
            'name': 'baz'
        },
    })
    with pytest.raises(ValidityError):
        merge_databases("a database", "another database")
    with pytest.raises(ValidityError):
        merge_databases("another database", "a database")

@bw2test
def test_merge_databases_nonexistent():
    first = Database("a database")
    first.write({
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
    with pytest.raises(AssertionError):
        merge_databases("a database", "another database")
    with pytest.raises(AssertionError):
        merge_databases("another database", "a database")

@bw2test
def test_merge_databases():
    first = Database("a database")
    first.write({
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
    second = Database("another database")
    second.write({
        ("another database", "bar"): {
            'exchanges': [{
                'input': ("another database", "bar"),
                'amount': 1,
                'type': 'production',
            }],
            'location': 'bar',
            'name': 'baz'
        },
    })
    merge_databases("a database", "another database")
    assert len(Database("a database")) == 2
    assert "another database" not in databases
    assert ("a database", "bar") in mapping
    array = np.load(Database("a database").filepath_processed())
    assert mapping[("a database", "bar")] in {x['output'] for x in array}
    assert mapping[("a database", "foo")] in {x['output'] for x in array}
