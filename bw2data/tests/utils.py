# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest
from .. import Database, Method, methods
from ..backends.peewee import Activity as PWActivity
from ..backends.single_file import Activity as SFActivity
from ..database import Database
from .fixtures import biosphere
from ..utils import (
    combine_methods,
    get_activity,
    natural_sort,
    random_string,
    safe_filename,
    uncertainify,
)
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
