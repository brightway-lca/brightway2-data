# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest
from ..database import Database
from ..database_parameters import database_parameters, DatabaseParameterSet


class DatabaseParameterTest(BW2DataTest):
    def extra_setup(self):
        self.db = Database("ttttttt")
        self.p = self.db.parameters
        self.p['foo'] = {'amount': 1}
        self.p['bar'] = {'formula': 'foo * 2'}

    def test_contains(self):
        self.assertTrue('foo' in self.p)

    def test_len(self):
        self.assertEqual(len(self.p), 2)

    def test_hash_equality(self):
        self.assertEqual(
            self.p,
            {'foo': {'amount': 1}, 'bar': {'formula': 'foo * 2'}}
        )

    def test_str(self):
        self.assertEqual(str(self.p), "Database parameter set with 2 values")

    def test_repr(self):
        self.assertEqual(repr(self.p), "Database parameter set with 2 values")

    def test_list(self):
        self.assertEqual(
            sorted(list(self.p)),
            ['bar', 'foo']
        )

    def test_iter(self):
        self.assertEqual(
            sorted([x for x in self.p]),
            ['bar', 'foo']
        )

    def test_evaluate(self):
        self.assertEqual(
            self.p.evaluate(),
            {'bar': 2, 'foo': 1}
        )

    def test_original_data_right_form(self):
        self.assertEqual(
            database_parameters['ttttttt'],
            {'foo': {'amount': 1}, 'bar': {'formula': 'foo * 2'}}
        )
