# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from voluptuous import Invalid
import unittest
from ..validate import *


class ValidationTestCase(unittest.TestCase):
    def test_valid_tuple(self):
        with self.assertRaises(Invalid):
            valid_tuple(())
        with self.assertRaises(Invalid):
            valid_tuple(["a", "b"])
        with self.assertRaises(Invalid):
            valid_tuple((1, "b"))
        with self.assertRaises(Invalid):
            valid_tuple(("b", 1))
        self.assertTrue(valid_tuple(("a", "b")))
        with self.assertRaises(Invalid):
            self.assertTrue(valid_tuple(("a", ())))
        with self.assertRaises(Invalid):
            self.assertTrue(valid_tuple(("a", [])))
        self.assertTrue(valid_tuple(("a", "1")))

    def test_uncertainty_dict(self):
        schema = Schema(uncertainty_dict)
        with self.assertRaises(Invalid):
            schema({})
        with self.assertRaises(Invalid):
            schema({'loc': 0})
        with self.assertRaises(Invalid):
            schema({'amount': 0, 'foo': 'bar'})
        self.assertTrue(schema({'amount': 0}))

    def test_maybe_uncertainty(self):
        schema = Schema(maybe_uncertainty)
        self.assertTrue(schema({'amount': 0}))
        self.assertTrue(schema(4))
        self.assertTrue(schema(4.2))

    def test_exchange(self):
        schema = Schema(exchange)
        with self.assertRaises(Invalid):
            schema({})
        with self.assertRaises(Invalid):
            schema({'amount': 1})
        with self.assertRaises(Invalid):
            schema({'input': ('a', 1), 'type': 'foo'})
        self.assertTrue(schema({'amount': 1, 'input': ('a', '1'), 'type': 'foo'}))

    def test_db_validator(self):
        self.assertTrue(db_validator({("a", "1"): {}}))
        self.assertTrue(db_validator({
            ("a", "1"): {
                'type': 'foo',
                'exchanges': [],
                }
        }))
        self.assertTrue(db_validator({
            ("a", "1"): {
                'name': 'foo',
                'exchanges': [],
                }
        }))
        self.assertTrue(db_validator({
            ("a", "1"): {
                'name': 'foo',
                'type': 'bar',
                }
        }))
        self.assertTrue(db_validator({
            ("a", "1"): {
                'name': 'foo',
                'type': 'bar',
                'exchanges': [],
                }
        }))
        self.assertTrue(db_validator({
            ("a", "1"): {
                'name': 'foo',
                'type': 'bar',
                'exchanges': [],
                'night': 'day',
                }
        }))

    def test_ia_validator(self):
        self.assertTrue(ia_validator([[("a", "1"), 2.]]))
        self.assertTrue(ia_validator([[("a", "1"), 2., "CH"]]))
        self.assertTrue(ia_validator([
            [("a", "1"), 2., "CH"],
            [("a", "1"), 2.],
        ]))

    def test_weighting_too_long(self):
        with self.assertRaises(Invalid):
            weighting_validator([{'amount': 0}, {'amount', 0}])

    def test_weighting_too_short(self):
        with self.assertRaises(Invalid):
            weighting_validator([])

    def test_weighting(self):
        self.assertTrue(weighting_validator([{'amount': 0}]))
