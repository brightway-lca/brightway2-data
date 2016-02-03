# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..serialization import JsonSanitizer
import unittest


class JsonSantizierTestCase(unittest.TestCase):
    def test_tuple(self):
        self.assertEqual(
            JsonSanitizer.sanitize((1,)),
            {'__tuple__': True, 'data': [1]}
        )
        self.assertEqual(
            JsonSanitizer.load({'__tuple__': True, 'data': [1]}),
            (1,)
        )

    def test_dict(self):
        self.assertEqual(
            JsonSanitizer.sanitize({1: 2}),
            {'__dict__': True, 'keys': [1], 'values': [2]}
        )
        self.assertEqual(
            JsonSanitizer.load({'__dict__': True, 'keys': [1], 'values': [2]}),
            {1: 2}
        )

    def test_nested(self):
        input_data = [
            {(1, 2): "foo"},
            ["bar", (5, 6)],
            {},
            tuple([]),
            ((7,),)
        ]
        expected = [
            {'__dict__': True, 'keys': [{'__tuple__': True, 'data': [1, 2]}], 'values': ["foo"]},
            ["bar", {'__tuple__': True, 'data': [5, 6]}],
            {'__dict__': True, 'keys': [], 'values': []},
            {'__tuple__': True, 'data': []},
            {'__tuple__': True, 'data': [{'__tuple__': True, 'data': [7]}]},
        ]
        self.assertEqual(JsonSanitizer.sanitize(input_data), expected)
        self.assertEqual(JsonSanitizer.load(expected), input_data)
