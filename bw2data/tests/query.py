# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..query import *
from .fixtures import food
import collections
import unittest


class FilterTest(unittest.TestCase):
    def test_invalid_function_operator(self):
        with self.assertRaises(ValueError):
            Filter("foo", "bar", "baz")

    def test_le(self):
        self.assertEqual(
            [("food", "1")],
            list(Filter("code", "<=", 1.5)(food).keys())
        )

    def test_lt(self):
        self.assertEqual(
            [("food", "1")],
            list(Filter("code", "<", 1.5)(food).keys())
        )

    def test_eq(self):
        self.assertEqual(
            [("food", "1")],
            list(Filter("code", "==", 1)(food).keys())
        )
        self.assertEqual(
            [("food", "1")],
            list(Filter("code", "is", 1)(food).keys())
        )

    def test_ne(self):
        self.assertEqual(
            [("food", "1")],
            list(Filter("code", "!=", 2)(food).keys())
        )
        self.assertEqual(
            [("food", "1")],
            list(Filter("code", "<>", 2)(food).keys())
        )
        self.assertEqual(
            [("food", "1")],
            list(Filter("code", "not", 2)(food).keys())
        )

    def test_inot(self):
        self.assertEqual(
            [("food", "2")],
            list(Filter("name", "inot", "LuNCH")(food).keys())
        )

    def test_gt(self):
        self.assertEqual(
            [("food", "2")],
            list(Filter("code", ">=", 1.5)(food).keys())
        )

    def test_ge(self):
        self.assertEqual(
            [("food", "2")],
            list(Filter("code", ">", 1.5)(food).keys())
        )

    def test_has(self):
        self.assertEqual(
            [("food", "1")],
            list(Filter("name", "has", "lun")(food).keys())
        )

    def test_ihas(self):
        self.assertEqual(
            [("food", "1")],
            list(Filter("name", "ihas", "LUN")(food).keys())
        )

    def test_nothas(self):
        self.assertEqual(
            [("food", "1")],
            list(Filter("name", "nothas", "inner")(food).keys())
        )

    def test_in(self):
        self.assertEqual(
            [("food", "1")],
            list(Filter("name", "in", {'lunch', 'breakfast'})(food).keys())
        )
        self.assertEqual(
            [("food", "1")],
            list(Filter("name", "in", 'lunchbreakfast')(food).keys())
        )

    def test_notin(self):
        self.assertEqual(
            [("food", "2")],
            list(Filter("name", "notin", {'lunch', 'breakfast'})(food).keys())
        )

    def test_len(self):
        self.assertEqual(
            [("food", "2")],
            list(Filter("name", "len", 6)(food).keys())
        )

    def test_custom_function(self):
        def my_func(x, y):
            return x + y == "lunchlunch!"
        self.assertEqual(
            [("food", "1")],
            list(Filter("name", my_func, 'lunch!')(food).keys())
        )

    def test_NF(self):
        self.assertEqual(
            [("food", "1")],
            list(NF("lun")(food).keys())
        )

    def test_PF(self):
        ds = {
            'a': {'reference product': 'foo'},
            'b': {'reference product': 'bar'},
        }
        self.assertEqual(
            ['a'],
            list(PF("fo")(ds).keys())
        )

    def test_missing_attributes(self):
        self.assertEqual(
            {},
            Filter("Elvis", "is", 'missing')({'a': {}, 'b': {}})
        )

    def test_results_dict(self):
        self.assertEqual(
            dict,
            type(Filter("name", "has", "lun")(food))
        )


class QueryTest(unittest.TestCase):
    def test_call(self):
        query = Query(NF("lun"), Filter("name", "nothas", "inner"))
        self.assertEqual(
            query(food).result,
            {("food", "1"): food[("food", "1")]}
        )

    def test_add(self):
        query = Query(NF("lun"))
        query.add(Filter("name", "nothas", "inner"))
        self.assertEqual(
            query(food).result,
            {("food", "1"): food[("food", "1")]}
        )


class ResultTest(unittest.TestCase):
    def r(self):
        return Result({x: {'name': 'foo%s' % x} for x in range(40)})

    def test_str(self):
        self.assertEqual(
            "Query result with 1 entries",
            str(Result({1: 2}))
        )

    def test_repr(self):
        self.assertEqual(
            u"Query result:\n\tNo query results found.",
            repr(Result({}))
        )
        self.assertEqual(
            repr(self.r()),
            u"""Query result: (total 40)
0: foo0
1: foo1
2: foo2
3: foo3
4: foo4
5: foo5
6: foo6
7: foo7
8: foo8
9: foo9
10: foo10
11: foo11
12: foo12
13: foo13
14: foo14
15: foo15
16: foo16
17: foo17
18: foo18
19: foo19"""
        )


    def test_sort_repr(self):
        r = self.r()
        r.sort('name', reverse=True)
        self.assertEqual(
            repr(r),
            u"""Query result: (total 40)
9: foo9
8: foo8
7: foo7
6: foo6
5: foo5
4: foo4
39: foo39
38: foo38
37: foo37
36: foo36
35: foo35
34: foo34
33: foo33
32: foo32
31: foo31
30: foo30
3: foo3
29: foo29
28: foo28
27: foo27"""
        )


    def test_wrong_arguments(self):
        with self.assertRaises(ValueError):
            Result([])

    def test_sort(self):
        result = self.r()
        result.sort('name', True)
        self.assertTrue(isinstance(result.result, collections.OrderedDict))
        # Sort by name - 'foo9' sorts highest
        self.assertEqual(list(result.result.keys())[0], 9)

    def test_len(self):
        self.assertEqual(40, len(self.r()))

    def test_iter(self):
        self.assertEqual(0, next(iter(self.r())))

    def test_keys(self):
        self.assertEqual(list(range(40)), list(self.r().keys()))

    def test_items(self):
        self.assertEqual(
            list(self.r().items()),
            list({x: {'name': 'foo%s' % x} for x in range(40)}.items())
        )
        self.assertEqual(
            list(self.r().items()),
            list({x: {'name': 'foo%s' % x} for x in range(40)}.items())
        )

    def test_contains(self):
        self.assertTrue(22 in self.r())

    def test_get(self):
        self.assertEqual(self.r()[22], {"name": "foo22"})
