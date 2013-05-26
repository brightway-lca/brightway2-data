# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import Database, Method, methods
from fixtures import biosphere
from ..utils import natural_sort, random_string, combine_methods


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
        d.register("Tests", [], len(biosphere))
        d.write(biosphere)
        m1 = Method(["test method 1"])
        m1.register(unit="p", num_cfs=2)
        m1.write([
            (("biosphere", 1), 1, "GLO"),
            (("biosphere", 2), 2, "GLO")
        ])
        m2 = Method(["test method 2"])
        m2.register(unit="p", num_cfs=1)
        m2.write([
            (("biosphere", 2), 10, "GLO")
        ])
        combine_methods(["test method 3"], ["test method 1"],
            ["test method 2"])
        cm = Method(["test method 3"])
        self.assertEqual(sorted(cm.load()), [
            (("biosphere", 1), 1, "GLO"),
            (("biosphere", 2), 12, "GLO")
        ])
        self.assertEqual(methods[["test method 3"]]["unit"], "p")
