# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest
from .. import Database, geomapping, config, Method, projects
from .fixtures import food, biosphere
import copy
import numpy as np
import os
import pickle


class GeoTest(BW2DataTest):
    def extra_setup(self):
        geomapping.data = {}
        geomapping.flush()
        geomapping.__init__()

    def add_biosphere(self):
        d = Database(u"biosphere")
        d.register(depends=[])
        d.write(biosphere)

    def add_method(self):
        self.add_biosphere()
        method = Method(("test method",))
        method.register(unit="kg")
        method.write([
            (("biosphere", 1), 6, "foo"),
            (("biosphere", 2), 5, "bar")
        ])
        return method

    def test_geomapping_retrieval(self):
        geomapping.add(["foobar"])
        self.assertTrue("foobar" in geomapping)
        geomapping.__init__()
        self.assertTrue("foobar" in geomapping)

    def test_glo_always_present(self):
        self.assertTrue(config.global_location in geomapping)

    def test_method_adds_correct_geo(self):
        method = self.add_method()
        pickled = pickle.load(open(os.path.join(projects.dir, "processed",
            method.get_abbreviation() + ".pickle"), "rb"))
        self.assertEqual(geomapping["foo"], int(pickled[0]["geo"]))
        self.assertEqual(geomapping["bar"], int(pickled[1]["geo"]))
        self.assertEqual(pickled.shape, (2,))

    # TODO: Adapt or remove
    def test_database_adds_correct_geo(self):
        return
        self.add_biosphere()
        database = Database("food")
        database.register(depends=["biosphere"])
        database.write(food)
        pickled = pickle.load(open(os.path.join(projects.dir, "processed",
            database.filename + ".pickle"), "rb"))
        self.assertTrue(geomapping["CA"] in pickled["geo"].tolist())
        self.assertTrue(geomapping["CH"] in pickled["geo"].tolist())

    # TODO: Adapt to geomapping processed data
    def test_database_adds_default_geo(self):
        return
        self.add_biosphere()
        database = Database("food")
        database.register(depends=["biosphere"])
        new_food = copy.deepcopy(food)
        for v in new_food.values():
            del v["location"]
        database.write(new_food)
        pickled = pickle.load(open(os.path.join(projects.dir, "processed",
            database.filename + ".pickle"), "rb"))
        self.assertTrue(np.allclose(pickled["geo"],
            geomapping["GLO"] * np.ones(pickled.shape)))

    def test_method_write_adds_to_geomapping(self):
        self.add_method()
        self.assertTrue("foo" in geomapping)
        self.assertTrue("bar" in geomapping)

    def test_database_write_adds_to_geomapping(self):
        self.add_biosphere()
        d = Database("food")
        d.register(depends=["biosphere"])
        d.write(food, process=False)
        self.assertTrue("CA" in geomapping)
        self.assertTrue("CH" in geomapping)
