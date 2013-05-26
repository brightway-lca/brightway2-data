# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import Database, databases, geomapping, reset_meta, config, Method
from fixtures import food, biosphere
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
        d = Database("biosphere")
        d.register("biosphere", [], len(biosphere))
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
        print geomapping.data
        self.assertTrue("GLO" in geomapping)

    def test_method_adds_correct_geo(self):
        method = self.add_method()
        method.process()
        pickled = pickle.load(open(os.path.join(config.dir, "processed",
            method.get_abbreviation() + ".pickle"), "rb"))
        self.assertEqual(geomapping["foo"], int(pickled[0]["geo"]))
        self.assertEqual(geomapping["bar"], int(pickled[1]["geo"]))
        self.assertEquals(pickled.shape, (2,))

    def test_database_adds_correct_geo(self):
        self.add_biosphere()
        database = Database("food")
        database.register("food", ["biosphere"], len(food))
        database.write(food)
        database.process()
        pickled = pickle.load(open(os.path.join(config.dir, "processed",
            database.database + ".pickle"), "rb"))
        self.assertTrue(geomapping["CA"] in pickled["geo"].tolist())
        self.assertTrue(geomapping["CH"] in pickled["geo"].tolist())

    def test_database_adds_default_geo(self):
        self.add_biosphere()
        database = Database("food")
        database.register("food", ["biosphere"], len(food))
        new_food = copy.deepcopy(food)
        for v in new_food.values():
            del v["location"]
        database.write(new_food)
        database.process()
        pickled = pickle.load(open(os.path.join(config.dir, "processed",
            database.database + ".pickle"), "rb"))
        self.assertTrue(np.allclose(pickled["geo"],
            geomapping["GLO"] * np.ones(pickled.shape)))

    def test_method_write_adds_to_geomapping(self):
        self.add_method()
        self.assertTrue("foo" in geomapping)
        self.assertTrue("bar" in geomapping)

    def test_database_write_adds_to_geomapping(self):
        self.add_biosphere()
        d = Database("food")
        d.register("Tests", ["biosphere"], len(food))
        d.write(food)
        self.assertTrue("CA" in geomapping)
        self.assertTrue("CH" in geomapping)
