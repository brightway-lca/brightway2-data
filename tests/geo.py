# -*- coding: utf-8 -*-
from .fixtures import food, biosphere
from bw2data import (
    config,
    Database,
    geomapping,
    mapping,
    Method,
    projects,
)
from bw2data.tests import bw2test
# from bw_processing import load_package
import copy
import numpy as np
import pytest


@pytest.fixture
@bw2test
def add_biosphere():
    Database("biosphere").write(biosphere)


@pytest.fixture
def add_method(add_biosphere):
    method = Method(("test method",))
    method.register(unit="kg")
    method.write([(("biosphere", 1), 6, "foo"), (("biosphere", 2), 5, "bar")])


@bw2test
def test_geomapping_retrieval():
    geomapping.add(["foobar"])
    assert "foobar" in geomapping
    geomapping.__init__()
    assert "foobar" in geomapping


@bw2test
def test_glo_always_present():
    assert config.global_location in geomapping


# def test_method_process_adds_correct_geo(add_method):
#     method = Method(("test method",))
#     package = load_package(method.filepath_processed())
#     mapped = {
#         row["row_value"]: row["col_value"]
#         for row in package["characterization_matrix.npy"]
#     }
#     assert geomapping["foo"] == mapped[mapping[("biosphere", 1)]]
#     assert geomapping["bar"] == mapped[mapping[("biosphere", 2)]]
#     assert package["characterization_matrix.npy"].shape == (2,)


# def test_database_process_adds_correct_geo(add_biosphere):
#     database = Database("food")
#     database.write(food)
#     gm = load_package(database.filepath_processed())["inv_geomapping_matrix.npy"]
#     assert geomapping["CA"] in gm["col_value"].tolist()
#     assert geomapping["CH"] in gm["col_value"].tolist()


# def test_database_process_adds_default_geo(add_biosphere):
#     database = Database("food")
#     new_food = copy.deepcopy(food)
#     for v in new_food.values():
#         del v["location"]
#     database.write(new_food)
#     gm = load_package(database.filepath_processed())["inv_geomapping_matrix.npy"]
#     assert np.allclose(gm["col_value"], geomapping["GLO"] * np.ones(gm.shape))


def test_method_write_adds_to_geomapping(add_method):
    assert "foo" in geomapping
    assert "bar" in geomapping


def test_database_write_adds_to_geomapping(add_biosphere):
    d = Database("food")
    d.write(food, process=False)
    assert "CA" in geomapping
    assert "CH" in geomapping
