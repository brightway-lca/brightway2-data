from .fixtures import food, biosphere
from bw2data import (
    config,
    Database,
    geomapping,
    get_id,
    Method,
    projects,
)
from bw2data.tests import bw2test
from bw_processing import load_datapackage
from fs.zipfs import ZipFS
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


def test_method_process_adds_correct_geo(add_method):
    method = Method(("test method",))
    package = load_datapackage(ZipFS(method.filepath_processed()))
    print(package.resources)

    mapped = {
        row["row"]: row["col"]
        for row in package.get_resource("test_method_matrix_data.indices")[0]
    }
    assert geomapping["foo"] == mapped[get_id(("biosphere", 1))]
    assert geomapping["bar"] == mapped[get_id(("biosphere", 2))]
    assert package.get_resource("test_method_matrix_data.data")[0].shape == (2,)


def test_database_process_adds_correct_geo(add_biosphere):
    database = Database("food")
    database.write(food)

    package = load_datapackage(ZipFS(database.filepath_processed()))
    data = package.get_resource("food_inventory_geomapping_matrix.indices")[0]

    assert geomapping["CA"] in data["col"].tolist()
    assert geomapping["CH"] in data["col"].tolist()


def test_database_process_adds_default_geo(add_biosphere):
    database = Database("food")
    new_food = copy.deepcopy(food)
    for v in new_food.values():
        del v["location"]
    database.write(new_food)

    package = load_datapackage(ZipFS(database.filepath_processed()))
    data = package.get_resource("food_inventory_geomapping_matrix.indices")[0]

    assert np.allclose(data["col"], geomapping[config.global_location])


def test_method_write_adds_to_geomapping(add_method):
    assert "foo" in geomapping
    assert "bar" in geomapping


def test_database_write_adds_to_geomapping(add_biosphere):
    d = Database("food")
    d.write(food, process=False)
    assert "CA" in geomapping
    assert "CH" in geomapping
