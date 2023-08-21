import numpy as np
import pytest
import stats_arrays as sa

from bw2data import Database, Method, methods
from bw2data.backends import Activity as PWActivity
from bw2data.errors import MultipleResults, UnknownObject, ValidityError
from bw2data.tests import bw2test
from bw2data.utils import (
    as_uncertainty_dict,
    get_activity,
    get_node,
    uncertainify,
)

from .fixtures import biosphere


def test_wrong_distribution():
    with pytest.raises(AssertionError):
        uncertainify({}, sa.LognormalUncertainty)


def test_uncertainify_factors_valid():
    with pytest.raises(AssertionError):
        uncertainify({}, bounds_factor=-1)
    with pytest.raises(TypeError):
        uncertainify({}, bounds_factor="foo")
    with pytest.raises(AssertionError):
        uncertainify({}, sd_factor=-1)
    with pytest.raises(TypeError):
        uncertainify({}, sd_factor="foo")


def test_uncertainify_bounds_factor_none_ok():
    uncertainify({}, bounds_factor=None)


def test_uncertainify_skips():
    data = {
        1: {
            "exchanges": [
                {"type": "production"},
                {"uncertainty type": sa.LognormalUncertainty.id},
            ]
        }
    }
    # Doesn't raise KeyError for 'amount'
    data = uncertainify(data)


def test_uncertainify_uniform():
    data = {1: {"exchanges": [{"amount": 10.0}]}}
    data = uncertainify(data)
    new_dict = {
        "amount": 10.0,
        "minimum": 9.0,
        "maximum": 11.0,
        "uncertainty type": sa.UniformUncertainty.id,
    }
    assert data[1]["exchanges"][0] == new_dict


def test_uncertainify_normal_bounded():
    data = {1: {"exchanges": [{"amount": 10.0}]}}
    data = uncertainify(data, sa.NormalUncertainty)
    new_dict = {
        "amount": 10.0,
        "loc": 10.0,
        "scale": 1.0,
        "minimum": 9.0,
        "maximum": 11.0,
        "uncertainty type": sa.NormalUncertainty.id,
    }
    assert data[1]["exchanges"][0] == new_dict


def test_uncertainify_normal_unbounded():
    data = {1: {"exchanges": [{"amount": 10.0}]}}
    data = uncertainify(data, sa.NormalUncertainty, bounds_factor=None)
    new_dict = {
        "amount": 10.0,
        "loc": 10.0,
        "scale": 1.0,
        "uncertainty type": sa.NormalUncertainty.id,
    }
    assert data[1]["exchanges"][0] == new_dict


def test_uncertainify_normal_negative_amount():
    data = {1: {"exchanges": [{"amount": -10.0}]}}
    data = uncertainify(data, sa.NormalUncertainty)
    new_dict = {
        "amount": -10.0,
        "loc": -10.0,
        "scale": 1.0,
        "minimum": -11.0,
        "maximum": -9.0,
        "uncertainty type": sa.NormalUncertainty.id,
    }
    assert data[1]["exchanges"][0] == new_dict


def test_uncertainify_bounds_flipped_negative_amount():
    data = {1: {"exchanges": [{"amount": -10.0}]}}
    data = uncertainify(data)
    new_dict = {
        "amount": -10.0,
        "minimum": -11.0,
        "maximum": -9.0,
        "uncertainty type": sa.UniformUncertainty.id,
    }
    assert data[1]["exchanges"][0] == new_dict


def test_uncertainify_skip_zero_amounts():
    data = {1: {"exchanges": [{"amount": 0.0}]}}
    data = uncertainify(data)
    new_dict = {
        "amount": 0.0,
    }
    assert data[1]["exchanges"][0] == new_dict


@bw2test
def test_get_activity_peewee():
    database = Database("a database", "sqlite")
    database.write(
        {
            ("a database", "foo"): {
                "exchanges": [
                    {
                        "input": ("a database", "foo"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "baz",
            },
        }
    )
    assert isinstance(get_activity(("a database", "foo")), PWActivity)


def test_as_uncertainty_dict():
    assert as_uncertainty_dict({}) == {}
    assert as_uncertainty_dict(1) == {"amount": 1.0}
    with pytest.raises(TypeError):
        as_uncertainty_dict("foo")


def test_as_uncertainty_dict_set_negative():
    given = {"uncertainty_type": 2, "amount": 1}
    expected = {"uncertainty_type": 2, "amount": 1}
    assert as_uncertainty_dict(given) == expected

    given = {"uncertainty_type": 2, "amount": -1}
    expected = {"uncertainty_type": 2, "amount": -1, "negative": True}
    assert as_uncertainty_dict(given) == expected

    given = {"uncertainty type": 2, "amount": -1}
    expected = {"uncertainty type": 2, "amount": -1, "negative": True}
    assert as_uncertainty_dict(given) == expected

    given = {"uncertainty_type": 8, "amount": -1}
    expected = {"uncertainty_type": 8, "amount": -1, "negative": True}
    assert as_uncertainty_dict(given) == expected

    given = {"uncertainty_type": 3, "amount": -1}
    expected = {"uncertainty_type": 3, "amount": -1}
    assert as_uncertainty_dict(given) == expected

    given = {"uncertainty_type": 3}
    expected = {"uncertainty_type": 3}
    assert as_uncertainty_dict(given) == expected

    given = {"uncertainty_type": 8, "amount": -1, "negative": False}
    expected = {"uncertainty_type": 8, "amount": -1, "negative": False}
    assert as_uncertainty_dict(given) == expected


@bw2test
def test_get_node_normal():
    Database("biosphere").write(biosphere)
    node = get_node(name="an emission")
    assert node.id == 1
    assert isinstance(node, PWActivity)


@bw2test
def test_get_node_multiple_filters():
    Database("biosphere").write(biosphere)
    node = get_node(name="an emission", type="emission")
    assert node.id == 1
    assert isinstance(node, PWActivity)


@bw2test
def test_get_node_nonunique():
    Database("biosphere").write(biosphere)
    with pytest.raises(MultipleResults):
        get_node(type="emission")


@bw2test
def test_get_node_no_node():
    Database("biosphere").write(biosphere)
    with pytest.raises(UnknownObject):
        get_node(type="product")


@bw2test
def test_get_node_extended_search():
    data = {
        ("biosphere", "1"): {
            "categories": ["things"],
            "code": "1",
            "exchanges": [],
            "name": "an emission",
            "type": "emission",
            "unit": "kg",
        },
        ("biosphere", "2"): {
            "categories": ["things"],
            "code": "2",
            "exchanges": [],
            "type": "emission",
            "name": "another emission",
            "unit": "kg",
            "foo": "bar",
        },
    }
    Database("biosphere").write(data)
    with pytest.warns(UserWarning):
        node = get_node(unit="kg", foo="bar")
    assert node["code"] == "2"


@bw2test
def test_get_activity_activity():
    Database("biosphere").write(biosphere)
    node = get_node(id=1)
    found = get_activity(node)
    assert found is node


@bw2test
def test_get_activity_id():
    Database("biosphere").write(biosphere)
    node = get_activity(1)
    assert node.id == 1
    assert isinstance(node, PWActivity)


@bw2test
def test_get_activity_id_different_ints():
    Database("biosphere").write(biosphere)
    different_ints = [
        int(1),
        np.int0(1),
        np.int8(1),
        np.int16(1),
        np.int32(1),
        np.int64(1),
    ]
    for i in different_ints:
        node = get_activity(i)
        assert node.id == i
        assert isinstance(node, PWActivity)


@bw2test
def test_get_activity_key():
    Database("biosphere").write(biosphere)
    node = get_activity(("biosphere", "1"))
    assert node.id == 1
    assert isinstance(node, PWActivity)


@bw2test
def test_get_activity_kwargs():
    Database("biosphere").write(biosphere)
    node = get_activity(name="an emission", type="emission")
    assert node.id == 1
    assert isinstance(node, PWActivity)
