import numpy as np
import pytest
import stats_arrays as sa

from bw2data import Database, Method, labels, methods
from bw2data.backends import Activity as PWActivity
from bw2data.errors import MultipleResults, UnknownObject, ValidityError
from bw2data.snowflake_ids import EPOCH_START_MS
from bw2data.tests import BW2DataTest, bw2test
from bw2data.utils import (
    as_uncertainty_dict,
    combine_methods,
    get_activity,
    get_node,
    merge_databases,
    natural_sort,
    random_string,
    set_correct_process_type,
    uncertainify,
)

from .fixtures import biosphere


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
        d.register(depends=[])
        d.write(biosphere)
        m1 = Method(("test method 1",))
        m1.register(unit="p")
        m1.write([(("biosphere", "1"), 1, "GLO"), (("biosphere", "2"), 2, "GLO")])
        m2 = Method(("test method 2",))
        m2.register(unit="p")
        m2.write([(("biosphere", "2"), 10, "GLO")])
        combine_methods(("test method 3",), ("test method 1",), ("test method 2",))
        cm = Method(("test method 3",))
        self.assertEqual(
            sorted(cm.load()),
            [(get_node(code="1").id, 1, "GLO"), (get_node(code="2").id, 12, "GLO")],
        )
        self.assertEqual(methods[["test method 3"]]["unit"], "p")


class UncertainifyTestCase(BW2DataTest):
    def test_wrong_distribution(self):
        with self.assertRaises(AssertionError):
            uncertainify({}, sa.LognormalUncertainty)

    def test_uncertainify_factors_valid(self):
        with self.assertRaises(AssertionError):
            uncertainify({}, bounds_factor=-1)
        with self.assertRaises(TypeError):
            uncertainify({}, bounds_factor="foo")
        with self.assertRaises(AssertionError):
            uncertainify({}, sd_factor=-1)
        with self.assertRaises(TypeError):
            uncertainify({}, sd_factor="foo")

    def test_uncertainify_bounds_factor_none_ok(self):
        uncertainify({}, bounds_factor=None)

    def test_uncertainify_skips(self):
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

    def test_uncertainify_uniform(self):
        data = {1: {"exchanges": [{"amount": 10.0}]}}
        data = uncertainify(data)
        new_dict = {
            "amount": 10.0,
            "minimum": 9.0,
            "maximum": 11.0,
            "uncertainty type": sa.UniformUncertainty.id,
        }
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_uncertainify_normal_bounded(self):
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
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_uncertainify_normal_unbounded(self):
        data = {1: {"exchanges": [{"amount": 10.0}]}}
        data = uncertainify(data, sa.NormalUncertainty, bounds_factor=None)
        new_dict = {
            "amount": 10.0,
            "loc": 10.0,
            "scale": 1.0,
            "uncertainty type": sa.NormalUncertainty.id,
        }
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_uncertainify_normal_negative_amount(self):
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
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_uncertainify_bounds_flipped_negative_amount(self):
        data = {1: {"exchanges": [{"amount": -10.0}]}}
        data = uncertainify(data)
        new_dict = {
            "amount": -10.0,
            "minimum": -11.0,
            "maximum": -9.0,
            "uncertainty type": sa.UniformUncertainty.id,
        }
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_uncertainify_skip_zero_amounts(self):
        data = {1: {"exchanges": [{"amount": 0.0}]}}
        data = uncertainify(data)
        new_dict = {
            "amount": 0.0,
        }
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_get_activity_peewee(self):
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
        self.assertTrue(isinstance(get_activity(("a database", "foo")), PWActivity))


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
    assert node.id > EPOCH_START_MS
    assert isinstance(node, PWActivity)


@bw2test
def test_get_node_key():
    Database("biosphere").write(biosphere)
    node = get_node(key=("biosphere", "2"))
    assert node["name"] == "another emission"
    assert isinstance(node, PWActivity)

    with pytest.raises(ValueError):
        get_node(key=["biosphere", "2"])
    with pytest.raises(ValueError):
        get_node(key="2")


@bw2test
def test_get_node_multiple_filters():
    Database("biosphere").write(biosphere)
    node = get_node(name="an emission", type="emission")
    assert node.id > EPOCH_START_MS
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
    node = get_node(code="1")
    found = get_activity(node)
    assert found is node


@bw2test
def test_get_activity_id():
    Database("biosphere").write(biosphere)
    node = get_activity(code="1")
    node = get_activity(node.id)
    assert node.id > EPOCH_START_MS
    assert isinstance(node, PWActivity)


@bw2test
def test_get_activity_key():
    Database("biosphere").write(biosphere)
    node = get_activity(("biosphere", "1"))
    assert node.id > EPOCH_START_MS
    assert isinstance(node, PWActivity)


@bw2test
def test_get_activity_kwargs():
    Database("biosphere").write(biosphere)
    node = get_activity(name="an emission", type="emission")
    assert node.id > EPOCH_START_MS
    assert isinstance(node, PWActivity)


@bw2test
def test_merge_databases_nonunique_activity_codes():
    first = Database("a database")
    first.write(
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
    second = Database("another database")
    second.write(
        {
            ("another database", "foo"): {
                "exchanges": [
                    {
                        "input": ("another database", "foo"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "baz",
            },
        }
    )
    with pytest.raises(ValidityError):
        merge_databases("a database", "another database")


@bw2test
def test_merge_databases_wrong_backend():
    first = Database("a database", "iotable")
    first.write(
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
        },
    )
    second = Database("another database")
    second.write(
        {
            ("another database", "bar"): {
                "exchanges": [
                    {
                        "input": ("another database", "bar"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "baz",
            },
        }
    )
    with pytest.raises(ValidityError):
        merge_databases("a database", "another database")
    with pytest.raises(ValidityError):
        merge_databases("another database", "a database")


@bw2test
def test_merge_databases_nonexistent():
    first = Database("a database")
    first.write(
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
    with pytest.raises(AssertionError):
        merge_databases("a database", "another database")
    with pytest.raises(AssertionError):
        merge_databases("another database", "a database")


def test_set_correct_process_type():
    data = [
        # Completely empty -> implicit production - chimaera
        ({"database": "a", "code": "b"}, labels.chimaera_node_default),
        # Exchanges but not production -> implicit production - chimaera
        (
            {"database": "a", "code": "b", "exchanges": [{"type": "biosphere"}]},
            labels.chimaera_node_default,
        ),
        # Exchanges with different functional input - process
        (
            {
                "database": "a",
                "code": "b",
                "exchanges": [{"functional": True, "input": "something"}],
            },
            labels.process_node_default,
        ),
        # Exchanges with same functional input - chimaera
        (
            {
                "database": "a",
                "code": "b",
                "exchanges": [{"functional": True, "input": ("a", "b")}],
            },
            labels.chimaera_node_default,
        ),
        # Exchanges with different production output - process
        (
            {
                "database": "a",
                "code": "b",
                "exchanges": [{"type": labels.production_edge_default, "input": "something"}],
            },
            labels.process_node_default,
        ),
        # Exchanges with same production output - chimaera
        (
            {
                "database": "a",
                "code": "b",
                "exchanges": [{"type": labels.production_edge_default, "input": ("a", "b")}],
            },
            labels.chimaera_node_default,
        ),
        # Exchanges with different production output and labelled `process` - process
        (
            {
                "database": "a",
                "code": "b",
                "type": "process",
                "exchanges": [{"type": labels.production_edge_default, "input": "something"}],
            },
            labels.process_node_default,
        ),
        # Exchanges with same production output and labelled `process` - chimaera
        (
            {
                "database": "a",
                "code": "b",
                "type": "process",
                "exchanges": [{"type": labels.production_edge_default, "input": ("a", "b")}],
            },
            labels.chimaera_node_default,
        ),
        # Exchanges with substitution output - process
        (
            {
                "database": "a",
                "code": "b",
                "exchanges": [{"type": labels.substitution_edge_default}],
            },
            labels.process_node_default,
        ),
        # Exchanges with substitution output and labelled `process` - process
        (
            {
                "database": "a",
                "code": "b",
                "type": "process",
                "exchanges": [{"type": labels.substitution_edge_default}],
            },
            labels.process_node_default,
        ),
        # No production but self-reference in input - chimaera
        (
            {
                "database": "a",
                "code": "b",
                "exchanges": [
                    {"type": labels.technosphere_negative_edge_types, "input": ("a", "b")}
                ],
            },
            labels.chimaera_node_default,
        ),
        # No production but self-reference in input and labelled `process` - chimaera
        (
            {
                "database": "a",
                "code": "b",
                "type": "process",
                "exchanges": [
                    {"type": labels.technosphere_negative_edge_types, "input": ("a", "b")}
                ],
            },
            labels.chimaera_node_default,
        ),
        # Biosphere
        (
            {"database": "a", "code": "b", "type": labels.biosphere_node_default},
            labels.biosphere_node_default,
        ),
        # Multifunctional
        (
            {"database": "a", "code": "b", "type": labels.multifunctional_node_default},
            labels.multifunctional_node_default,
        ),
        # Product
        (
            {"database": "a", "code": "b", "type": labels.product_node_default},
            labels.product_node_default,
        ),
        # Already processwithreferenceproduct
        (
            {"database": "a", "code": "b", "type": labels.chimaera_node_default},
            labels.chimaera_node_default,
        ),
    ]
    for ds, label in data:
        assert set_correct_process_type(ds)["type"] == label


# @bw2test
# def test_merge_databases():
#     first = Database("a database")
#     first.write(
#         {
#             ("a database", "foo"): {
#                 "exchanges": [
#                     {"input": ("a database", "foo"), "amount": 1, "type": "production",}
#                 ],
#                 "location": "bar",
#                 "name": "baz",
#             },
#         }
#     )
#     second = Database("another database")
#     second.write(
#         {
#             ("another database", "bar"): {
#                 "exchanges": [
#                     {
#                         "input": ("another database", "bar"),
#                         "amount": 1,
#                         "type": "production",
#                     }
#                 ],
#                 "location": "bar",
#                 "name": "baz",
#             },
#         }
#     )
#     merge_databases("a database", "another database")
#     merged = Database("a database")
#     assert len(merged) == 2
#     assert "another database" not in databases
#     assert ("a database", "bar") in mapping
#     package = load_package(merged.filepath_processed())
#     array = package["technosphere_matrix.npy"]
#     assert mapping[("a database", "bar")] in {x["col_value"] for x in array}
#     assert mapping[("a database", "foo")] in {x["col_value"] for x in array}
