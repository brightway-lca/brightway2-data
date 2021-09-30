from .fixtures import biosphere
from bw2data import Database, Method, methods
from bw2data.backends import Activity as PWActivity
from bw2data.errors import ValidityError
from bw2data.tests import BW2DataTest, bw2test
from bw2data.utils import (
    as_uncertainty_dict,
    combine_methods,
    get_activity,
    merge_databases,
    natural_sort,
    random_string,
    uncertainify,
)

# from bw_processing import load_package
import pytest
import stats_arrays as sa


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
        m1.write([(("biosphere", 1), 1, "GLO"), (("biosphere", 2), 2, "GLO")])
        m2 = Method(("test method 2",))
        m2.register(unit="p")
        m2.write([(("biosphere", 2), 10, "GLO")])
        combine_methods(("test method 3",), ("test method 1",), ("test method 2",))
        cm = Method(("test method 3",))
        self.assertEqual(
            sorted(cm.load()),
            [(("biosphere", 1), 1, "GLO"), (("biosphere", 2), 12, "GLO")],
        )
        self.assertEqual(methods[["test method 3"]]["unit"], "p")


class UncertainifyTestCase(BW2DataTest):
    def test_wrong_distribution(self):
        with self.assertRaises(AssertionError):
            uncertainify({}, sa.LognormalUncertainty)

    def test_factors_valid(self):
        with self.assertRaises(AssertionError):
            uncertainify({}, bounds_factor=-1)
        with self.assertRaises(TypeError):
            uncertainify({}, bounds_factor="foo")
        with self.assertRaises(AssertionError):
            uncertainify({}, sd_factor=-1)
        with self.assertRaises(TypeError):
            uncertainify({}, sd_factor="foo")

    def test_bounds_factor_none_ok(self):
        uncertainify({}, bounds_factor=None)

    def test_skips(self):
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

    def test_uniform(self):
        data = {1: {"exchanges": [{"amount": 10.0}]}}
        data = uncertainify(data)
        new_dict = {
            "amount": 10.0,
            "minimum": 9.0,
            "maximum": 11.0,
            "uncertainty type": sa.UniformUncertainty.id,
        }
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_normal_bounded(self):
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

    def test_normal_unbounded(self):
        data = {1: {"exchanges": [{"amount": 10.0}]}}
        data = uncertainify(data, sa.NormalUncertainty, bounds_factor=None)
        new_dict = {
            "amount": 10.0,
            "loc": 10.0,
            "scale": 1.0,
            "uncertainty type": sa.NormalUncertainty.id,
        }
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_normal_negative_amount(self):
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

    def test_bounds_flipped_negative_amount(self):
        data = {1: {"exchanges": [{"amount": -10.0}]}}
        data = uncertainify(data)
        new_dict = {
            "amount": -10.0,
            "minimum": -11.0,
            "maximum": -9.0,
            "uncertainty type": sa.UniformUncertainty.id,
        }
        self.assertEqual(data[1]["exchanges"][0], new_dict)

    def test_skip_zero_amounts(self):
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


@bw2test
def test_merge_databases_nonunique_activity_codes():
    first = Database("a database")
    first.write(
        {
            ("a database", "foo"): {
                "exchanges": [
                    {"input": ("a database", "foo"), "amount": 1, "type": "production",}
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
                    {"input": ("a database", "foo"), "amount": 1, "type": "production",}
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
                    {"input": ("a database", "foo"), "amount": 1, "type": "production",}
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
#     print(merged.filepath_processed())
#     package = load_package(merged.filepath_processed())
#     print(package.keys())
#     array = package["technosphere_matrix.npy"]
#     assert mapping[("a database", "bar")] in {x["col_value"] for x in array}
#     assert mapping[("a database", "foo")] in {x["col_value"] for x in array}
