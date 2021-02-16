# -*- coding: utf-8 -*-
from bw2data.tests import bw2test
from .fixtures import food as food_data, biosphere, get_naughty
from bw2data.database import DatabaseChooser
from bw2data.backends.peewee import (
    Activity as PWActivity,
    ActivityDataset,
    Exchange as PWExchange,
    ExchangeDataset,
    sqlite3_lci_db,
)
from bw2data.errors import (
    InvalidExchange,
    MissingIntermediateData,
    UnknownObject,
    UntypedExchange,
    ValidityError,
)
from bw2data.errors import WrongDatabase
from bw2data.meta import mapping, geomapping, databases
from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    ParameterizedExchange,
    parameters,
)
# from bw_processing import load_package
import copy
import datetime
import numpy as np
import pytest
import warnings


# package = load_package(method.filepath_processed())


@pytest.fixture
@bw2test
def food():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    d = DatabaseChooser("food")
    d.write(food_data)


def test_food(food):
    assert len(databases) == 2
    assert sorted(x for x in databases) == ["biosphere", "food"]


### Basic functions


@bw2test
def test_get():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    activity = d.get("1")
    assert isinstance(activity, PWActivity)
    assert activity["name"] == "an emission"


@bw2test
def test_iter():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    activity = next(iter(d))
    assert isinstance(activity, PWActivity)
    assert activity["name"] in ("an emission", "another emission")


@bw2test
def test_get_random():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    activity = d.random()
    assert isinstance(activity, PWActivity)
    assert activity["name"] in ("an emission", "another emission")


def test_copy(food):
    d = DatabaseChooser("food")
    with pytest.raises(AssertionError):
        d.copy("food")
    d.copy("repas")
    assert "repas" in databases


@bw2test
def test_copy_does_deepcopy():
    data = {
        ("old name", "1"): {
            "exchanges": [
                {"input": ("old name", "1"), "amount": 1.0, "type": "technosphere"}
            ]
        }
    }
    d = DatabaseChooser("old name")
    d.write(data)
    new_db = d.copy("new name")
    new_data = new_db.load()
    assert list(new_data.values())[0]["exchanges"][0]["input"] == ("new name", "1")
    assert list(data.values())[0]["exchanges"][0]["input"] == ("old name", "1")
    assert list(d.load().values())[0]["exchanges"][0]["input"] == ("old name", "1")


@bw2test
def test_raise_wrong_database():
    data = {("foo", "1"): {}}
    d = DatabaseChooser("bar")
    with pytest.raises(WrongDatabase):
        d.write(data)


@bw2test
def test_deletes_from_database():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    assert "biosphere" in databases
    del databases["biosphere"]
    assert next(
        sqlite3_lci_db.execute_sql(
            "select count(*) from activitydataset where database = 'biosphere'"
        )
    ) == (0,)
    assert next(
        sqlite3_lci_db.execute_sql(
            "select count(*) from exchangedataset where output_database = 'biosphere'"
        )
    ) == (0,)


@bw2test
def test_delete_warning():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    with pytest.warns(UserWarning):
        d.delete()


@bw2test
def test_relabel_data():
    old_data = {
        ("old and boring", "1"): {
            "exchanges": [{"input": ("old and boring", "42"), "amount": 1.0}]
        },
        ("old and boring", "2"): {
            "exchanges": [{"input": ("old and boring", "1"), "amount": 4.0}]
        },
    }
    shiny_new = {
        ("shiny new", "1"): {
            "exchanges": [{"input": ("old and boring", "42"), "amount": 1.0}]
        },
        ("shiny new", "2"): {
            "exchanges": [{"input": ("shiny new", "1"), "amount": 4.0}]
        },
    }
    db = DatabaseChooser("foo")
    assert shiny_new == db.relabel_data(old_data, "shiny new")


### Metadata


@bw2test
def test_find_graph_dependents():
    databases["one"] = {"depends": ["two", "three"]}
    databases["two"] = {"depends": ["four", "five"]}
    databases["three"] = {"depends": ["four"]}
    databases["four"] = {"depends": ["six"]}
    databases["five"] = {"depends": ["two"]}
    databases["six"] = {"depends": []}
    assert DatabaseChooser("one").find_graph_dependents() == {
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
    }


@bw2test
def test_register():
    database = DatabaseChooser("testy")
    database.register()
    assert "testy" in databases
    assert "depends" in databases["testy"]


@bw2test
def test_deregister():
    d = DatabaseChooser("food")
    d.register()
    assert "food" in databases
    d.deregister()
    assert "food" not in databases


@bw2test
def test_write_sets_databases_number_attribute():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    assert databases["biosphere"]["number"] == len(biosphere)


### Processed arrays


# @bw2test
# def test_sqlite_processed_array_order():
#     database = DatabaseChooser("testy")
#     data = {
#         ("testy", "C"): {},
#         ("testy", "A"): {"type": "biosphere"},
#         ("testy", "B"): {
#             "exchanges": [
#                 {"input": ("testy", "A"), "amount": 1, "type": "technosphere"},
#                 {"input": ("testy", "A"), "amount": 2, "type": "technosphere"},
#                 {"input": ("testy", "C"), "amount": 2, "type": "biosphere"},
#                 {"input": ("testy", "C"), "amount": 3, "type": "biosphere"},
#                 {"input": ("testy", "B"), "amount": 4, "type": "production"},
#                 {"input": ("testy", "B"), "amount": 1, "type": "production"},
#             ]
#         },
#     }
#     database.write(data)
#     lookup = {k: mapping[("testy", k)] for k in "ABC"}
#     expected = sorted(
#         [
#             (lookup["B"], lookup["B"], 1),
#             (lookup["B"], lookup["B"], 4),
#             (lookup["C"], lookup["C"], 1),
#             (lookup["A"], lookup["B"], 1),
#             (lookup["A"], lookup["B"], 2),
#         ]
#     )
#     package = load_package(database.filepath_processed())
#     array = package["technosphere_matrix.npy"]
#     assert array.shape == (5,)
#     result = [
#         (array["row_value"][x], array["col_value"][x], array["amount"][x])
#         for x in range(5)
#     ]
#     assert expected == result
#     array = package["biosphere_matrix.npy"]
#     assert array.shape == (2,)


@bw2test
def test_process_adds_to_mappings():
    database = DatabaseChooser("testy")
    database_data = {
        ("testy", "A"): {"location": "CH"},
        ("testy", "B"): {"location": "DE"},
    }
    database.write(database_data)
    assert ("testy", "A") in mapping and ("testy", "B") in mapping
    assert "CH" in geomapping and "DE" in geomapping


@bw2test
def test_process_unknown_object():
    database = DatabaseChooser("testy")
    data = {
        ("testy", "A"): {},
        ("testy", "B"): {
            "exchanges": [
                {"input": ("testy", "A"), "amount": 1, "type": "technosphere"},
                {"input": ("testy", "C"), "amount": 1, "type": "technosphere"},
            ]
        },
    }
    with pytest.raises(UnknownObject):
        database.write(data)


### String handling


@bw2test
def test_naughty_activity_codes():
    db = DatabaseChooser("foo")
    data = {("foo", str(i)): {"name": x} for i, x in enumerate(get_naughty())}
    db.write(data)
    assert set(get_naughty()) == set(x["name"] for x in db)


@bw2test
def test_setup():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    d = DatabaseChooser("food")
    d.write(food_data)


@bw2test
def test_rename():
    d = DatabaseChooser("biosphere")
    d.write(biosphere)
    d = DatabaseChooser("food")
    d.write(copy.deepcopy(food_data))
    ndb = d.rename("buildings")
    ndb_data = ndb.load()
    assert ndb.name == "buildings"
    assert d.name == "buildings"
    assert len(ndb_data) == len(food_data)
    for key in ndb_data:
        assert key[0] == "buildings"
        for exc in ndb_data[key]["exchanges"]:
            assert exc["input"][0] in ("biosphere", "buildings")


@bw2test
def test_exchange_save():
    database = DatabaseChooser("testy")
    data = {
        ("testy", "A"): {},
        ("testy", "C"): {"type": "biosphere"},
        ("testy", "B"): {
            "exchanges": [
                {"input": ("testy", "A"), "amount": 1, "type": "technosphere"},
                {"input": ("testy", "B"), "amount": 1, "type": "production"},
                {"input": ("testy", "C"), "amount": 1, "type": "biosphere"},
            ]
        },
    }
    then = datetime.datetime.now().isoformat()
    database.write(data)
    act = database.get("B")
    exc = [x for x in act.production()][0]
    exc["amount"] = 2
    exc.save()
    assert databases[database.name].get("dirty")
    assert database.metadata.get("dirty")
    assert database.metadata["modified"] > then

    exc = [x for x in act.production()][0]
    assert exc["amount"] == 2


@bw2test
@pytest.mark.skip()
def test_dirty_activities():
    database = DatabaseChooser("testy")
    data = {
        ("testy", "A"): {},
        ("testy", "C"): {"type": "biosphere"},
        ("testy", "B"): {
            "exchanges": [
                {"input": ("testy", "A"), "amount": 1, "type": "technosphere"},
                {"input": ("testy", "B"), "amount": 1, "type": "production"},
                {"input": ("testy", "C"), "amount": 1, "type": "biosphere"},
            ]
        },
    }
    database.write(data)
    act = database.get("B")
    exc = [x for x in act.production()][0]
    exc["amount"] = 2
    exc.save()
    assert databases["testy"]["dirty"]
    lca = act.lca()
    assert not databases["testy"].get("dirty")
    assert lca.supply_array[lca.activity_dict[("testy", "A")]] == 0.5


@bw2test
def test_process_invalid_exchange_value():
    database = DatabaseChooser("testy")
    data = {
        ("testy", "A"): {},
        ("testy", "B"): {
            "exchanges": [
                {"input": ("testy", "A"), "amount": np.nan, "type": "technosphere"},
                {"input": ("testy", "C"), "amount": 1, "type": "technosphere"},
            ]
        },
    }
    with pytest.raises(ValueError):
        database.write(data)


@bw2test
def test_untyped_exchange_error():
    database = DatabaseChooser("testy")
    database_data = {
        ("testy", "A"): {"exchanges": [{"amount": 1, "input": ("testy", "A")}]},
    }
    with pytest.raises(UntypedExchange):
        database.write(database_data, process=False)


@bw2test
def test_no_input_raises_invalid_exchange():
    database = DatabaseChooser("testy")
    database_data = {
        ("testy", "A"): {"exchanges": [{"amount": 1}]},
    }
    with pytest.raises(InvalidExchange):
        database.write(database_data, process=False)


@bw2test
def test_no_amount_raises_invalid_exchange():
    database = DatabaseChooser("testy")
    database_data = {
        ("testy", "A"): {
            "exchanges": [{"input": ("testy", "A"), "type": "technosphere"}]
        },
    }
    with pytest.raises(InvalidExchange):
        database.write(database_data, process=False)


@bw2test
def test_zero_amount_is_valid_exchange():
    database = DatabaseChooser("testy")
    database_data = {
        ("testy", "A"): {
            "exchanges": [
                {"input": ("testy", "A"), "type": "technosphere", "amount": 0.0}
            ]
        },
    }
    database.write(database_data, process=False)


@bw2test
def test_process_checks_process_type():
    database = DatabaseChooser("a database")
    database.write(
        {
            ("a database", "foo"): {"exchanges": [], "type": "process"},
            ("a database", "bar"): {"type": "definitely not a process"},
        },
        process=True,
    )
    # This shouldn't raise an error
    assert database.process() is None


# @bw2test
# def test_geomapping_array_includes_only_processes():
#     database = DatabaseChooser("a database")
#     database.write(
#         {
#             ("a database", "foo"): {
#                 "exchanges": [],
#                 "type": "process",
#                 "location": "bar",
#             },
#             ("a database", "baz"): {"exchanges": [], "type": "emission"},
#         }
#     )
#     package = load_package(database.filepath_processed())
#     array = package["inv_geomapping_matrix.npy"]
#     assert array.shape == (1,)
#     assert array[0]["col_value"] == geomapping["bar"]


# @bw2test
# def test_processed_array():
#     database = DatabaseChooser("a database")
#     database.write(
#         {
#             ("a database", "2"): {
#                 "type": "process",
#                 "exchanges": [
#                     {
#                         "input": ("a database", "2"),
#                         "amount": 42,
#                         "uncertainty_type": 7,
#                         "type": "production",
#                     }
#                 ],
#             }
#         }
#     )
#     package = load_package(database.filepath_processed())
#     array = package["technosphere_matrix.npy"]
#     assert array.shape == (1,)
#     assert array[0]["uncertainty_type"] == 7
#     assert array[0]["amount"] == 42


@bw2test
def test_base_class():
    database = DatabaseChooser("a database")
    assert database._metadata is databases


@bw2test
def test_find_dependents():
    database = DatabaseChooser("a database")
    database.write(
        {
            ("a database", "foo"): {
                "exchanges": [
                    {"input": ("foo", "bar"), "type": "technosphere", "amount": 0,},
                    {
                        "input": ("biosphere", "bar"),
                        "type": "technosphere",
                        "amount": 0,
                    },
                    # Ignore becuase of 'ignore'
                    {
                        "input": ("awkward", "silence"),
                        "type": "technosphere",
                        "amount": 0,
                    },
                    # Ignored because of 'unknown' type
                    {"input": ("who", "am I?"), "type": "unknown", "amount": 0,},
                    {
                        "input": ("biosphere", "bar"),
                        "type": "technosphere",
                        "amount": 0,
                    },
                ],
                "location": "bar",
            },
            ("a database", "baz"): {
                "exchanges": [
                    {"input": ("baz", "w00t"), "type": "technosphere", "amount": 0,}
                ],
                "type": "emission",  # Ignored because of type
            },
            ("a database", "nonce"): {},  # OK not to have 'exchanges'
        },
        process=False,
    )
    assert database.find_dependents(ignore={"awkward"}) == ["biosphere", "foo"]


@bw2test
def test_set_dependents():
    database = DatabaseChooser("a database")
    database.register()
    assert databases["a database"]["depends"] == []
    keys = [("biosphere", "bar"), ("baz", "w00t"), ("foo", "bar")]
    mapping.add(keys)
    database.write(
        {
            ("a database", "foo"): {
                "exchanges": [
                    {"input": ("foo", "bar"), "type": "technosphere", "amount": 1},
                    {"input": ("biosphere", "bar"), "type": "biosphere", "amount": 1,},
                ],
                "type": "process",
                "location": "bar",
            },
            ("a database", "baz"): {
                "exchanges": [
                    {"input": ("baz", "w00t"), "type": "technosphere", "amount": 1}
                ],
                "type": "emission",
            },
        }
    )
    assert databases["a database"]["depends"] == ["baz", "biosphere", "foo"]


# @bw2test
# def test_process_without_exchanges_still_in_processed_array():
#     database = DatabaseChooser("a database")
#     database.write({("a database", "foo"): {}})
#     package = load_package(database.filepath_processed())
#     array = package["technosphere_matrix.npy"]
#     assert array["amount"][0] == 1
#     assert array.shape == (1,)


@bw2test
def test_random_empty():
    database = DatabaseChooser("a database")
    database.write({})
    with warnings.catch_warnings() as w:
        warnings.simplefilter("ignore")
        assert database.random() is None


@bw2test
def test_new_activity():
    database = DatabaseChooser("a database")
    database.register()
    act = database.new_activity("foo", this="that", name="something")
    act.save()

    act = database.get("foo")
    assert act["database"] == "a database"
    assert act["code"] == "foo"
    assert act["location"] == "GLO"
    assert act["this"] == "that"


# @bw2test
# def test_can_split_processes_products():
#     database = DatabaseChooser("a database")
#     database.write(
#         {
#             ("a database", "product"): {"type": "product"},
#             ("a database", "foo"): {
#                 "exchanges": [
#                     {
#                         "input": ("a database", "product"),
#                         "output": ("a database", "product"),
#                         "type": "production",
#                         "amount": 1,
#                     }
#                 ],
#                 "type": "process",
#             },
#         }
#     )
#     assert ("a database", "product") in mapping
#     package = load_package(database.filepath_processed())
#     array = package["technosphere_matrix.npy"]
#     assert array.shape == (1,)
#     assert array["col_value"][0] == mapping[("a database", "foo")]
#     assert array["row_value"][0] == mapping[("a database", "product")]


# @bw2test
# def test_loc_value_if_no_uncertainty():
#     database = DatabaseChooser("a database")
#     database.write(
#         {
#             ("a database", "2"): {
#                 "type": "process",
#                 "exchanges": [
#                     {
#                         "input": ("a database", "2"),
#                         "amount": 42.0,
#                         "type": "technosphere",
#                     }
#                 ],
#             }
#         }
#     )
#     package = load_package(database.filepath_processed())
#     array = package["technosphere_matrix.npy"]
#     assert array.shape == (2,)
#     assert array["loc"][1] == 42.0
#     assert array["loc"][0] == 1.0


@bw2test
def test_database_delete_parameters():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(
        amount=0, input=b, type="technosphere", formula="foo * bar + 4"
    ).save()

    database_data = [
        {"name": "red", "formula": "(blue ** 2) / 5",},
        {"name": "blue", "amount": 12},
    ]
    parameters.new_database_parameters(database_data, "example")

    activity_data = [
        {
            "name": "reference_me",
            "formula": "sqrt(red - 20)",
            "database": "example",
            "code": "B",
        },
        {
            "name": "bar",
            "formula": "reference_me + 2",
            "database": "example",
            "code": "A",
        },
    ]
    parameters.new_activity_parameters(activity_data, "my group")
    parameters.add_exchanges_to_group("my group", a)

    assert ActivityParameter.select().count() == 2
    assert ParameterizedExchange.select().count() == 1
    assert DatabaseParameter.select().count() == 2
    assert len(parameters) == 4

    del databases["example"]
    assert not len(parameters)
    assert not ParameterizedExchange.select().count()
