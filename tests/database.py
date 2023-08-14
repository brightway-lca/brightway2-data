import copy
import datetime
import warnings

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from bw2data import Database, databases, geomapping, get_activity, get_id
from bw2data.backends import Activity as PWActivity
from bw2data.backends import sqlite3_lci_db
from bw2data.database import Database
from bw2data.errors import (
    DuplicateNode,
    InvalidExchange,
    UnknownObject,
    UntypedExchange,
    WrongDatabase,
)
from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    ParameterizedExchange,
    parameters,
)

from .fixtures import bw2test
from .fixtures import biosphere
from .fixtures import food as food_data
from .fixtures import get_naughty


@pytest.fixture
@bw2test
def food():
    d = Database("biosphere")
    d.write(biosphere)
    d = Database("food")
    d.write(food_data)


def test_food(food):
    assert len(databases) == 2
    assert sorted(x for x in databases) == ["biosphere", "food"]


### Basic functions


@bw2test
def test_get_code():
    d = Database("biosphere")
    d.write(biosphere)
    activity = d.get_node("1")
    assert isinstance(activity, PWActivity)
    assert activity["name"] == "an emission"
    assert activity.id == 1


@bw2test
def test_get_kwargs():
    d = Database("biosphere")
    d.write(biosphere)
    activity = d.get_node(name="an emission")
    assert isinstance(activity, PWActivity)
    assert activity["name"] == "an emission"
    assert activity.id == 1


@bw2test
def test_iter():
    d = Database("biosphere")
    d.write(biosphere)
    activity = next(iter(d))
    assert isinstance(activity, PWActivity)
    assert activity["name"] in ("an emission", "another emission")


@bw2test
def test_get_random():
    d = Database("biosphere")
    d.write(biosphere)
    activity = d.random()
    assert isinstance(activity, PWActivity)
    assert activity["name"] in ("an emission", "another emission")


def test_copy(food):
    d = Database("food")
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
    d = Database("old name")
    d.write(data)
    new_db = d.copy("new name")
    new_data = new_db.load()
    assert list(new_data.values())[0]["exchanges"][0]["input"] == ("new name", "1")
    assert list(data.values())[0]["exchanges"][0]["input"] == ("old name", "1")
    assert list(d.load().values())[0]["exchanges"][0]["input"] == ("old name", "1")


@bw2test
def test_raise_wrong_database():
    data = {("foo", "1"): {}}
    d = Database("bar")
    with pytest.raises(WrongDatabase):
        d.write(data)


@bw2test
def test_deletes_from_database():
    d = Database("biosphere")
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
    db = Database("foo")
    assert shiny_new == db.relabel_data(old_data, "shiny new")


### Metadata


@bw2test
def test_find_graph_dependents():
    Database(name="one", depends=["two", "three"]).save()
    Database(name="two", depends=["four", "five"]).save()
    Database(name="three", depends=["four"]).save()
    Database(name="four", depends=["six"]).save()
    Database(name="five", depends=["two"]).save()
    Database(name="six", depends=[]).save()
    assert Database.get(Database.name == "one").find_graph_dependents() == {
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
    }


@bw2test
def test_register():
    database = Database("testy")
    database.register()
    assert "testy" in databases
    assert "depends" in databases["testy"]


@bw2test
def test_deregister():
    d = Database("food")
    d.register()
    assert "food" in databases
    d.deregister()
    assert "food" not in databases


@bw2test
def test_write_sets_databases_number_attribute():
    d = Database("biosphere")
    d.write(biosphere)
    assert databases["biosphere"]["number"] == len(biosphere)


### Processed arrays


@bw2test
def test_process_unknown_object():
    database = Database("testy")
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
    db = Database("foo")
    data = {("foo", str(i)): {"name": x} for i, x in enumerate(get_naughty())}
    db.write(data)
    assert set(get_naughty()) == set(x["name"] for x in db)


@bw2test
def test_setup():
    d = Database("biosphere")
    d.write(biosphere)
    d = Database("food")
    d.write(food_data)


@bw2test
def test_rename():
    Database(name="biosphere").write(biosphere)
    d = Database(name="food")
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
    database = Database("testy")
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
    act = database.get_node("B")
    exc = [x for x in act.production()][0]
    exc["amount"] = 2
    exc.save()
    assert database.metadata.get("dirty")
    assert databases[database.name]["dirty"]
    assert database.metadata["modified"] > then

    exc = [x for x in act.production()][0]
    assert exc["amount"] == 2


@bw2test
@pytest.mark.skip()
def test_dirty_activities():
    database = Database("testy")
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
    act = database.get_node("B")
    exc = [x for x in act.production()][0]
    exc["amount"] = 2
    exc.save()
    assert databases["testy"]["dirty"]
    lca = act.lca()
    assert not databases["testy"].get("dirty")
    assert lca.supply_array[lca.activity_dict[("testy", "A")]] == 0.5


@bw2test
def test_process_invalid_exchange_value():
    database = Database("testy")
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
    database = Database("testy")
    database_data = {
        ("testy", "A"): {"exchanges": [{"amount": 1, "input": ("testy", "A")}]},
    }
    with pytest.raises(UntypedExchange):
        database.write(database_data, process=False)


@bw2test
def test_no_input_raises_invalid_exchange():
    database = Database("testy")
    database_data = {
        ("testy", "A"): {"exchanges": [{"amount": 1}]},
    }
    with pytest.raises(InvalidExchange):
        database.write(database_data, process=False)


@bw2test
def test_no_amount_raises_invalid_exchange():
    database = Database("testy")
    database_data = {
        ("testy", "A"): {
            "exchanges": [{"input": ("testy", "A"), "type": "technosphere"}]
        },
    }
    with pytest.raises(InvalidExchange):
        database.write(database_data, process=False)


@bw2test
def test_zero_amount_is_valid_exchange():
    database = Database("testy")
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
    database = Database("a database")
    database.write(
        {
            ("a database", "foo"): {"exchanges": [], "type": "process"},
            ("a database", "bar"): {"type": "definitely not a process"},
        },
        process=True,
    )
    # This shouldn't raise an error
    assert database.process() is None


@bw2test
def test_geomapping_array_includes_only_processes():
    database = Database("a database")
    database.write(
        {
            ("a database", "foo"): {
                "exchanges": [],
                "type": "process",
                "location": "bar",
            },
            ("a database", "baz"): {"exchanges": [], "type": "emission"},
        }
    )
    package = database.datapackage()
    array = package.get_resource("a_database_inventory_geomapping_matrix.indices")[0]
    assert array.shape == (1,)
    assert array[0]["col"] == geomapping["bar"]


@bw2test
def test_processed_array():
    database = Database("a database")
    database.write(
        {
            ("a database", "2"): {
                "type": "process",
                "exchanges": [
                    {
                        "input": ("a database", "2"),
                        "amount": 42,
                        "uncertainty_type": 7,
                        "type": "production",
                    }
                ],
            }
        }
    )
    package = database.datapackage()
    array = package.get_resource("a_database_technosphere_matrix.data")[0]

    assert array.shape == (1,)
    assert array[0] == 42

    array = package.get_resource("a_database_technosphere_matrix.distributions")[0]
    assert array.shape == (1,)
    assert array[0]["uncertainty_type"] == 7


@bw2test
def test_processed_array_with_metadata():
    database = Database("a database")
    database.write(
        {
            ("a database", "2"): {
                "type": "process",
                "name": "fooz",
                "unit": "something",
                "exchanges": [
                    {
                        "input": ("a database", "2"),
                        "amount": 42,
                        "uncertainty_type": 7,
                        "type": "production",
                    }
                ],
            }
        }
    )
    package = database.datapackage()

    with pytest.raises(KeyError):
        package.get_resource("a_database_activity_metadata")

    database.process(csv=True)
    package = database.datapackage()
    df = package.get_resource("a_database_activity_metadata")[0]
    if "Unnamed: 0" in df.columns:
        df.drop("Unnamed: 0", axis=1, inplace=True)

    expected = pd.DataFrame(
        [
            {
                "name": "fooz",
                "reference product": np.NaN,
                "unit": "something",
                "location": np.NaN,
                "id": 1,
            }
        ]
    )
    assert isinstance(df, pd.DataFrame)
    assert_frame_equal(
        df.reindex(sorted(df.columns), axis=1),
        expected.reindex(sorted(expected.columns), axis=1),
    )


@bw2test
def test_base_class():
    database = Database("a database")
    assert database._metadata is databases


@bw2test
def test_find_dependents():
    database = Database("a database")
    database.write(
        {
            ("a database", "foo"): {
                "exchanges": [
                    {
                        "input": ("foo", "bar"),
                        "type": "technosphere",
                        "amount": 0,
                    },
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
                    {
                        "input": ("who", "am I?"),
                        "type": "unknown",
                        "amount": 0,
                    },
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
                    {
                        "input": ("baz", "w00t"),
                        "type": "technosphere",
                        "amount": 0,
                    }
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
    foo = Database("foo")
    foo.write(
        {
            ("foo", "bar"): {
                "exchanges": [],
                "type": "process",
            },
        }
    )
    baz = Database("baz")
    baz.write(
        {
            ("baz", "w00t"): {
                "exchanges": [],
                "type": "process",
            },
        }
    )
    biosphere = Database("biosphere")
    biosphere.write(
        {
            ("biosphere", "bar"): {
                "exchanges": [],
                "type": "process",
            },
        }
    )
    database = Database("a database")
    database.register()
    assert databases["a database"]["depends"] == []
    database.write(
        {
            ("a database", "foo"): {
                "exchanges": [
                    {"input": ("foo", "bar"), "type": "technosphere", "amount": 1},
                    {
                        "input": ("biosphere", "bar"),
                        "type": "biosphere",
                        "amount": 1,
                    },
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


@bw2test
def test_process_without_exchanges_still_in_processed_array():
    database = Database("a database")
    database.write({("a database", "foo"): {}})

    package = database.datapackage()
    array = package.get_resource("a_database_technosphere_matrix.data")[0]
    assert array[0] == 1
    assert array.shape == (1,)


@bw2test
def test_random_empty():
    database = Database("a database")
    database.write({})
    with warnings.catch_warnings() as w:
        warnings.simplefilter("ignore")
        assert database.random() is None


@bw2test
def test_new_node():
    database = Database("a database")
    database.register()
    act = database.new_node("foo", this="that", name="something")
    act.save()

    act = database.get_node("foo")
    assert act["database"] == "a database"
    assert act["code"] == "foo"
    assert act["location"] == "GLO"
    assert act["this"] == "that"


@bw2test
def test_new_node_no_code():
    database = Database("a database")
    database.register()
    act = database.new_node(this="that", name="something")
    act.save()

    act = database.get_node(this="that")
    assert len(act['code']) == 32


@bw2test
def test_new_node_error():
    database = Database("a database")
    database.register()
    act = database.new_node("foo", this="that", name="something")
    act.save()

    with pytest.raises(DuplicateNode):
        database.new_node("foo")
    with pytest.raises(DuplicateNode):
        database.new_node(code="bar", id=act.id)


@bw2test
def test_new_activity():
    database = Database("a database")
    database.register()
    act = database.new_activity("foo", this="that", name="something")
    act.save()

    act = database.get_node("foo")
    assert act["database"] == "a database"
    assert act["code"] == "foo"
    assert act["location"] == "GLO"
    assert act["this"] == "that"


@bw2test
def test_can_split_processes_products():
    database = Database("a database")
    database.write(
        {
            # No implicit production because type specified and not process
            ("a database", "product"): {"type": "product"},
            ("a database", "foo"): {
                "exchanges": [
                    {
                        "input": ("a database", "product"),
                        "output": ("a database", "product"),
                        "type": "production",
                        "amount": 1,
                    }
                ],
                "type": "process",
            },
        }
    )
    package = database.datapackage()
    array = package.get_resource("a_database_technosphere_matrix.indices")[0]
    # print statements to get debugging for CI test runners
    for x in database:
        print(x.id, x.key, get_id(x.key))
    print("array:", array)
    print("array col:", array["col"])
    print("array dtype:", array.dtype)
    assert array.shape == (1,)
    assert array["col"][0] == get_id(("a database", "foo"))
    assert array["row"][0] == get_id(("a database", "product"))


@bw2test
def test_sqlite_processed_array_order():
    database = Database("testy_new")
    data = {
        ("testy_new", "C"): {},
        ("testy_new", "A"): {},
        ("testy_new", "B"): {
            "exchanges": [
                {"input": ("testy_new", "A"), "amount": 1, "type": "technosphere"},
                {"input": ("testy_new", "A"), "amount": 2, "type": "technosphere"},
                {"input": ("testy_new", "C"), "amount": 2, "type": "biosphere"},
                {"input": ("testy_new", "C"), "amount": 3, "type": "biosphere"},
                {"input": ("testy_new", "B"), "amount": 4, "type": "production"},
                {"input": ("testy_new", "B"), "amount": 1, "type": "production"},
            ]
        },
    }
    database.write(data)
    lookup = {k: get_id(("testy_new", k)) for k in "ABC"}
    # print statements to get debugging for CI test runners
    print("lookup:", lookup)
    assert len(lookup) == 3
    t = sorted(
        [
            (lookup["A"], lookup["B"], 1),
            (lookup["A"], lookup["B"], 2),
            # Implicit production
            (lookup["C"], lookup["C"], 1),
            (lookup["A"], lookup["A"], 1),
            # Explicit production
            (lookup["B"], lookup["B"], 4),
            (lookup["B"], lookup["B"], 1),
        ]
    )
    b = sorted([(lookup["C"], lookup["B"], 2), (lookup["C"], lookup["B"], 3)])
    print("t:", t)
    print("b:", b)

    package = database.datapackage()

    array = package.get_resource("testy_new_technosphere_matrix.data")[0]
    print("data array:", array)
    assert array.shape == (6,)
    assert np.allclose(array, [x[2] for x in t])

    array = package.get_resource("testy_new_technosphere_matrix.indices")[0]
    print("indices array:", array)
    assert array.shape == (6,)
    assert np.allclose(array["row"], [x[0] for x in t])
    assert np.allclose(array["col"], [x[1] for x in t])

    array = package.get_resource("testy_new_biosphere_matrix.data")[0]
    print("data array:", array)
    assert array.shape == (2,)
    assert np.allclose(array, [x[2] for x in b])

    array = package.get_resource("testy_new_biosphere_matrix.indices")[0]
    print("indices array:", array)
    assert array.shape == (2,)
    assert np.allclose(array["row"], [x[0] for x in b])
    assert np.allclose(array["col"], [x[1] for x in b])


@bw2test
def test_no_distributions_if_no_uncertainty():
    database = Database("a database")
    database.write(
        {
            ("a database", "2"): {
                "type": "process",
                "exchanges": [
                    {
                        "input": ("a database", "2"),
                        "amount": 42.0,
                        "type": "technosphere",
                    }
                ],
            }
        }
    )

    package = database.datapackage()
    with pytest.raises(KeyError):
        package.get_resource("a_database_technosphere_matrix.distributions")


@bw2test
def test_database_delete_parameters():
    db = Database("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(
        amount=0, input=b, type="technosphere", formula="foo * bar + 4"
    ).save()

    database_data = [
        {
            "name": "red",
            "formula": "(blue ** 2) / 5",
        },
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


@bw2test
def test_delete_duplicate_exchanges():
    all_exchanges = lambda db: [exc for ds in db for exc in ds.exchanges()]

    db = Database("test-case")

    db.write(
        {
            ("test-case", "1"): {"exchanges": []},
            ("test-case", "2"): {"exchanges": []},
            ("test-case", "3"): {
                "exchanges": [
                    {"input": ("test-case", "2"), "type": "foo", "amount": 1},
                    {"input": ("test-case", "2"), "type": "foo", "amount": 2},
                    {"input": ("test-case", "2"), "type": "bar", "amount": 2},
                    {"input": ("test-case", "1"), "type": "foo", "amount": 12},
                    {"input": ("test-case", "1"), "type": "foo", "amount": 12},
                ]
            },
        }
    )

    assert len(all_exchanges(db)) == 5
    db.delete_duplicate_exchanges()
    assert len(all_exchanges(db)) == 4
    db.delete_duplicate_exchanges(fields=["amount"])
    assert len(all_exchanges(db)) == 3


@bw2test
def test_add_geocollections(capsys):
    db = Database("test-case")
    db.write(
        {
            ("test-case", "1"): {"location": "RU", "exchanges": []},
            ("test-case", "2"): {"exchanges": []},
            ("test-case", "3"): {"exchanges": [], "location": ("foo", "bar")},
        }
    )
    assert db.metadata["geocollections"] == ["foo", "world"]
    assert "Not able" in capsys.readouterr().out


@bw2test
def test_set_geocollections(capsys):
    db = Database("test-case")
    db.write(
        {
            ("test-case", "1"): {"location": "RU", "exchanges": [], "name": "a"},
            ("test-case", "2"): {"exchanges": [], "name": "b"},
            ("test-case", "3"): {
                "exchanges": [],
                "location": ("foo", "bar"),
                "name": "c",
            },
        }
    )
    assert db.metadata["geocollections"] == ["foo", "world"]
    assert "Not able" in capsys.readouterr().out

    act = get_activity(("test-case", "1"))
    act["location"] = ("this", "that")
    act.save()

    act = get_activity(("test-case", "2"))
    act["location"] = "DE"
    act.save()

    db.set_geocollections()
    assert db.metadata["geocollections"] == ["foo", "this", "world"]


@bw2test
def test_add_geocollections_unable(capsys):
    db = Database("test-case")
    db.write(
        {
            ("test-case", "1"): {"location": "Russia", "exchanges": []},
            ("test-case", "3"): {"exchanges": [], "location": ("foo", "bar")},
        }
    )
    assert db.metadata["geocollections"] == ["foo"]
    assert "Not able" in capsys.readouterr().out


@bw2test
def test_add_geocollections_no_unable_for_product(capsys):
    db = Database("test-case")
    db.write(
        {
            ("test-case", "1"): {
                "location": "Russia",
                "type": "product",
                "exchanges": [],
            },
            ("test-case", "3"): {"exchanges": [], "location": ("foo", "bar")},
        }
    )
    assert db.metadata["geocollections"] == ["foo"]
    assert "Not able" not in capsys.readouterr().out


@pytest.fixture
@bw2test
def df_fixture():
    Database("biosphere").write(biosphere)
    Database("food").write(food_data)


def test_edges_to_dataframe_simple(df_fixture):
    df = Database("food").edges_to_dataframe(categorical=False)
    id_map = {obj["code"]: obj.id for obj in Database("food")}

    expected = pd.DataFrame(
        [
            {
                "target_id": id_map["1"],
                "target_database": "food",
                "target_code": "1",
                "target_name": "lunch",
                "target_reference_product": None,
                "target_location": "CA",
                "target_unit": "kg",
                "target_type": "process",
                "source_id": id_map["2"],
                "source_database": "food",
                "source_code": "2",
                "source_name": "dinner",
                "source_product": None,
                "source_location": "CH",
                "source_unit": "kg",
                "source_categories": None,
                "edge_amount": 0.5,
                "edge_type": "technosphere",
            },
            {
                "target_id": id_map["1"],
                "target_database": "food",
                "target_code": "1",
                "target_name": "lunch",
                "target_reference_product": None,
                "target_location": "CA",
                "target_unit": "kg",
                "target_type": "process",
                "source_id": get_id(("biosphere", "1")),
                "source_database": "biosphere",
                "source_code": "1",
                "source_name": "an emission",
                "source_product": None,
                "source_location": None,
                "source_unit": "kg",
                "source_categories": "things",
                "edge_amount": 0.05,
                "edge_type": "biosphere",
            },
            {
                "target_id": id_map["2"],
                "target_database": "food",
                "target_code": "2",
                "target_name": "dinner",
                "target_reference_product": None,
                "target_location": "CH",
                "target_unit": "kg",
                "target_type": "process",
                "source_id": get_id(("biosphere", "2")),
                "source_database": "biosphere",
                "source_code": "2",
                "source_name": "another emission",
                "source_product": None,
                "source_location": None,
                "source_unit": "kg",
                "source_categories": "things",
                "edge_amount": 0.15,
                "edge_type": "biosphere",
            },
            {
                "target_id": id_map["2"],
                "target_database": "food",
                "target_code": "2",
                "target_name": "dinner",
                "target_reference_product": None,
                "target_location": "CH",
                "target_unit": "kg",
                "target_type": "process",
                "source_id": id_map["1"],
                "source_database": "food",
                "source_code": "1",
                "source_name": "lunch",
                "source_product": None,
                "source_location": "CA",
                "source_unit": "kg",
                "source_categories": None,
                "edge_amount": 0.25,
                "edge_type": "technosphere",
            },
        ]
    )
    assert_frame_equal(
        df.sort_values(["target_id", "source_id"]).reset_index(drop=True),
        expected.sort_values(["target_id", "source_id"]).reset_index(drop=True),
        check_dtype=False,
    )


def test_edges_to_dataframe_categorical(df_fixture):
    df = Database("food").edges_to_dataframe()
    assert df.shape == (4, 18)
    assert df["edge_type"].dtype.name == "category"


def test_edges_to_dataframe_formatters(df_fixture):
    def foo(node, edge, row):
        row["foo"] = "bar"

    df = Database("food").edges_to_dataframe(formatters=[foo])
    assert_series_equal(df["foo"], pd.Series(["bar"] * 4, name="foo"))


def test_nodes_to_dataframe_simple(df_fixture):
    df = Database("food").nodes_to_dataframe()
    expected = pd.DataFrame(
        [
            {
                "categories": ["stuff", "meals"],
                "code": "2",
                "database": "food",
                "id": get_id(("food", "2")),
                "location": "CH",
                "name": "dinner",
                "reference product": None,
                "type": "process",
                "unit": "kg",
            },
            {
                "categories": ("stuff", "meals"),
                "code": "1",
                "database": "food",
                "id": get_id(("food", "1")),
                "location": "CA",
                "name": "lunch",
                "reference product": None,
                "type": "process",
                "unit": "kg",
            },
        ]
    )
    assert_frame_equal(
        df.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_nodes_to_dataframe_columns(df_fixture):
    df = Database("food").nodes_to_dataframe(columns=["id", "name", "unit"])
    expected = pd.DataFrame(
        [
            {
                "id": get_id(("food", "2")),
                "name": "dinner",
                "unit": "kg",
            },
            {
                "id": get_id(("food", "1")),
                "name": "lunch",
                "unit": "kg",
            },
        ]
    )
    assert_frame_equal(
        df.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_nodes_to_dataframe_unsorted(df_fixture):
    df = Database("food").nodes_to_dataframe()
    assert df.shape == (2, 9)
