from bw2data import Method, databases, geomapping, get_activity, get_node, methods, projects
from bw2data.configuration import labels
from bw2data.database import DatabaseChooser
from bw2data.tests import bw2test

try:
    import bw2calc
except ImportError:
    bw2calc = None
import warnings

import numpy as np
import pandas as pd
import pytest
import stats_arrays as sa
from pandas.testing import assert_frame_equal


@pytest.fixture
@bw2test
def activity():
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "a"): {
                "exchanges": [
                    {
                        "input": ("db", "a"),
                        "amount": 2,
                        "type": "production",
                    },
                    {
                        "input": ("db", "b"),
                        "amount": 3,
                        "type": "technosphere",
                    },
                    {
                        "input": ("db", "c"),
                        "amount": 4,
                        "type": "biosphere",
                    },
                ],
                "name": "a",
            },
            ("db", "b"): {"name": "b"},
            ("db", "c"): {"name": "c", "type": "biosphere"},
            ("db", "d"): {
                "name": "d",
                "exchanges": [
                    {"input": ("db", "a"), "amount": 5, "type": "technosphere"},
                    {"input": ("db", "b"), "amount": -0.1, "type": "substitution"},
                ],
            },
        }
    )
    return database.get("a")


@pytest.fixture
@bw2test
def activity_and_method():
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "a"): {
                "exchanges": [
                    {
                        "input": ("db", "a"),
                        "amount": 2,
                        "type": "production",
                    },
                    {
                        "input": ("db", "b"),
                        "amount": 3,
                        "type": "technosphere",
                    },
                    {
                        "input": ("db", "c"),
                        "amount": 4,
                        "type": "biosphere",
                    },
                ],
                "name": "a",
            },
            ("db", "b"): {"name": "b"},
            ("db", "c"): {"name": "c", "type": "biosphere"},
            ("db", "d"): {
                "name": "d",
                "exchanges": [{"input": ("db", "a"), "amount": 5, "type": "technosphere"}],
            },
        }
    )
    cfs = [(("db", "c"), 42)]
    method = Method(("a method",))
    method.register()
    method.write(cfs)
    return database.get("a"), method


def test_setup_clean(activity):
    assert len(databases) == 1
    assert list(methods) == []
    assert len(geomapping) == 1  # GLO
    assert "GLO" in geomapping
    assert len(projects) == 1  # Random test project
    assert "default" not in projects


def test_production(activity):
    assert len(list(activity.production())) == 1
    assert len(activity.production()) == 1
    exc = list(activity.production())[0]
    assert exc["amount"] == 2


def test_substitution(activity):
    d = get_activity(("db", "d"))
    assert len(activity.substitution()) == 0
    assert len(d.substitution()) == 1


def test_biosphere(activity):
    assert len(list(activity.biosphere())) == 1
    assert len(activity.biosphere()) == 1
    exc = list(activity.biosphere())[0]
    assert exc["amount"] == 4


def test_technosphere(activity):
    assert len(list(activity.technosphere())) == 1
    assert len(activity.technosphere()) == 1
    exc = list(activity.technosphere())[0]
    assert exc["amount"] == 3


def test_upstream(activity):
    assert len(list(activity.upstream())) == 1
    assert len(activity.upstream()) == 1
    exc = list(activity.upstream())[0]
    assert exc["amount"] == 5


def test_upstream_no_kinds(activity):
    act = get_activity(("db", "c"))
    assert len(list(act.upstream(kinds=None))) == 1
    assert len(act.upstream(kinds=None)) == 1
    exc = list(act.upstream(kinds=None))[0]
    assert exc["amount"] == 4


def test_upstream_bio(activity):
    act = get_activity(("db", "c"))
    assert len(list(act.upstream())) == 0
    assert len(act.upstream()) == 0


def test_ordering_consistency(activity):
    ordering = [[exc["amount"] for exc in activity.exchanges()] for _ in range(100)]
    for sample in ordering[1:]:
        assert sample == ordering[0]


def test_exchanges_to_dataframe(activity):
    df = get_node(code="a").exchanges().to_dataframe()
    id_map = {obj["code"]: obj.id for obj in DatabaseChooser("db")}

    tech_exchanges = [
        ("a", "a", 2, "production"),
        ("b", "a", 3, "technosphere"),
        ("c", "a", 4, "biosphere"),
    ]
    expected = pd.DataFrame(
        [
            {
                "target_id": id_map[a],
                "target_database": "db",
                "target_code": a,
                "target_name": get_activity(code=a).get("name"),
                "target_reference_product": None,
                "target_location": get_activity(code=a).get("location"),
                "target_unit": get_activity(code=a).get("unit"),
                "target_type": get_activity(code=a).get("type") or labels.process_node_default,
                "source_id": id_map[b],
                "source_database": "db",
                "source_code": b,
                "source_name": get_activity(code=b).get("name"),
                "source_product": None,
                "source_location": get_activity(code=b).get("location"),
                "source_unit": get_activity(code=b).get("unit"),
                "source_categories": None,
                "edge_amount": c,
                "edge_type": d,
            }
            for b, a, c, d in tech_exchanges
        ]
    )

    categorical_columns = [
        "target_database",
        "target_name",
        "target_reference_product",
        "target_location",
        "target_unit",
        "target_type",
        "source_database",
        "source_code",
        "source_name",
        "source_product",
        "source_location",
        "source_unit",
        "source_categories",
        "edge_type",
    ]
    for column in categorical_columns:
        if column in expected.columns:
            expected[column] = expected[column].astype("category")

    assert_frame_equal(
        df.sort_values(["target_id", "source_id"]).reset_index(drop=True),
        expected.sort_values(["target_id", "source_id"]).reset_index(drop=True),
        check_dtype=False,
    )


@bw2test
def test_uncertainty():
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "a"): {
                "exchanges": [
                    {
                        "input": ("db", "a"),
                        "amount": 2,
                        "type": "production",
                        "uncertainty type": 1,
                        "loc": 2,
                        "scale": 3,
                        "shape": 4,
                        "maximum": 5,
                        "negative": True,
                        "name": "a",
                    }
                ]
            }
        }
    )
    expected = {
        "uncertainty type": 1,
        "loc": 2,
        "scale": 3,
        "shape": 4,
        "maximum": 5,
        "negative": True,
    }
    exchange = list(database.get("a").exchanges())[0]
    assert exchange.uncertainty == expected


@bw2test
def test_uncertainty_type():
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "a"): {
                "exchanges": [
                    {
                        "input": ("db", "a"),
                        "amount": 2,
                        "type": "production",
                        "uncertainty type": 1,
                        "name": "a",
                    }
                ]
            }
        }
    )
    exchange = list(database.get("a").exchanges())[0]
    assert exchange.uncertainty_type.id == 1
    assert exchange.uncertainty_type == sa.NoUncertainty


@bw2test
def test_uncertainty_type_missing():
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "a"): {
                "exchanges": [
                    {
                        "input": ("db", "a"),
                        "amount": 2,
                        "type": "production",
                        "name": "a",
                    }
                ]
            }
        }
    )
    exchange = list(database.get("a").exchanges())[0]
    assert exchange.uncertainty_type.id == 0
    assert exchange.uncertainty_type == sa.UndefinedUncertainty


@bw2test
def test_random_sample():
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "a"): {
                "exchanges": [
                    {
                        "input": ("db", "a"),
                        "type": "production",
                        "amount": 20,
                        "unit": "kg",
                        "type": "biosphere",
                        "uncertainty type": 2,
                        "loc": np.log(20),
                        "scale": 1.01,
                    }
                ],
                "name": "a",
            }
        }
    )
    exchange = list(database.get("a").exchanges())[0]
    assert (exchange.random_sample() > 0).sum() == 100
    assert exchange.random_sample().shape == (100,)


@bw2test
def test_random_sample_negative():
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "a"): {
                "exchanges": [
                    {
                        "input": ("db", "a"),
                        "type": "production",
                        "amount": -20,
                        "negative": True,
                        "unit": "kg",
                        "type": "biosphere",
                        "uncertainty type": 2,
                        "loc": np.log(20),
                        "scale": 1.01,
                    }
                ],
                "name": "a",
            }
        }
    )
    exchange = list(database.get("a").exchanges())[0]
    assert (exchange.random_sample() < 0).sum() == 100
    assert exchange.random_sample().shape == (100,)


@pytest.mark.skip()
@pytest.mark.skipif(bw2calc is None, reason="requires bw2calc")
def test_lca(activity_and_method):
    a, m = activity_and_method
    exc = list(a.production())[0]
    lca = exc.lca(method=m.name)
    assert lca.score == 4 * 42
    lca = exc.lca(method=m.name, amount=1)
    assert lca.score == 2 * 42


def test_exchange_eq(activity):
    ex = list(activity.exchanges())[0]
    assert ex == ex


def test_exchange_hash(activity):
    ex = list(activity.exchanges())[0]
    assert ex.__hash__()


@bw2test
def test_typo_exchange_type():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    exc = a.new_exchange(amount=0, input=b, type="technsphere", formula="foo * bar + 4")

    expected = (
        "Possible typo found: Given exchange type `technsphere` but `technosphere` is more common"
    )
    with pytest.warns(UserWarning, match=expected):
        exc.save()


@bw2test
def test_typo_exchange_key():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    exc = a.new_exchange(amount=11, input=b, type="technosphere", temporal_distrbution=[])

    expected = "Possible incorrect exchange key found: Given `temporal_distrbution` but `temporal_distribution` is more common"
    with pytest.warns(UserWarning, match=expected):
        exc.save()


@bw2test
def test_valid_exchange_type():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    exc = a.new_exchange(amount=0, input=b, type="technosphere", formula="foo * bar + 4")

    # assert that no warnings are raised
    # https://docs.pytest.org/en/8.0.x/how-to/capture-warnings.html
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        exc.save()


@bw2test
def test_valid_exchange_key():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    exc = a.new_exchange(amount=11, input=b, type="technosphere", temporal_distribution=[])

    # assert that no warnings are raised
    # https://docs.pytest.org/en/8.0.x/how-to/capture-warnings.html
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        exc.save()


@bw2test
def test_typo_exchange_type_multiple_corrections():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    exc = a.new_exchange(amount=0, input=b, type="technshhere", formula="foo * bar + 4")

    expected = (
        "Possible typo found: Given exchange type `technshhere` but `technosphere` is more common"
    )
    with pytest.warns(UserWarning, match=expected):
        exc.save()
