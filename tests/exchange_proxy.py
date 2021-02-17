from bw2data.tests import bw2test
from bw2data import (
    geomapping,
    databases,
    methods,
    Method,
    projects,
    get_activity,
)
from bw2data.database import DatabaseChooser
from bw2data.parameters import (
    ActivityParameter,
    ParameterizedExchange,
    parameters,
)

try:
    import bw2calc
except ImportError:
    bw2calc = None
import numpy as np
import stats_arrays as sa
import pytest


@pytest.fixture
@bw2test
def activity():
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "a"): {
                "exchanges": [
                    {"input": ("db", "a"), "amount": 2, "type": "production",},
                    {"input": ("db", "b"), "amount": 3, "type": "technosphere",},
                    {"input": ("db", "c"), "amount": 4, "type": "biosphere",},
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
                    {"input": ("db", "a"), "amount": 2, "type": "production",},
                    {"input": ("db", "b"), "amount": 3, "type": "technosphere",},
                    {"input": ("db", "c"), "amount": 4, "type": "biosphere",},
                ],
                "name": "a",
            },
            ("db", "b"): {"name": "b"},
            ("db", "c"): {"name": "c", "type": "biosphere"},
            ("db", "d"): {
                "name": "d",
                "exchanges": [
                    {"input": ("db", "a"), "amount": 5, "type": "technosphere"}
                ],
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
    assert len(projects) == 2  # Default project
    assert "default" in projects


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


def test_technosphere_include_substitution(activity):
    d = get_activity(("db", "d"))
    assert len(d.technosphere()) == 2
    assert len(d.technosphere(include_substitution=True)) == 2


def test_technosphere_exclude_substitution(activity):
    d = get_activity(("db", "d"))
    assert len(d.technosphere(include_substitution=False)) == 1


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


@bw2test
def test_delete_parameterized_exchange():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    exc = a.new_exchange(
        amount=0, input=b, type="technosphere", formula="foo * bar + 4"
    )
    exc.save()

    activity_data = [
        {
            "name": "reference_me",
            "formula": "sqrt(25)",
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

    exc.delete()
    assert ActivityParameter.select().count() == 2
    assert not ParameterizedExchange.select().count()
