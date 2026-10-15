from bw2data import Method, databases, geomapping, get_activity, get_node, methods, projects
from bw2data.configuration import labels
from bw2data.database import DatabaseChooser
from bw2data.parameters import ActivityParameter, ParameterizedExchange, parameters
from bw2data.tests import bw2test

try:
    import bw2calc
except ImportError:
    bw2calc = None
import json
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


@bw2test
def test_exchange_str_invalid():
    """Test __str__ method for invalid exchange"""
    from bw2data.backends.proxies import Exchange

    # Create an invalid exchange (missing required fields)
    exc = Exchange()
    # Don't set required fields like input, output, amount, type

    result = str(exc)
    assert result == "Exchange with missing fields (call ``valid(why=True)`` to see more)"


@bw2test
def test_exchange_str_normal():
    """Test __str__ method for normal exchange (input to output)"""

    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="Activity A", unit="kg", location="GLO")
    a.save()
    b = db.new_activity(code="B", name="Activity B", unit="kg", location="GLO")
    b.save()

    # Create a technosphere exchange (normal order: input to output)
    exc = a.new_exchange(amount=2.5, input=b, type="technosphere")
    exc.save()

    result = str(exc)
    assert result == "Exchange: 2.5 kg 'Activity B' (kg, GLO, None) to 'Activity A' (kg, GLO, None)"


@bw2test
def test_exchange_str_production_reversed():
    """Test __str__ method for production exchange (product to process, reversed order)"""

    db = DatabaseChooser("example")
    db.register()

    # Create a product node
    product = db.new_activity(
        code="product_X", name="Product X", type="product", unit="kg", location="GLO"
    )
    product.save()

    # Create a process node
    process = db.new_activity(
        code="process_A", name="Process A", type="process", unit="kg", location="GLO"
    )
    process.save()

    # Create a production exchange (product to process with positive edge type)
    # This should use reversed order: output to input
    exc = process.new_exchange(amount=1.0, input=product, type="production")
    exc.save()

    result = str(exc)
    assert result == "Exchange: 1.0 kg 'Product X' (kg, GLO, None) from 'Process A' (kg, GLO, None)"


@bw2test
def test_exchange_str_biosphere():
    """Test __str__ method for biosphere exchange"""
    from bw2data import Database

    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            }
        }
    )

    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="Activity A", unit="kg", location="GLO")
    a.save()

    # Create a biosphere exchange
    exc = a.new_exchange(amount=10.0, input=("biosphere", "CO2"), type="biosphere")
    exc.save()

    result = str(exc)
    assert (
        result
        == "Exchange: 10.0 kg 'Carbon dioxide' (kg, None, None) to 'Activity A' (kg, GLO, None)"
    )


@bw2test
def test_delete_parameterized_exchange():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    exc = a.new_exchange(amount=0, input=b, type="technosphere", formula="foo * bar + 4")
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


def _td(amounts):
    bw_temporalis = pytest.importorskip("bw_temporalis")
    return bw_temporalis.TemporalDistribution(
        date=np.array([0, 5], dtype="timedelta64[Y]"), amount=np.array(amounts)
    )


def _two_activities():
    db = DatabaseChooser("example")
    db.register()
    a = db.new_activity(code="A", name="An activity", type="process")
    a.save()
    b = db.new_activity(code="B", name="Another activity", type="process")
    b.save()
    return a, b


@bw2test
def test_td_sum_warns_on_exchange_save():
    a, b = _two_activities()
    exc = a.new_exchange(
        amount=1.0, input=b, type="technosphere", temporal_distribution=_td([0.5, 0.35])
    )

    with pytest.warns(UserWarning, match=r"sums to 0\.85, not 1"):
        exc.save()

    saved = list(a.technosphere())[0]
    assert saved["temporal_distribution"].amount.tolist() == [0.5, 0.35]


@bw2test
def test_td_sum_silent_when_sum_is_one():
    a, b = _two_activities()
    exc = a.new_exchange(
        amount=1.0, input=b, type="technosphere", temporal_distribution=_td([0.6, 0.4])
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        exc.save()
    assert not [w for w in caught if "Temporal distribution" in str(w.message)]


@bw2test
def test_td_sum_warns_on_database_write():
    td = _td([0.5, 0.35])
    db = DatabaseChooser("example")

    expected = r"\('example', 'B'\) -> \('example', 'A'\) sums to 0\.85, not 1"
    with pytest.warns(UserWarning, match=expected):
        db.write(
            {
                ("example", "A"): {
                    "name": "An activity",
                    "type": "process",
                    "exchanges": [
                        {
                            "input": ("example", "B"),
                            "amount": 1.0,
                            "type": "technosphere",
                            "temporal_distribution": td,
                        }
                    ],
                },
                ("example", "B"): {"name": "Another activity", "type": "process"},
            },
            process=False,
        )


@bw2test
def test_td_sum_checks_json_string():
    a, b = _two_activities()
    exc = a.new_exchange(
        amount=1.0,
        input=b,
        type="technosphere",
        temporal_distribution=_td([0.5, 0.35]).to_json(),
    )

    with pytest.warns(UserWarning, match=r"sums to 0\.85, not 1"):
        exc.save()


@bw2test
@pytest.mark.parametrize(
    "value",
    [
        None,
        {"year": 2020, "value": 0.5},
        "not a TD",
        '{"amount": "x"}',
        '{"amount": [null]}',
        '{"amount": [[1], [2, 3]]}',
    ],
)
def test_td_sum_never_breaks_save_for_non_td_values(value):
    a, b = _two_activities()
    exc = a.new_exchange(amount=1.0, input=b, type="technosphere", temporal_distribution=value)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        exc.save()
    assert not [w for w in caught if "Temporal distribution" in str(w.message)]


def _stored_td(exc):
    """Raw ``temporal_distribution`` value as stored in the ``ExchangeDataset`` table."""
    from bw2data.backends import ExchangeDataset

    return ExchangeDataset.get_by_id(exc.id).data["temporal_distribution"]


def _assert_same_td(first, second):
    assert type(first) is type(second)
    assert np.array_equal(first.date, second.date)
    assert np.array_equal(first.amount, second.amount)


@bw2test
def test_td_stored_as_json_and_restored_on_exchange_save():
    a, b = _two_activities()
    td = _td([0.6, 0.4])
    exc = a.new_exchange(amount=1.0, input=b, type="technosphere", temporal_distribution=td)
    exc.save()

    # Saving doesn't change the TD on the exchange object
    assert exc["temporal_distribution"] is td

    stored = _stored_td(exc)
    assert isinstance(stored, str)
    assert json.loads(stored)["__loader__"] == "bw_temporalis.TemporalDistribution"

    _assert_same_td(list(a.technosphere())[0]["temporal_distribution"], td)


@bw2test
def test_td_stored_as_json_and_restored_on_database_write():
    td = _td([0.6, 0.4])
    data = {
        ("example", "A"): {
            "name": "An activity",
            "type": "process",
            "exchanges": [
                {
                    "input": ("example", "B"),
                    "amount": 1.0,
                    "type": "technosphere",
                    "temporal_distribution": td,
                }
            ],
        },
        ("example", "B"): {"name": "Another activity", "type": "process"},
    }
    DatabaseChooser("example").write(data, process=False)

    # Input data isn't modified
    assert data[("example", "A")]["exchanges"][0]["temporal_distribution"] is td

    exc = list(get_node(database="example", code="A").technosphere())[0]
    assert isinstance(_stored_td(exc), str)
    _assert_same_td(exc["temporal_distribution"], td)


@bw2test
def test_td_stored_as_json_on_activity_copy():
    a, b = _two_activities()
    td = _td([0.6, 0.4])
    a.new_exchange(amount=1.0, input=b, type="technosphere", temporal_distribution=td).save()

    exc = list(a.copy(code="C").technosphere())[0]
    assert isinstance(_stored_td(exc), str)
    _assert_same_td(exc["temporal_distribution"], td)


@bw2test
def test_td_stored_as_json_on_copy_activities():
    a, b = _two_activities()
    td = _td([0.6, 0.4])
    a.new_exchange(amount=1.0, input=b, type="technosphere", temporal_distribution=td).save()
    DatabaseChooser("other").register()

    (new,) = DatabaseChooser("example").copy_activities([a], "other")
    exc = list(new.technosphere())[0]
    assert isinstance(_stored_td(exc), str)
    _assert_same_td(exc["temporal_distribution"], td)


@bw2test
@pytest.mark.parametrize("cls_name", ["TemporalDistribution", "FixedTD", "FixedTimeOfYearTD"])
def test_td_roundtrip_subclasses(cls_name):
    bw_temporalis = pytest.importorskip("bw_temporalis")
    if cls_name == "FixedTimeOfYearTD":
        td = bw_temporalis.FixedTimeOfYearTD(
            date=np.array([10, 20], dtype="timedelta64[D]"), amount=np.array([0.5, 0.5])
        )
    else:
        td = getattr(bw_temporalis, cls_name)(
            date=np.array(["2020-01-01", "2021-01-01"], dtype="datetime64[D]"),
            amount=np.array([0.5, 0.5]),
        )
    a, b = _two_activities()
    exc = a.new_exchange(amount=1.0, input=b, type="technosphere", temporal_distribution=td)
    exc.save()

    assert json.loads(_stored_td(exc))["__loader__"] == f"bw_temporalis.{cls_name}"
    _assert_same_td(list(a.technosphere())[0]["temporal_distribution"], td)


@bw2test
def test_td_legacy_pickled_object_still_loads():
    from bw2data.backends import ExchangeDataset

    a, b = _two_activities()
    td = _td([0.6, 0.4])
    # Data written before TDs were serialized contains the pickled object
    ExchangeDataset.create(
        data={"input": b.key, "output": a.key, "amount": 1.0, "type": "technosphere", "temporal_distribution": td},
        input_database=b["database"],
        input_code=b["code"],
        output_database=a["database"],
        output_code=a["code"],
        type="technosphere",
    )
    _assert_same_td(list(a.technosphere())[0]["temporal_distribution"], td)


def _save_raw_td(value):
    from bw2data.backends import ExchangeDataset

    a, b = _two_activities()
    ExchangeDataset.create(
        data={"input": b.key, "output": a.key, "amount": 1.0, "type": "technosphere", "temporal_distribution": value},
        input_database=b["database"],
        input_code=b["code"],
        output_database=a["database"],
        output_code=a["code"],
        type="technosphere",
    )
    return a


TD_JSON = json.dumps(
    {
        "__loader__": "bw_temporalis.TemporalDistribution",
        "date_dtype": "timedelta64[Y]",
        "date": [0, 5],
        "amount": [0.6, 0.4],
    }
)


@bw2test
def test_td_restore_without_bw_temporalis(monkeypatch):
    import bw2data.backends.utils as utils_module

    monkeypatch.setattr(utils_module, "_import_bw_temporalis", lambda: None)
    a = _save_raw_td(TD_JSON)

    with pytest.warns(UserWarning, match="can't be imported"):
        exc = list(a.technosphere())[0]
    assert exc["temporal_distribution"] == TD_JSON


@bw2test
def test_td_restore_unknown_loader_warns():
    pytest.importorskip("bw_temporalis")
    value = json.dumps({"__loader__": "bw_temporalis.NoSuchTD", "date": [], "amount": []})
    a = _save_raw_td(value)

    with pytest.warns(UserWarning, match="Can't restore temporal distribution"):
        exc = list(a.technosphere())[0]
    assert exc["temporal_distribution"] == value


@bw2test
@pytest.mark.parametrize(
    "value",
    [
        "not a TD",
        '{"__loader__": "some_other_library.Thing"}',
        '{"__loader__": broken json',
        {"__loader__": "bw_temporalis.TemporalDistribution"},
        {"year": 2020, "value": 0.5},
    ],
)
def test_td_restore_ignores_other_values(value):
    a = _save_raw_td(value)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        exc = list(a.technosphere())[0]
    assert exc["temporal_distribution"] == value
