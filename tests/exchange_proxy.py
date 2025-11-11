from bw2data import Method, databases, geomapping, get_activity, get_node, methods, projects
from bw2data.configuration import labels
from bw2data.database import DatabaseChooser
from bw2data.parameters import ActivityParameter, ParameterizedExchange, parameters
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


# Mock TemporalDistribution class for testing
class MockTemporalDistribution:
    """Mock class to simulate bw_temporalis.TemporalDistribution for testing"""
    def __init__(self, data):
        self.data = data
    
    def to_json(self):
        return {"type": "temporal_distribution", "data": self.data}


@bw2test
def test_temporal_distribution_processing_without_bw_temporalis():
    """Test that temporal_distribution processing works when bw_temporalis is not available"""

    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()

    # Create exchange with temporal_distribution (should be passed through unchanged)
    exc = a.new_exchange(
        amount=1.0,
        input=b,
        type="technosphere",
        temporal_distribution={"year": 2020, "value": 0.5}
    )

    # Save should work without errors
    exc.save()

    # Verify the exchange was saved correctly
    saved_exc = list(a.exchanges())[0]
    assert saved_exc["temporal_distribution"] == {"year": 2020, "value": 0.5}


@bw2test
def test_temporal_distribution_processing_with_mock():
    """Test temporal_distribution processing with a mock TemporalDistribution"""
    import bw2data.backends.proxies as proxies_module

    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = MockTemporalDistribution

    try:
        db = DatabaseChooser("example")
        db.register()

        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()

        # Create exchange with TemporalDistribution instance
        temporal_dist = MockTemporalDistribution({"year": 2020, "value": 0.5})
        exc = a.new_exchange(
            amount=1.0,
            input=b,
            type="technosphere",
            temporal_distribution=temporal_dist
        )

        # Save should convert TemporalDistribution to JSON
        exc.save()

        # Verify the exchange was saved and restored as TemporalDistribution
        saved_exc = list(a.exchanges())[0]
        assert isinstance(saved_exc["temporal_distribution"], MockTemporalDistribution)
        assert saved_exc["temporal_distribution"].data == {"year": 2020, "value": 0.5}

        # Verify original data was not modified
        assert exc["temporal_distribution"] == temporal_dist

    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


@bw2test
def test_temporal_distribution_processing_no_temporal_distribution():
    """Test that exchanges without temporal_distribution are processed normally"""

    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()

    # Create exchange without temporal_distribution
    exc = a.new_exchange(
        amount=1.0,
        input=b,
        type="technosphere"
    )

    # Save should work normally
    exc.save()

    # Verify the exchange was saved correctly
    saved_exc = list(a.exchanges())[0]
    assert saved_exc["amount"] == 1.0
    assert saved_exc["type"] == "technosphere"
    assert "temporal_distribution" not in saved_exc


@bw2test
def test_temporal_distribution_processing_non_temporal_distribution_value():
    """Test that non-TemporalDistribution values in temporal_distribution are not converted"""
    import bw2data.backends.proxies as proxies_module

    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = MockTemporalDistribution

    try:
        db = DatabaseChooser("example")
        db.register()

        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()

        # Create exchange with non-TemporalDistribution value in temporal_distribution
        exc = a.new_exchange(
            amount=1.0,
            input=b,
            type="technosphere",
            temporal_distribution="not a TemporalDistribution"
        )

        # Save should pass through the value unchanged
        exc.save()

        # Verify the exchange was saved with original value
        saved_exc = list(a.exchanges())[0]
        assert saved_exc["temporal_distribution"] == "not a TemporalDistribution"

    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


@bw2test
def test_temporal_distribution_processing_multiple_exchanges():
    """Test that temporal_distribution processing works with multiple exchanges"""
    import bw2data.backends.proxies as proxies_module

    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = MockTemporalDistribution

    try:
        db = DatabaseChooser("example")
        db.register()

        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()
        c = db.new_activity(code="C", name="Third activity")
        c.save()

        # Create multiple exchanges with different temporal_distribution values
        exc1 = a.new_exchange(
            amount=1.0,
            input=b,
            type="technosphere",
            temporal_distribution=MockTemporalDistribution({"year": 2020, "value": 0.5})
        )
        exc2 = a.new_exchange(
            amount=2.0,
            input=c,
            type="technosphere",
            temporal_distribution=MockTemporalDistribution({"year": 2021, "value": 0.8})
        )
        exc3 = a.new_exchange(
            amount=3.0,
            input=b,
            type="biosphere",
            temporal_distribution={"not": "a TemporalDistribution"}
        )

        # Save all exchanges
        exc1.save()
        exc2.save()
        exc3.save()

        # Verify all exchanges were saved correctly
        exchanges = list(a.exchanges())
        assert len(exchanges) == 3

        # Check first exchange
        exc1_saved = exchanges[0]
        assert isinstance(exc1_saved["temporal_distribution"], MockTemporalDistribution)
        assert exc1_saved["temporal_distribution"].data == {"year": 2020, "value": 0.5}

        # Check second exchange
        exc2_saved = exchanges[1]
        assert isinstance(exc2_saved["temporal_distribution"], MockTemporalDistribution)
        assert exc2_saved["temporal_distribution"].data == {"year": 2021, "value": 0.8}

        # Check third exchange (non-TemporalDistribution value)
        exc3_saved = exchanges[2]
        assert exc3_saved["temporal_distribution"] == {"not": "a TemporalDistribution"}

    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


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
    product = db.new_activity(code="product_X", name="Product X", type="product", unit="kg", location="GLO")
    product.save()

    # Create a process node
    process = db.new_activity(code="process_A", name="Process A", type="process", unit="kg", location="GLO")
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
    biosphere_db.write({
        ("biosphere", "CO2"): {
            "name": "Carbon dioxide",
            "type": "emission",
            "unit": "kg",
        }
    })

    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="Activity A", unit="kg", location="GLO")
    a.save()

    # Create a biosphere exchange
    exc = a.new_exchange(amount=10.0, input=("biosphere", "CO2"), type="biosphere")
    exc.save()

    result = str(exc)
    assert result == "Exchange: 10.0 kg 'Carbon dioxide' (kg, None, None) to 'Activity A' (kg, GLO, None)"


@bw2test
def test_temporal_distribution_processing_data_already_set():
    """Test that temporal_distribution processing is skipped when data_already_set=True"""
    from bw2data.backends.utils import dict_as_exchangedataset
    import bw2data.backends.proxies as proxies_module

    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = MockTemporalDistribution

    try:
        db = DatabaseChooser("example")
        db.register()

        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()

        # Create exchange with TemporalDistribution instance
        temporal_dist = MockTemporalDistribution({"year": 2020, "value": 0.5})
        exc = a.new_exchange(
            amount=1.0,
            input=b,
            type="technosphere",
            temporal_distribution=temporal_dist
        )

        # Manually set the document data (simulating data_already_set=True scenario)
        processed_data = exc._process_temporal_distributions(exc._data)
        for key, value in dict_as_exchangedataset(processed_data).items():
            setattr(exc._document, key, value)

        # Save with data_already_set=True should skip processing
        exc.save(data_already_set=True)

        # The temporal_distribution should remain as the original object
        # since processing was skipped
        assert exc["temporal_distribution"] == temporal_dist

        # Verify the exchange was saved and restored as TemporalDistribution
        saved_exc = list(a.exchanges())[0]
        assert isinstance(saved_exc["temporal_distribution"], MockTemporalDistribution)
        assert saved_exc["temporal_distribution"].data == {"year": 2020, "value": 0.5}

    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


@bw2test
def test_temporal_distribution_restoration_from_json():
    """Test that temporal_distribution JSON is converted back to TemporalDistribution when loading Exchange"""
    import bw2data.backends.proxies as proxies_module

    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = MockTemporalDistribution

    try:
        db = DatabaseChooser("example")
        db.register()

        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()

        # Create exchange with TemporalDistribution instance and save it
        temporal_dist = MockTemporalDistribution({"year": 2020, "value": 0.5})
        exc = a.new_exchange(
            amount=1.0,
            input=b,
            type="technosphere",
            temporal_distribution=temporal_dist
        )
        exc.save()

        # Get the saved exchange from database (this will trigger the restoration)
        saved_exc = list(a.exchanges())[0]

        # Verify that temporal_distribution was restored to a TemporalDistribution instance
        assert isinstance(saved_exc["temporal_distribution"], MockTemporalDistribution)
        assert saved_exc["temporal_distribution"].data == {"year": 2020, "value": 0.5}

    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


@bw2test
def test_temporal_distribution_restoration_without_bw_temporalis():
    """Test that temporal_distribution JSON is left as-is when bw_temporalis is not available"""

    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()

    # Create exchange with temporal_distribution JSON data and save it
    exc = a.new_exchange(
        amount=1.0,
        input=b,
        type="technosphere",
        temporal_distribution={"type": "temporal_distribution", "data": {"year": 2020, "value": 0.5}}
    )
    exc.save()

    # Get the saved exchange from database
    saved_exc = list(a.exchanges())[0]

    # Verify that temporal_distribution remains as JSON (since bw_temporalis is not available)
    assert saved_exc["temporal_distribution"] == {"type": "temporal_distribution", "data": {"year": 2020, "value": 0.5}}


@bw2test
def test_temporal_distribution_restoration_non_json_data():
    """Test that non-JSON temporal_distribution data is left unchanged"""
    import bw2data.backends.proxies as proxies_module

    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = MockTemporalDistribution

    try:
        db = DatabaseChooser("example")
        db.register()

        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()

        # Create exchange with non-JSON temporal_distribution data
        exc = a.new_exchange(
            amount=1.0,
            input=b,
            type="technosphere",
            temporal_distribution="not json data"
        )
        exc.save()

        # Get the saved exchange from database
        saved_exc = list(a.exchanges())[0]

        # Verify that temporal_distribution remains unchanged
        assert saved_exc["temporal_distribution"] == "not json data"

    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


@bw2test
def test_temporal_distribution_restoration_invalid_json():
    """Test that invalid temporal_distribution JSON is left unchanged"""
    import bw2data.backends.proxies as proxies_module

    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = MockTemporalDistribution

    try:
        db = DatabaseChooser("example")
        db.register()

        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()

        # Create exchange with invalid temporal_distribution JSON
        exc = a.new_exchange(
            amount=1.0,
            input=b,
            type="technosphere",
            temporal_distribution={"type": "temporal_distribution", "invalid": "data"}
        )
        exc.save()

        # Get the saved exchange from database
        saved_exc = list(a.exchanges())[0]

        # Verify that temporal_distribution remains unchanged (conversion failed)
        assert saved_exc["temporal_distribution"] == {"type": "temporal_distribution", "invalid": "data"}

    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


@bw2test
def test_temporal_distribution_roundtrip():
    """Test complete roundtrip: TemporalDistribution -> JSON -> TemporalDistribution"""
    import bw2data.backends.proxies as proxies_module

    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = MockTemporalDistribution

    try:
        db = DatabaseChooser("example")
        db.register()

        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()

        # Create original TemporalDistribution
        original_temporal_dist = MockTemporalDistribution({"year": 2020, "value": 0.5})

        # Create exchange with TemporalDistribution instance
        exc = a.new_exchange(
            amount=1.0,
            input=b,
            type="technosphere",
            temporal_distribution=original_temporal_dist
        )

        # Save (converts to JSON)
        exc.save()

        # Reload from database (converts back to TemporalDistribution)
        saved_exc = list(a.exchanges())[0]

        # Verify roundtrip worked correctly
        assert isinstance(saved_exc["temporal_distribution"], MockTemporalDistribution)
        assert saved_exc["temporal_distribution"].data == original_temporal_dist.data

        # Verify we can call to_json() on the restored object
        json_data = saved_exc["temporal_distribution"].to_json()
        assert json_data == {"type": "temporal_distribution", "data": {"year": 2020, "value": 0.5}}

    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


@bw2test
def test_temporal_distribution_restoration_logging_missing_library():
    """Test that appropriate logging occurs when bw_temporalis is not available"""

    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()

    # Create exchange with temporal_distribution JSON data
    exc = a.new_exchange(
        amount=1.0,
        input=b,
        type="technosphere",
        temporal_distribution={"type": "temporal_distribution", "data": {"year": 2020, "value": 0.5}}
    )
    exc.save()

    # Capture log messages
    with pytest.warns(UserWarning) as warning_list:
        # Get the saved exchange from database (this should trigger the warning)
        saved_exc = list(a.exchanges())[0]

    # Verify that temporal_distribution remains as JSON
    assert saved_exc["temporal_distribution"] == {"type": "temporal_distribution", "data": {"year": 2020, "value": 0.5}}


@bw2test
def test_temporal_distribution_restoration_logging_conversion_failure():
    """Test that appropriate logging occurs when TemporalDistribution conversion fails"""
    import bw2data.backends.proxies as proxies_module
    
    # Create a mock TemporalDistribution that will fail on construction
    class FailingTemporalDistribution:
        def __init__(self, data):
            raise ValueError("Simulated construction failure")
    
    # Temporarily replace the TemporalDistribution import
    original_temporal_distribution = proxies_module.TemporalDistribution
    proxies_module.TemporalDistribution = FailingTemporalDistribution
    
    try:
        db = DatabaseChooser("example")
        db.register()
        
        a = db.new_activity(code="A", name="An activity")
        a.save()
        b = db.new_activity(code="B", name="Another activity")
        b.save()
        
        # Create exchange with temporal_distribution JSON data
        exc = a.new_exchange(
            amount=1.0, 
            input=b, 
            type="technosphere", 
            temporal_distribution={"type": "temporal_distribution", "data": {"year": 2020, "value": 0.5}}
        )
        exc.save()
        
        # Capture log messages
        with pytest.warns(UserWarning) as warning_list:
            # Get the saved exchange from database (this should trigger the warning)
            saved_exc = list(a.exchanges())[0]
        
        # Verify that temporal_distribution remains as JSON due to conversion failure
        assert saved_exc["temporal_distribution"] == {"type": "temporal_distribution", "data": {"year": 2020, "value": 0.5}}
        
    finally:
        # Restore original import
        proxies_module.TemporalDistribution = original_temporal_distribution


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
