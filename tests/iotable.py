import numpy as np
import pandas as pd
import pytest
from bw2calc import LCA
from pandas.testing import assert_frame_equal

from bw2data import (
    Database,
    Method,
    databases,
    get_activity,
    get_id,
    get_node,
    methods,
    projects,
)
from bw2data.backends import Activity
from bw2data.backends.iotable.proxies import (
    IOTableActivity,
    IOTableExchanges,
    ReadOnlyExchange,
)
from bw2data.errors import InvalidDatapackage
from bw2data.tests import bw2test


@pytest.fixture
@bw2test
def iotable_fixture():
    """Technosphere matrix:

        a   b   c
    a   2   0   -3
    b   -1  1   0
    c   4   0.2 -1

    Biosphere matrix:

        a   b   c
    d   0   1   2

    Characterization matrix:

        d
    d   42

    """
    tech_exchanges = [
        ("a", "a", 2, False),
        ("a", "c", 3, True),
        ("b", "a", -1, False),
        ("b", "b", -1, True),
        ("c", "a", 4, False),
        ("c", "b", 0.2, True),
        ("c", "c", 1, True),
    ]
    bio_exchanges = [
        ("d", "b", -1, True),
        ("d", "c", 2, False),
    ]

    mouse = Database("mouse")
    mouse.write({("mouse", "d"): {"name": "squeak", "type": "emission"}})

    cat = Database("cat", backend="iotable")
    cat_data = {
        ("cat", "a"): {"name": "a", "unit": "meow", "location": "sunshine"},
        ("cat", "b"): {"name": "b", "unit": "purr", "location": "curled up"},
        ("cat", "c"): {"name": "c", "unit": "meow", "location": "on lap"},
    }
    cat.write(cat_data)
    cat.write_exchanges(
        technosphere=[
            {
                "row": get_activity(code=m).id,
                "col": get_activity(code=n).id,
                "amount": o,
                "flip": p,
            }
            for m, n, o, p in tech_exchanges
        ],
        biosphere=[
            {
                "row": get_activity(code=m).id,
                "col": get_activity(code=n).id,
                "amount": o,
                "flip": p,
            }
            for m, n, o, p in bio_exchanges
        ],
        dependents=["mouse"],
    )

    cfs = [(("mouse", "d"), 42)]
    method = Method(("a method",))
    method.register()
    method.write(cfs)


def test_iotable_setup_clean(iotable_fixture):
    print(databases)
    assert len(databases) == 2
    assert list(methods) == [("a method",)]
    assert len(projects) == 2  # Default project
    assert "default" in projects


def test_iotable_matrix_construction(iotable_fixture):
    lca = LCA({("cat", "a"): 1}, ("a method",))
    lca.lci()
    lca.lcia()

    tech_values = [
        ("a", "a", 2, False),
        ("a", "c", 3, True),
        ("b", "a", -1, False),
        ("b", "b", -1, True),
        ("c", "a", 4, False),
        ("c", "b", 0.2, True),
        ("c", "c", 1, True),
    ]
    for a, b, c, d in tech_values:
        print(a, b, c, d)
        print(
            lca.technosphere_matrix[
                lca.dicts.product[get_activity(code=a).id],
                lca.dicts.activity[get_activity(code=b).id],
            ]
        )
        assert np.allclose(
            lca.technosphere_matrix[
                lca.dicts.product[get_activity(code=a).id],
                lca.dicts.activity[get_activity(code=b).id],
            ],
            (-1 if d else 1) * c,
        )
    assert np.allclose(lca.technosphere_matrix.sum(), 2 - 3 - 1 + 1 + 4 - 0.2 - 1)

    bio_values = [
        ("d", "b", -1, True),
        ("d", "c", 2, False),
    ]
    for a, b, c, d in bio_values:
        assert np.allclose(
            lca.biosphere_matrix[
                lca.dicts.biosphere[get_activity(code=a).id],
                lca.dicts.activity[get_activity(code=b).id],
            ],
            (-1 if d else 1) * c,
        )
    assert np.allclose(lca.biosphere_matrix.sum(), 3)

    assert np.allclose(lca.characterization_matrix.sum(), 42)
    assert lca.characterization_matrix.shape == (1, 1)


def test_iotable_process_method(iotable_fixture):
    Database("cat").process()


def test_iotable_edges_to_dataframe(iotable_fixture):
    df = Database("cat").edges_to_dataframe()
    id_map = {obj["code"]: obj.id for obj in Database("cat")} | {
        obj["code"]: obj.id for obj in Database("mouse")
    }

    tech_exchanges = [
        ("a", "a", 2, False),
        ("a", "c", 3, True),
        ("b", "a", -1, False),
        ("b", "b", -1, True),
        ("c", "a", 4, False),
        ("c", "b", 0.2, True),
        ("c", "c", 1, True),
    ]
    bio_exchanges = [
        ("d", "b", -1, True),
        ("d", "c", 2, False),
    ]
    expected = pd.DataFrame(
        [
            {
                "target_id": id_map[a],
                "source_id": id_map[b],
                "edge_amount": c,
                "edge_type": "technosphere"
                if ((-1 if d else 1) * c) < 0
                else "production",
                "target_database": "mouse" if a == "d" else "cat",
                "target_code": a,
                "target_name": get_activity(code=a).get("name"),
                "target_location": get_activity(code=a).get("location"),
                "target_unit": get_activity(code=a).get("unit"),
                "target_type": get_activity(code=a).get("type") or "process",
                "target_reference_product": None,
                "source_database": "mouse" if b == "d" else "cat",
                "source_code": b,
                "source_name": get_activity(code=b).get("name"),
                "source_location": get_activity(code=b).get("location"),
                "source_unit": get_activity(code=b).get("unit"),
                "source_categories": None,
                "source_product": None,
            }
            for b, a, c, d in tech_exchanges
        ]
        + [
            {
                "target_id": id_map[a],
                "source_id": id_map[b],
                "edge_amount": c,
                "edge_type": "biosphere",
                "target_database": "mouse" if a == "d" else "cat",
                "target_code": a,
                "target_name": get_activity(code=a).get("name"),
                "target_location": get_activity(code=a).get("location"),
                "target_unit": get_activity(code=a).get("unit"),
                "target_type": get_activity(code=a).get("type") or "process",
                "target_reference_product": None,
                "source_database": "mouse" if b == "d" else "cat",
                "source_code": b,
                "source_name": get_activity(code=b).get("name"),
                "source_location": get_activity(code=b).get("location"),
                "source_unit": get_activity(code=b).get("unit"),
                "source_categories": None,
                "source_product": None,
            }
            for b, a, c, d in bio_exchanges
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


def test_iotable_nodes_to_dataframe(iotable_fixture):
    df = Database("cat").nodes_to_dataframe()
    expected = pd.DataFrame(
        [
            {
                "code": "a",
                "database": "cat",
                "id": get_id(("cat", "a")),
                "location": "sunshine",
                "name": "a",
                "unit": "meow",
            },
            {
                "code": "b",
                "database": "cat",
                "id": get_id(("cat", "b")),
                "location": "curled up",
                "name": "b",
                "unit": "purr",
            },
            {
                "code": "c",
                "database": "cat",
                "id": get_id(("cat", "c")),
                "location": "on lap",
                "name": "c",
                "unit": "meow",
            },
        ]
    )
    assert_frame_equal(
        df.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_iotable_get_methods_correct_class(iotable_fixture):
    act = get_activity(("cat", "a"))
    assert isinstance(act, IOTableActivity)

    act = get_node(code="a")
    assert isinstance(act, IOTableActivity)

    act = Database("cat").get(code="a")
    assert isinstance(act, IOTableActivity)

    act = get_activity(("mouse", "d"))
    assert isinstance(act, Activity)

    act = get_node(code="d")
    assert isinstance(act, Activity)

    act = Database("mouse").get(code="d")
    assert isinstance(act, Activity)


def test_iotable_activity(iotable_fixture):
    act = get_activity(("cat", "a"))
    assert act["name"] == "a"
    assert act["unit"] == "meow"
    assert act["location"] == "sunshine"

    with pytest.raises(NotImplementedError):
        act.delete()

    with pytest.raises(ValueError):
        act.rp_exchange()


def test_iotable_activity_edges_to_dataframe(iotable_fixture):
    df = get_node(code="a").exchanges().to_dataframe()
    id_map = {obj["code"]: obj.id for obj in Database("cat")} | {
        obj["code"]: obj.id for obj in Database("mouse")
    }

    tech_exchanges = [
        ("a", "a", 2, False),
        ("b", "a", -1, False),
        ("c", "a", 4, False),
    ]
    expected = pd.DataFrame(
        [
            {
                "target_id": id_map[a],
                "target_database": "mouse" if a == "d" else "cat",
                "target_code": a,
                "target_name": get_activity(code=a).get("name"),
                "target_reference_product": None,
                "target_location": get_activity(code=a).get("location"),
                "target_unit": get_activity(code=a).get("unit"),
                "target_type": get_activity(code=a).get("type") or "process",
                "source_id": id_map[b],
                "source_database": "mouse" if b == "d" else "cat",
                "source_code": b,
                "source_name": get_activity(code=b).get("name"),
                "source_product": None,
                "source_location": get_activity(code=b).get("location"),
                "source_unit": get_activity(code=b).get("unit"),
                "source_categories": None,
                "edge_amount": c,
                "edge_type": "technosphere"
                if ((-1 if d else 1) * c) < 0
                else "production",
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


def test_correct_backend_fixture(iotable_fixture):
    act = get_activity(("mouse", "d"))
    assert not isinstance(act, IOTableActivity)


def test_iotable_edges_production(iotable_fixture):
    act = get_activity(("cat", "a"))
    assert len(act.production()) == 2
    exc = next(iter(act.production()))
    assert exc.input == act
    assert exc.output in (act, get_activity(("cat", "c")))
    assert isinstance(exc, ReadOnlyExchange)
    assert exc["amount"] in (2, 4)


def test_iotable_edges_technosphere(iotable_fixture):
    act = get_activity(("cat", "a"))
    assert len(act.technosphere()) == 1
    exc = next(iter(act.technosphere()))
    print(exc)
    assert exc.input == get_activity(("cat", "b"))
    assert exc.output == act
    assert isinstance(exc, ReadOnlyExchange)
    assert exc["amount"] == -1


def test_iotable_edges_biosphere(iotable_fixture):
    act = get_activity(("cat", "b"))
    assert len(act.biosphere()) == 1
    exc = next(iter(act.biosphere()))
    assert exc.input == get_activity(("mouse", "d"))
    assert exc.output == act
    assert isinstance(exc, ReadOnlyExchange)
    assert exc["amount"] == -1


def test_substitution(iotable_fixture):
    act = get_activity(("cat", "b"))
    assert len(list(act.substitution())) == 0


def test_iotabe_readonlyexchange(iotable_fixture):
    a = get_node(code="b")
    exc = next(iter(a.technosphere()))
    exc2 = next(iter(a.biosphere()))

    assert exc < exc2

    assert str(exc)
    assert "output" in exc
    assert exc == exc
    assert len(exc) == len(exc._data)
    assert hash(exc)
    assert isinstance(exc.input, IOTableActivity)
    assert isinstance(exc.output, IOTableActivity)
    assert exc.amount

    assert exc["type"] in ("technosphere", "production")
    assert isinstance(exc.as_dict(), dict)

    exc.lca(("a method",), 1)


def test_iotabe_readonlyexchange_missing_methods(iotable_fixture):
    a = get_node(code="b")
    exc = next(iter(a.technosphere()))

    with pytest.raises(AttributeError):
        exc.save()
    with pytest.raises(AttributeError):
        exc.delete()


def test_iotabe_readonlyexchange_not_setitem(iotable_fixture):
    a = get_node(code="b")
    exc = next(iter(a.technosphere()))

    with pytest.raises(
        TypeError, match="'ReadOnlyExchange' object does not support item assignment"
    ):
        exc["type"] = "biosphere"


def test_iotable_filtered_datapackage(iotable_fixture):
    dp = Database("cat").datapackage()
    IOTableExchanges(datapackage=dp, target=get_node(code="b"))
    with pytest.raises(InvalidDatapackage):
        IOTableExchanges(datapackage=dp, target=get_node(code="a"))
