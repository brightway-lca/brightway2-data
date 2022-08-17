from bw2data import Method, Database, databases, methods, projects, get_activity, get_id, get_node
from bw2data.backends import Activity
import pytest
from bw2data.tests import bw2test
from bw2calc import LCA
import numpy as np
from pandas.testing import assert_frame_equal
import pandas as pd
from bw2data.backends.iotable.proxies import ReadOnlyExchange, IOTableExchanges, IOTableActivity


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
                "flip": p
            }
            for m, n, o, p in tech_exchanges
        ],
        biosphere=[
            {
                "row": get_activity(code=m).id,
                "col": get_activity(code=n).id,
                "amount": o,
                "flip": p
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
        print(lca.technosphere_matrix[
                lca.dicts.product[get_activity(code=a).id],
                lca.dicts.activity[get_activity(code=b).id],
            ])
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
                "target_database": "mouse" if a == "d" else "cat",
                "target_code": a,
                "target_name": get_activity(code=a).get("name"),
                "target_reference_product": None,
                "target_location": get_activity(code=a).get("location"),
                "target_unit": get_activity(code=a).get("unit"),
                "target_type": get_activity(code=a).get("type") or 'process',
                "source_id": id_map[b],
                "source_database": "mouse" if b == "d" else "cat",
                "source_code": b,
                "source_name": get_activity(code=b).get("name"),
                "source_product": None,
                "source_location": get_activity(code=b).get("location"),
                "source_unit": get_activity(code=b).get("unit"),
                "source_categories": None,
                "edge_amount": c,
                "edge_type": "technosphere" if ((-1 if d else 1) * c) < 0 else "production",
            }
            for b, a, c, d in tech_exchanges
        ]
        + [
            {
                "target_id": id_map[a],
                "target_database": "mouse" if a == "d" else "cat",
                "target_code": a,
                "target_name": get_activity(code=a).get("name"),
                "target_reference_product": None,
                "target_location": get_activity(code=a).get("location"),
                "target_unit": get_activity(code=a).get("unit"),
                "target_type": get_activity(code=a).get("type") or 'process',
                "source_id": id_map[b],
                "source_database": "mouse" if b == "d" else "cat",
                "source_code": b,
                "source_name": get_activity(code=b).get("name"),
                "source_product": None,
                "source_location": get_activity(code=b).get("location"),
                "source_unit": get_activity(code=b).get("unit"),
                "source_categories": None,
                "edge_amount": c,
                "edge_type": "biosphere",
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
    assert act['name'] == 'a'
    assert act['unit'] == 'meow'
    assert act['location'] == 'sunshine'

    with pytest.raises(NotImplementedError):
        act.delete()

    with pytest.raises(ValueError):
        act.rp_exchange()


def test_correct_backend_fixture(iotable_fixture):
    act = get_activity(("mouse", "d"))
    assert not isinstance(act, IOTableActivity)


# def test_production(activity_and_method):
#     activity, method = activity_and_method
#     assert len(list(activity.production())) == 1
#     assert len(activity.production()) == 1
#     exc = list(activity.production())[0]
#     assert exc["amount"] == 2


# def test_substitution(activity_and_method):
#     activity, method = activity_and_method
#     assert len(activity.substitution()) == 0


# def test_biosphere(activity_and_method):
#     activity, method = activity_and_method
#     for exc in activity.exchanges():
#         print(exc)
#     assert len(list(activity.biosphere())) == 1
#     assert len(activity.biosphere()) == 1
#     exc = list(activity.biosphere())[0]
#     assert exc["amount"] == 4


# def test_technosphere(activity_and_method):
#     activity, method = activity_and_method
#     assert len(list(activity.technosphere())) == 1
#     assert len(activity.technosphere()) == 1
#     exc = list(activity.technosphere())[0]
#     assert exc["amount"] == 3


# def test_readonlyexchange(activity_and_method):
#     # test if all properties and methods implemented in class ReadOnlyExchange are working correctly

#     activity, method = activity_and_method
#     exc = list(activity.technosphere())[0]
#     # __str__
#     assert str(exc) == "Exchange: 3.0 None 'd' (None, None, None) to 'a' (None, None, None)>"
#     # __contains__
#     assert "output" in exc
#     # __eq__
#     assert exc == exc
#     # __len__
#     assert len(exc) == 4
#     # __hash__
#     assert exc.__hash__() is not None
#     # output getter
#     assert exc.output == activity
#     # input getter
#     assert exc.input == get_activity(("db","d"))
#     # amount getter
#     assert exc.amount == 3
#     # unit getter
#     assert exc.unit == None
#     # type getter
#     assert exc['type'] == "technosphere"
#     # as_dict
#     assert exc.as_dict() == {'input': ('db', 'd'), 'output': ('db', 'a'), 'amount': 3.0, 'type': 'technosphere'}
#     # valid
#     assert exc.valid()
#     # lca
#     exc.lca(method.name, 1)

#     # test if exchange is read-only
#     with pytest.raises(AttributeError, match="can't set attribute"):
#         exc.input = exc.output
#         exc.output = exc.input
#         exc.amount = 1
#     with pytest.raises(AttributeError, match="'ReadOnlyExchange' object has no attribute .*"):
#         exc.delete()
#         exc.save()
#     with pytest.raises(TypeError, match="'ReadOnlyExchange' object does not support item assignment"):
#         exc['type'] = 'biosphere'
