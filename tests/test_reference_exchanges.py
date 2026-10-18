from unittest.mock import Mock

from bw2data import get_id
from bw2data.backends import base
from bw2data.database import Database
from bw2data.tests import bw2test


def technosphere(database):
    """Return ``(row, col, flip, reference)`` for each technosphere array element."""
    package = database.datapackage()
    prefix = database.name.replace(" ", "_") + "_technosphere_matrix"
    indices = package.get_resource(prefix + ".indices")[0]

    def optional(suffix):
        # Boolean side resources are only written if any element is ``True``
        try:
            return package.get_resource(prefix + suffix)[0]
        except KeyError:
            return [False] * len(indices)

    flip, reference = optional(".flip"), optional(".reference")
    return [
        (int(row), int(col), bool(f), bool(r)) for (row, col), f, r in zip(indices, flip, reference)
    ]


def references(database):
    return {(row, col) for row, col, _, reference in technosphere(database) if reference}


def ids(*codes):
    return tuple(get_id(("db", code)) for code in codes)


def edge(input_code, edge_type, amount=1, **kwargs):
    return {"input": ("db", input_code), "type": edge_type, "amount": amount, **kwargs}


@bw2test
def test_implicit_production():
    db = Database("db")
    db.write({("db", "a"): {"type": "processwithreferenceproduct"}})
    assert references(db) == {ids("a", "a")}


@bw2test
def test_explicit_production_to_itself():
    db = Database("db")
    db.write(
        {
            ("db", "b"): {"type": "processwithreferenceproduct"},
            ("db", "a"): {
                "type": "processwithreferenceproduct",
                "exchanges": [edge("a", "production", 2), edge("b", "technosphere")],
            },
        }
    )
    assert references(db) == {ids("a", "a"), ids("b", "b")}


@bw2test
def test_separate_product_node():
    db = Database("db")
    db.write(
        {
            ("db", "product"): {"type": "product"},
            ("db", "a"): {"type": "process", "exchanges": [edge("product", "production")]},
        }
    )
    assert references(db) == {ids("product", "a")}


@bw2test
def test_substitution_never_flagged():
    db = Database("db")
    db.write(
        {
            ("db", "product"): {"type": "product"},
            ("db", "avoided"): {"type": "product"},
            ("db", "a"): {
                "type": "process",
                "exchanges": [
                    edge("product", "production"),
                    edge("avoided", "substitution", 0.5),
                ],
            },
        }
    )
    assert references(db) == {ids("product", "a")}


@bw2test
def test_functional_waste_input():
    db = Database("db")
    db.write(
        {
            ("db", "waste"): {"type": "product"},
            ("db", "electricity"): {"type": "product"},
            ("db", "incineration"): {
                "type": "process",
                "exchanges": [
                    edge("waste", "technosphere", functional=True),
                    edge("electricity", "production", 0.3),
                ],
            },
        }
    )
    assert references(db) == {ids("waste", "incineration")}
    (waste,) = [
        element for element in technosphere(db) if element[:2] == ids("waste", "incineration")
    ]
    assert waste[2], "flip must be kept on the functional input"


@bw2test
def test_functional_input_suppresses_implicit_production():
    db = Database("db")
    db.write(
        {
            ("db", "waste"): {"type": "product"},
            ("db", "a"): {
                "type": "processwithreferenceproduct",
                "exchanges": [edge("waste", "technosphere", functional=True)],
            },
        }
    )
    elements = technosphere(db)
    assert ids("a", "a") in {element[:2] for element in elements}
    assert references(db) == {ids("waste", "a")}


@bw2test
def test_co_production_prefers_production_to_itself():
    db = Database("db")
    db.write(
        {
            ("db", "co-product"): {"type": "product"},
            ("db", "a"): {
                "type": "processwithreferenceproduct",
                "exchanges": [edge("a", "production"), edge("co-product", "production", 0.2)],
            },
        }
    )
    assert references(db) == {ids("a", "a")}


@bw2test
def test_duplicate_production_edges_all_flagged():
    db = Database("db")
    db.write(
        {
            ("db", "product"): {"type": "product"},
            ("db", "a"): {
                "type": "process",
                "exchanges": [
                    edge("product", "production", 0.4),
                    edge("product", "production", 0.6),
                ],
            },
        }
    )
    flagged = [element for element in technosphere(db) if element[3]]
    assert [element[:2] for element in flagged] == [ids("product", "a")] * 2


@bw2test
def test_ambiguous_column_not_flagged(monkeypatch):
    logger = Mock()
    monkeypatch.setattr(base, "stdout_feedback_logger", logger)
    db = Database("db")
    db.write(
        {
            ("db", "one"): {"type": "product", "name": "one"},
            ("db", "two"): {"type": "product", "name": "two"},
            ("db", "a"): {
                "type": "process",
                # Name matching against `reference product` is deliberately not used
                "reference product": "one",
                "exchanges": [edge("one", "production"), edge("two", "production")],
            },
            ("db", "product"): {"type": "product"},
            ("db", "b"): {"type": "process", "exchanges": [edge("product", "production")]},
        }
    )
    assert references(db) == {ids("product", "b")}
    (message,) = logger.warning.call_args[0]
    assert message.startswith("1 of 2 processes in database db")


@bw2test
def test_no_reference_resource_without_flags():
    db = Database("db")
    db.write(
        {
            ("db", "avoided"): {"type": "product"},
            ("db", "a"): {"type": "process", "exchanges": [edge("avoided", "substitution")]},
        }
    )
    assert technosphere(db)
    package = db.datapackage()
    assert "db_technosphere_matrix.reference" not in {
        resource["name"] for resource in package.resources
    }


@bw2test
def test_columns_with_interleaved_edges():
    # Negative and positive edges come from separate queries; check they are merged by column
    db = Database("db")
    db.write(
        {
            ("db", "p1"): {"type": "product"},
            ("db", "p2"): {"type": "product"},
            ("db", "p3"): {"type": "product"},
            ("db", "a"): {
                "type": "process",
                "exchanges": [edge("p1", "production"), edge("p2", "technosphere")],
            },
            ("db", "b"): {
                "type": "process",
                "exchanges": [edge("p3", "technosphere"), edge("p2", "production")],
            },
            ("db", "c"): {
                "type": "process",
                "exchanges": [edge("p1", "technosphere"), edge("p3", "production")],
            },
            ("db", "d"): {
                "type": "processwithreferenceproduct",
                "exchanges": [edge("p2", "technosphere")],
            },
        }
    )
    assert references(db) == {
        ids("p1", "a"),
        ids("p2", "b"),
        ids("p3", "c"),
        ids("d", "d"),
    }
    assert len(technosphere(db)) == 8
