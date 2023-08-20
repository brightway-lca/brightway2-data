import pytest

from bw2data import databases, geomapping, methods, projects
from bw2data.backends.utils import convert_backend
from bw2data.database import DatabaseChooser
from bw2data.errors import UnknownObject
from bw2data.tests import bw2test


@pytest.fixture
@bw2test
def db_query_fixture():
    db = DatabaseChooser("Order!")
    db.write(
        {
            ("Order!", "first"): {
                "name": "a",
                "location": "delaware",
                "reference product": "widget",
            },
            ("Order!", "second"): {
                "name": "b",
                "location": "carolina",
                "reference product": "wiggle",
            },
            ("Order!", "third"): {
                "name": "c",
                "location": "baseball",
                "reference product": "lollipop",
            },
            ("Order!", "fourth"): {
                "name": "d",
                "location": "alabama",
                "reference product": "widget",
            },
        }
    )
    return db


def test_setup_clean(db_query_fixture):
    assert len(databases) == 1
    assert "Order!" in databases
    assert list(methods) == []
    assert len(geomapping) == 5  # GLO
    assert "GLO" in geomapping
    assert "carolina" in geomapping
    assert len(projects) == 2  # Default project
    assert "default" in projects


def test_random_with_global_filters(db_query_fixture):
    db = db_query_fixture
    db.filters = {"product": "lollipop"}
    for _ in range(10):
        assert db.random()["name"] == "c"


def test_random_with_local_filters(db_query_fixture):
    for _ in range(10):
        assert db_query_fixture.random(filters={"product": "lollipop"})["name"] == "c"


def test_random_with_local_and_global_filters(db_query_fixture):
    db = DatabaseChooser("Newt")
    db.write(
        {
            ("Newt", "first"): {
                "name": "a",
                "location": "delaware",
                "reference product": "widget",
            },
            ("Newt", "second"): {
                "name": "b",
                "location": "delaware",
                "reference product": "wiggle",
            },
            ("Newt", "third"): {
                "name": "c",
                "location": "alabama",
                "reference product": "widget",
            },
            ("Newt", "fourth"): {
                "name": "d",
                "location": "alabama",
                "reference product": "wiggle",
            },
        }
    )
    assert len({db.random()["name"] for _ in range(10)}) > 1
    db.filters = {"product": "widget"}
    for _ in range(10):
        assert db_query_fixture.random(filters={"location": "delaware"})["name"] == "a"


def test_contains_respects_filters(db_query_fixture):
    db_query_fixture.filters = {"product": "lollipop"}
    assert ("Order!", "fourth") not in db_query_fixture


def test_get_ignores_filters(db_query_fixture):
    db_query_fixture.filters = {"product": "giggles"}
    assert db_query_fixture.get("fourth")["name"] == "d"


def test_filter(db_query_fixture):
    db_query_fixture.filters = {"product": "widget"}
    assert len([x for x in db_query_fixture]) == 2


def test_order_by(db_query_fixture):
    db_query_fixture.order_by = "name"
    assert [x["name"] for x in db_query_fixture] == ["a", "b", "c", "d"]


def test_order_by_bad_field(db_query_fixture):
    with pytest.raises(AssertionError):
        db_query_fixture.order_by = "poopy"


def test_filter_bad_field(db_query_fixture):
    with pytest.raises(AssertionError):
        db_query_fixture.filters = {"poopy": "yuck"}


def test_filter_not_dict(db_query_fixture):
    with pytest.raises(AssertionError):
        db_query_fixture.filters = "poopy"


def test_reset_order_by(db_query_fixture):
    db_query_fixture.order_by = "name"
    db_query_fixture.order_by = None
    as_lists = [[x["name"] for x in db_query_fixture] for _ in range(10)]
    first_elements = {x[0] for x in as_lists}
    assert len(first_elements) > 1


def test_reset_filters(db_query_fixture):
    db_query_fixture.filters = {"product": "widget"}
    assert len([x for x in db_query_fixture]) == 2
    db_query_fixture.filters = None
    assert len([x for x in db_query_fixture]) == 4


def test_len_respects_filters(db_query_fixture):
    db_query_fixture.filters = {"product": "widget"}
    assert len(db_query_fixture) == 2


@bw2test
def test_make_searchable_unknown_object():
    db = DatabaseChooser("mysterious")
    with pytest.raises(UnknownObject):
        db.make_searchable()


@bw2test
def test_convert_same_backend():
    database = DatabaseChooser("a database")
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
    assert not convert_backend("a database", "sqlite")


@pytest.mark.skip(reason="Not an actual test")
@bw2test
def test_convert_backend():
    database = DatabaseChooser("a database")
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
