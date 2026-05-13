from bw2data import databases, get_id, projects
from bw2data.backends.schema import _get_id_cache
from bw2data.database import Database
from bw2data.tests import bw2test


def _write_db(name="db", data=None):
    db = Database(name)
    db.write(data or {(name, "a"): {}, (name, "b"): {}})
    return db


@bw2test
def test_get_id_cache_populates():
    _write_db()
    get_id(("db", "a"))
    assert ("db", "a") in _get_id_cache


@bw2test
def test_get_id_cache_returns_same_value():
    _write_db()
    first = get_id(("db", "a"))
    second = get_id(("db", "a"))
    assert first == second


@bw2test
def test_get_id_cache_project_changed():
    _write_db()
    get_id(("db", "a"))
    assert ("db", "a") in _get_id_cache

    projects.set_current("another project")

    assert len(_get_id_cache) == 0


@bw2test
def test_get_id_cache_database_delete():
    _write_db("foo")
    _write_db("bar")
    get_id(("foo", "a"))
    get_id(("bar", "a"))
    assert ("foo", "a") in _get_id_cache
    assert ("bar", "a") in _get_id_cache

    del databases["foo"]

    assert ("foo", "a") not in _get_id_cache
    assert ("bar", "a") in _get_id_cache


@bw2test
def test_get_id_cache_database_reset():
    _write_db("foo")
    _write_db("bar")
    get_id(("foo", "a"))
    get_id(("bar", "a"))
    assert ("foo", "a") in _get_id_cache
    assert ("bar", "a") in _get_id_cache

    Database("foo").delete(warn=False)

    assert ("foo", "a") not in _get_id_cache
    assert ("bar", "a") in _get_id_cache


@bw2test
def test_get_id_cache_database_write():
    _write_db("foo")
    _write_db("bar")
    get_id(("foo", "a"))
    get_id(("bar", "a"))
    assert ("foo", "a") in _get_id_cache
    assert ("bar", "a") in _get_id_cache

    Database("foo").write({("foo", "x"): {}})

    assert ("foo", "a") not in _get_id_cache
    assert ("bar", "a") in _get_id_cache


@bw2test
def test_get_id_cache_activity_delete():
    _write_db("foo")
    _write_db("bar")
    get_id(("foo", "a"))
    get_id(("foo", "b"))
    get_id(("bar", "a"))
    assert ("foo", "a") in _get_id_cache
    assert ("foo", "b") in _get_id_cache
    assert ("bar", "a") in _get_id_cache

    Database("foo").get("a").delete()

    assert ("foo", "a") not in _get_id_cache
    assert ("foo", "b") in _get_id_cache
    assert ("bar", "a") in _get_id_cache


@bw2test
def test_get_id_cache_activity_code_change():
    _write_db("foo")
    _write_db("bar")
    get_id(("foo", "a"))
    get_id(("foo", "b"))
    get_id(("bar", "a"))
    assert ("foo", "a") in _get_id_cache

    act = Database("foo").get("a")
    act["code"] = "new_code"

    assert ("foo", "a") not in _get_id_cache
    assert ("foo", "b") in _get_id_cache
    assert ("bar", "a") in _get_id_cache


@bw2test
def test_get_id_cache_activity_database_change():
    _write_db("foo")
    _write_db("bar")
    get_id(("foo", "a"))
    get_id(("foo", "b"))
    get_id(("bar", "a"))
    assert ("foo", "a") in _get_id_cache

    act = Database("foo").get("a")
    act["database"] = "bar"

    assert ("foo", "a") not in _get_id_cache
    assert ("foo", "b") in _get_id_cache
    assert ("bar", "a") in _get_id_cache
