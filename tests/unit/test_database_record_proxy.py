"""Database metadata changes are kept in memory until explicitly saved."""

import pytest

from bw2data import Database, databases
from bw2data.backends.schema import DatabaseRecord
from bw2data.errors import UnknownObject
from bw2data.tests import bw2test


def stored(name):
    """Metadata as saved in SQLite, ignoring pending changes."""
    return databases.data[name]


@bw2test
def test_changes_are_pending_until_save():
    db = Database("a")
    db.register()

    db.metadata["description"] = "Example"
    assert db.metadata["description"] == "Example"
    assert "description" not in stored("a")

    db.metadata.save()
    assert stored("a")["description"] == "Example"


@bw2test
def test_database_metadata_and_databases_share_pending_changes():
    db = Database("a")
    db.register()

    db.metadata["description"] = "Example"
    assert databases["a"] is db.metadata
    assert databases["a"]["description"] == "Example"

    databases["a"].save()
    assert stored("a")["description"] == "Example"


@bw2test
def test_delete_is_pending_until_save():
    databases["a"] = {"backend": "sqlite", "custom": 1}

    del databases["a"]["custom"]
    del databases["a"]["backend"]
    assert stored("a") == {"backend": "sqlite", "custom": 1}

    databases["a"].save()
    assert stored("a") == {}


@bw2test
def test_delete_missing_key_raises():
    databases["a"] = {"backend": "sqlite"}
    with pytest.raises(KeyError):
        del databases["a"]["missing"]
    with pytest.raises(KeyError):
        del databases["a"]["dirty"]


@bw2test
def test_nested_in_place_change_is_saved():
    databases["a"] = {"backend": "sqlite", "depends": [], "custom": {"x": 1}}

    databases["a"]["depends"].append("b")
    databases["a"]["custom"]["y"] = 2
    assert stored("a")["depends"] == []

    databases["a"].save()
    assert stored("a")["depends"] == ["b"]
    assert stored("a")["custom"] == {"x": 1, "y": 2}


@bw2test
def test_unchanged_proxy_is_refreshed_from_sqlite():
    databases["a"] = {"backend": "sqlite"}
    proxy = databases["a"]

    DatabaseRecord.update(searchable=True).where(DatabaseRecord.name == "a").execute()

    assert databases["a"] is proxy
    assert proxy["searchable"] is True


@bw2test
def test_save_writes_only_changed_keys():
    databases["a"] = {"backend": "sqlite", "first": 1}
    proxy = databases["a"]
    proxy["second"] = 2
    proxy["number"] = 5

    # Saved by other code (e.g. another process) while `proxy` has pending changes
    DatabaseRecord.update(searchable=True, extra={"first": 1, "third": 3}).where(
        DatabaseRecord.name == "a"
    ).execute()

    proxy.save()
    assert stored("a") == {
        "backend": "sqlite",
        "searchable": True,
        "number": 5,
        "first": 1,
        "second": 2,
        "third": 3,
    }
    assert dict(proxy) == stored("a")


@bw2test
def test_replacing_whole_entry_is_saved_immediately():
    databases["a"] = {"backend": "sqlite"}
    proxy = databases["a"]

    databases["a"] = {"backend": "iotable", "description": "Replaced"}
    assert stored("a") == {"backend": "iotable", "description": "Replaced"}
    assert dict(proxy) == stored("a")


@bw2test
def test_set_dirty_saves_pending_changes():
    databases["a"] = {"backend": "sqlite"}
    databases["a"]["description"] = "Example"

    databases.set_dirty("a")
    assert stored("a")["dirty"] is True
    assert stored("a")["description"] == "Example"


@bw2test
def test_flush_warns_and_saves_all_pending_changes():
    databases["a"] = {"backend": "sqlite"}
    databases["b"] = {"backend": "sqlite"}
    databases["a"]["description"] = "A"
    databases["b"]["description"] = "B"

    with pytest.warns(DeprecationWarning, match=r"metadata\.save\(\)"):
        databases.flush()

    assert stored("a")["description"] == "A"
    assert stored("b")["description"] == "B"


@bw2test
def test_save_after_database_deleted_raises():
    databases["a"] = {"backend": "sqlite"}
    proxy = databases["a"]
    proxy["description"] = "Example"

    DatabaseRecord.delete().where(DatabaseRecord.name == "a").execute()

    with pytest.raises(UnknownObject):
        proxy.save()


@bw2test
def test_save_without_changes_is_a_no_op():
    databases["a"] = {"backend": "sqlite"}
    databases["a"].save()
    assert stored("a") == {"backend": "sqlite"}
