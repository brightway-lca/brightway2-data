"""``preferences`` stored in SQLite, and the versioned schema migrations of ``lci/databases.db``."""

import pickle
import sqlite3
import warnings

import pytest

from bw2data import preferences, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import (
    MIGRATIONS,
    SCHEMA_VERSION,
    Migration,
    migrate,
    schema_version,
)
from bw2data.backends.schema import Preference
from bw2data.tests import bw2test


def _stored():
    return dict(Preference.select().tuples())


def _write_legacy_pickle(project_dir, data):
    with open(project_dir / "preferences.pickle", "wb") as f:
        pickle.dump(data, f, protocol=4)


def _reset_schema_version():
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 0")


# ----------------------------------------------------------------------
# Schema versioning
# ----------------------------------------------------------------------


def test_migration_versions_are_consecutive():
    assert [m.version for m in MIGRATIONS] == list(range(1, len(MIGRATIONS) + 1))
    assert SCHEMA_VERSION == len(MIGRATIONS)


@bw2test
def test_new_project_is_at_current_schema_version():
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    projects.set_current("another one")
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION


@bw2test
def test_migrate_is_noop_when_up_to_date():
    assert migrate(sqlite3_lci_db.db, projects.dir) == []


@bw2test
def test_migrate_runs_pending_steps_in_order(monkeypatch):
    calls = []
    steps = (
        Migration(1, "one", lambda db, path: calls.append(1)),
        Migration(2, "two", lambda db, path: calls.append(2)),
        Migration(3, "three", lambda db, path: calls.append(3)),
    )
    monkeypatch.setattr("bw2data.backends.migrations.MIGRATIONS", steps)
    monkeypatch.setattr("bw2data.backends.migrations.SCHEMA_VERSION", 3)

    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 1")
    assert migrate(sqlite3_lci_db.db, projects.dir) == [2, 3]
    assert calls == [2, 3]
    assert schema_version(sqlite3_lci_db.db) == 3


@bw2test
def test_failed_migration_changes_nothing(monkeypatch):
    def boom(db, path):
        raise RuntimeError("boom")

    steps = (
        Migration(1, "write", lambda db, path: Preference.create(key="half", value=1)),
        Migration(2, "fail", boom),
    )
    monkeypatch.setattr("bw2data.backends.migrations.MIGRATIONS", steps)
    monkeypatch.setattr("bw2data.backends.migrations.SCHEMA_VERSION", 2)
    _reset_schema_version()

    with pytest.raises(RuntimeError):
        migrate(sqlite3_lci_db.db, projects.dir)
    assert schema_version(sqlite3_lci_db.db) == 0
    assert "half" not in _stored()


@bw2test
def test_newer_schema_version_warns_and_is_left_alone():
    sqlite3_lci_db.execute_sql(f"PRAGMA main.user_version = {SCHEMA_VERSION + 1}")
    with pytest.warns(UserWarning, match="newer version of bw2data"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == []
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION + 1


@bw2test
def test_schema_version_is_read_from_main_only(tmp_path):
    other = tmp_path / "other.db"
    with sqlite3.connect(other) as conn:
        conn.execute("PRAGMA user_version = 99")
    conn.close()
    sqlite3_lci_db.execute_sql("ATTACH ? AS other", (str(other),))
    try:
        assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    finally:
        sqlite3_lci_db.execute_sql("DETACH other")


# ----------------------------------------------------------------------
# Preferences migration
# ----------------------------------------------------------------------


@bw2test
def test_preferences_pickle_is_migrated_on_project_open():
    projects.set_current("legacy")
    legacy_dir = projects.dir
    projects.set_current("other")

    # Simulate a project last opened by an older bw2data
    from bw2data.updates import Updates

    legacy = {
        "use_cache": False,
        "biosphere_database": "bio",
        "nested": {"a": [1, 2]},
        "updates": {key: True for key in Updates.UPDATES},
    }
    _write_legacy_pickle(legacy_dir, legacy)
    with sqlite3.connect(legacy_dir / "lci" / "databases.db") as conn:
        conn.execute("DELETE FROM preference")
        conn.execute("PRAGMA user_version = 0")
    conn.close()

    projects.set_current("legacy")
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert _stored() == legacy
    assert preferences["biosphere_database"] == "bio"
    assert preferences["use_cache"] is False
    # The legacy file is kept for older versions of bw2data
    assert (legacy_dir / "preferences.pickle").is_file()


@bw2test
def test_preferences_migration_runs_only_once():
    _write_legacy_pickle(projects.dir, {"from_pickle": 1})
    _reset_schema_version()
    migrate(sqlite3_lci_db.db, projects.dir)
    Preference.delete().where(Preference.key == "from_pickle").execute()

    # Already at the current version: the pickle is not read again
    migrate(sqlite3_lci_db.db, projects.dir)
    assert "from_pickle" not in _stored()


@bw2test
def test_preferences_migration_does_not_overwrite_existing_rows():
    preferences["kept"] = "sqlite"
    _write_legacy_pickle(projects.dir, {"kept": "pickle", "other": 1})
    _reset_schema_version()
    migrate(sqlite3_lci_db.db, projects.dir)
    assert _stored()["kept"] == "sqlite"
    assert "other" not in _stored()


@bw2test
def test_preferences_migration_skips_non_string_keys():
    _write_legacy_pickle(projects.dir, {"ok": 1, ("a", "b"): 2, 3: 4})
    _reset_schema_version()
    with pytest.warns(UserWarning, match="not strings"):
        migrate(sqlite3_lci_db.db, projects.dir)
    assert _stored() == {"ok": 1}


@bw2test
def test_preferences_migration_unreadable_file_warns():
    (projects.dir / "preferences.pickle").write_bytes(b"not a pickle")
    _reset_schema_version()
    with pytest.warns(UserWarning, match="default preferences"):
        migrate(sqlite3_lci_db.db, projects.dir)
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION


@bw2test
def test_no_preferences_pickle_written():
    preferences["x"] = 1
    assert not (projects.dir / "preferences.pickle").exists()


# ----------------------------------------------------------------------
# Preferences mapping
# ----------------------------------------------------------------------


@bw2test
def test_default_use_cache_is_saved():
    assert preferences["use_cache"] is True
    assert _stored()["use_cache"] is True


@bw2test
def test_set_get_delete():
    preferences["output_dir"] = "/somewhere"
    assert preferences["output_dir"] == "/somewhere"
    assert "output_dir" in preferences
    assert _stored()["output_dir"] == "/somewhere"

    del preferences["output_dir"]
    assert "output_dir" not in preferences
    assert "output_dir" not in _stored()
    with pytest.raises(KeyError):
        preferences["output_dir"]


@bw2test
def test_non_string_key_raises():
    with pytest.raises(TypeError):
        preferences[("a", "b")] = 1
    assert ("a", "b") not in preferences


@bw2test
def test_nested_change_is_saved_by_flush():
    preferences["updates"] = {"a": False}
    preferences["updates"]["a"] = True
    assert _stored()["updates"] == {"a": False}
    preferences.flush()
    assert _stored()["updates"] == {"a": True}


@bw2test
def test_nested_change_is_saved_by_next_assignment():
    preferences["updates"] = {"a": False}
    preferences["updates"]["a"] = True
    preferences["other"] = 1
    assert _stored()["updates"] == {"a": True}


@bw2test
def test_values_keep_python_types():
    value = {"t": (1, 2), "s": {1, 2}, "b": b"x"}
    preferences["complex"] = value
    preferences.load()
    assert preferences["complex"] == value


@bw2test
def test_preferences_follow_project_switch():
    first = projects.current
    preferences["where"] = "first"
    projects.set_current("second")
    assert "where" not in preferences
    preferences["where"] = "second"
    projects.set_current(first)
    assert preferences["where"] == "first"


@bw2test
def test_copy_project_keeps_preferences():
    preferences["kept"] = 42
    projects.copy_project("copied")
    assert projects.current == "copied"
    assert preferences["kept"] == 42


@bw2test
def test_mapping_interface():
    for key in list(preferences):
        del preferences[key]
    preferences.update({"b": 2, "a": 1})
    assert len(preferences) == 2
    assert preferences.list == ["a", "b"]
    assert dict(preferences) == {"a": 1, "b": 2}
    assert preferences.get("missing", "default") == "default"
    assert "2 object(s)" in repr(preferences)


@bw2test
def test_config_p_is_preferences():
    from bw2data import config

    assert config.p is preferences
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert config.biosphere == "biosphere3"
