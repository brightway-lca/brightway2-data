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
from bw2data.parameters import parameters
from bw2data.sqlite import BWSqliteDatabase, _sqlite_uri
from bw2data.tests import bw2test


def _stored():
    return dict(Preference.select().tuples())


def _write_legacy_pickle(project_dir, data):
    with open(project_dir / "preferences.pickle", "wb") as f:
        pickle.dump(data, f, protocol=4)


def _reset_schema_version():
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 0")


class _WriteLock:
    """Hold the write lock on the current ``lci/databases.db`` from a second connection, as
    another process would. The connection of this process doesn't wait for the lock."""

    def __enter__(self):
        sqlite3_lci_db.execute_sql("PRAGMA busy_timeout = 0")
        self.conn = sqlite3.connect(sqlite3_lci_db.db.database, isolation_level=None)
        self.conn.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, *args):
        self.conn.execute("ROLLBACK")
        self.conn.close()


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
def test_newer_schema_version_takes_no_write_lock():
    sqlite3_lci_db.execute_sql(f"PRAGMA main.user_version = {SCHEMA_VERSION + 1}")
    with _WriteLock():
        with pytest.warns(UserWarning, match="newer version of bw2data"):
            assert migrate(sqlite3_lci_db.db, projects.dir) == []


@bw2test
def test_up_to_date_file_takes_no_write_lock():
    with _WriteLock():
        assert migrate(sqlite3_lci_db.db, projects.dir) == []


@bw2test
def test_migration_is_postponed_when_file_is_locked():
    _write_legacy_pickle(projects.dir, {"from_pickle": 1})
    _reset_schema_version()
    with _WriteLock():
        with pytest.warns(UserWarning, match="postponed"):
            assert migrate(sqlite3_lci_db.db, projects.dir) == []
    assert schema_version(sqlite3_lci_db.db) == 0
    assert "from_pickle" not in _stored()

    # Tried again on the next open
    assert migrate(sqlite3_lci_db.db, projects.dir) == list(range(1, SCHEMA_VERSION + 1))
    assert _stored()["from_pickle"] == 1


@bw2test
def test_migration_is_postponed_when_file_cannot_be_read():
    _reset_schema_version()
    sqlite3_lci_db.execute_sql("PRAGMA busy_timeout = 0")
    # An exclusive lock, as held by another process while it commits: no reading either
    conn = sqlite3.connect(sqlite3_lci_db.db.database, isolation_level=None)
    conn.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.warns(UserWarning, match="Can't read the schema version") as record:
            assert migrate(sqlite3_lci_db.db, projects.dir) == []
        assert record[0].filename == __file__
    finally:
        conn.execute("ROLLBACK")
        conn.close()

    # Tried again on the next open
    assert migrate(sqlite3_lci_db.db, projects.dir) == list(range(1, SCHEMA_VERSION + 1))


@bw2test
def test_migration_is_postponed_when_file_is_read_only():
    _reset_schema_version()
    db = BWSqliteDatabase(_sqlite_uri(sqlite3_lci_db.db.database, mode="ro"))
    try:
        with pytest.warns(UserWarning, match="postponed"):
            assert migrate(db, projects.dir) == []
        assert schema_version(db) == 0
    finally:
        db.close()


@bw2test
def test_other_errors_in_migration_are_not_postponed(monkeypatch):
    def broken(db, path):
        db.execute_sql("INSERT INTO main.no_such_table VALUES (1)")

    monkeypatch.setattr(
        "bw2data.backends.migrations.MIGRATIONS", (Migration(1, "broken", broken),)
    )
    _reset_schema_version()
    with pytest.raises(Exception, match="no_such_table"):
        migrate(sqlite3_lci_db.db, projects.dir)
    assert schema_version(sqlite3_lci_db.db) == 0


@bw2test
def test_failed_project_switch_stays_on_previous_project(monkeypatch):
    preferences["here"] = 1
    before = projects.current
    lci_path = sqlite3_lci_db.db.database
    parameters_path = parameters.db.db.database

    def boom(db):
        raise RuntimeError("can't migrate")

    monkeypatch.setattr(sqlite3_lci_db, "_on_open", boom)
    with pytest.raises(RuntimeError, match="can't migrate"):
        projects.set_current("unreachable")
    monkeypatch.undo()

    assert projects.current == before
    assert projects.dataset.name == before
    assert sqlite3_lci_db.db.database == lci_path
    assert parameters.db.db.database == parameters_path
    # Models and the database object point at the same, open file
    assert Preference._meta.database is sqlite3_lci_db.db
    assert not sqlite3_lci_db.db.is_closed()
    assert _stored()["here"] == 1
    preferences["still works"] = 2
    assert _stored()["still works"] == 2

    projects.set_current("unreachable")
    assert projects.current == "unreachable"
    assert "here" not in preferences


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
    # Preferences set before a postponed migration ran win; the rest comes from the pickle
    assert _stored()["kept"] == "sqlite"
    assert _stored()["other"] == 1


@bw2test
def test_preferences_migration_has_no_size_limit():
    legacy = {f"key {i}": i for i in range(2500)}
    _write_legacy_pickle(projects.dir, legacy)
    _reset_schema_version()
    migrate(sqlite3_lci_db.db, projects.dir)
    assert _stored() == legacy


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
def test_reading_does_not_write():
    # Also not for the defaults: reading must work while the file is locked or read-only
    with _WriteLock():
        preferences.load()
        assert preferences["use_cache"] is True
        assert preferences.get("output_dir") is None
    assert _stored() == {}


@bw2test
def test_default_is_stored_once_changed():
    preferences["other"] = 1
    assert "use_cache" not in _stored()
    preferences["use_cache"] = False
    assert _stored()["use_cache"] is False
    preferences.load()
    assert preferences["use_cache"] is False


@bw2test
def test_changes_of_another_process_are_kept():
    preferences["updates"] = {"a": False}
    preferences["removed elsewhere"] = 1

    # Another process changes one preference, adds one and removes one
    Preference.replace(key="updates", value={"a": True}).execute()
    Preference.replace(key="added elsewhere", value=1).execute()
    Preference.delete().where(Preference.key == "removed elsewhere").execute()

    preferences["output_dir"] = "/somewhere"
    preferences.flush()
    assert _stored() == {
        "updates": {"a": True},
        "added elsewhere": 1,
        "output_dir": "/somewhere",
    }

    preferences.load()
    assert preferences["updates"] == {"a": True}
    assert "removed elsewhere" not in preferences


@bw2test
def test_failed_save_changes_nothing():
    preferences["x"] = 1
    with pytest.raises(Exception):
        preferences["f"] = lambda: 0
    assert "f" not in preferences

    with _WriteLock():
        with pytest.raises(Exception, match="locked"):
            preferences["x"] = 2
        with pytest.raises(Exception, match="locked"):
            del preferences["x"]
    assert preferences["x"] == 1
    assert _stored()["x"] == 1

    # Later saves are not affected
    preferences["y"] = 3
    assert _stored() == {"x": 1, "y": 3}


@bw2test
def test_data_can_be_changed_in_place_but_not_replaced():
    preferences.data["x"] = 1
    preferences.flush()
    assert _stored()["x"] == 1

    del preferences.data["x"]
    preferences.flush()
    assert "x" not in _stored()

    preferences.data[("a", "b")] = 1
    with pytest.raises(TypeError):
        preferences.flush()
    del preferences.data[("a", "b")]

    with pytest.raises(AttributeError):
        preferences.data = {}


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
