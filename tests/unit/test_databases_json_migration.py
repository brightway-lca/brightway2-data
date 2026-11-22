"""Schema migration 3: the legacy per-project ``databases.json`` moves into ``DatabaseRecord``."""

import json
import sqlite3

import pytest

from bw2data import databases
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import SCHEMA_VERSION, migrate, schema_version
from bw2data.backends.schema import DatabaseRecord
from bw2data.project import projects
from bw2data.tests import bw2test

LEGACY = {
    "biosphere": {"backend": "sqlite", "depends": [], "number": 3, "searchable": True},
    "food": {
        "backend": "sqlite",
        "depends": ["biosphere"],
        "geocollections": ["world"],
        "format": "Custom importer",
        "dirty": False,
        "modified": "2024-01-01T12:00:00",
        "version": 2,
        "nested": {"a": [1, "ü"]},
    },
}


def _write_legacy_json(project_dir, data=LEGACY):
    with open(project_dir / "databases.json", "w", encoding="utf-8") as f:
        json.dump(data, f)


def _migrate_from_version_2():
    """Run only the steps after migration 2, as for a project last opened before step 3."""
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 2")
    return migrate(sqlite3_lci_db.db, projects.dir)


@bw2test
def test_migrates_json_and_keeps_backup():
    _write_legacy_json(projects.dir)
    assert _migrate_from_version_2() == list(range(3, SCHEMA_VERSION + 1))

    assert DatabaseRecord.select().count() == 2
    assert dict(databases["biosphere"]) == LEGACY["biosphere"]
    assert dict(databases["food"]) == LEGACY["food"]
    # Unknown keys land in `extra`, known ones in their own columns
    row = DatabaseRecord.get(DatabaseRecord.name == "food")
    assert row.extra == {"format": "Custom importer", "nested": {"a": [1, "ü"]}}
    assert row.version == 2
    assert row.dirty is False
    assert row.depends == ["biosphere"]
    assert row.number is None
    assert (projects.dir / "databases.json").exists()


@bw2test
def test_migrated_rows_match_rows_written_by_peewee():
    """The raw SQL in the migration stores values exactly as the model fields do."""
    _write_legacy_json(projects.dir)
    _migrate_from_version_2()
    migrated = list(sqlite3_lci_db.execute_sql("SELECT * FROM databaserecord ORDER BY name"))

    DatabaseRecord.delete().execute()
    for name, metadata in LEGACY.items():
        databases[name] = metadata
    written = list(sqlite3_lci_db.execute_sql("SELECT * FROM databaserecord ORDER BY name"))
    assert migrated == written


@bw2test
def test_migrates_json_on_project_open():
    projects.set_current("legacy")
    legacy_dir = projects.dir
    projects.set_current("other")

    _write_legacy_json(legacy_dir)
    with sqlite3.connect(legacy_dir / "lci" / "databases.db") as conn:
        conn.execute("PRAGMA user_version = 2")
    conn.close()

    projects.set_current("legacy")
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert sorted(databases) == ["biosphere", "food"]


@bw2test
def test_migration_keeps_databases_already_in_table():
    """As after a postponed migration: databases registered in the meantime are kept as they
    are, and the legacy ones are added."""
    databases["existing"] = {"backend": "sqlite", "depends": []}
    databases["food"] = {"backend": "iotable", "depends": []}
    _write_legacy_json(projects.dir)
    _migrate_from_version_2()

    assert sorted(databases) == ["biosphere", "existing", "food"]
    assert dict(databases["food"]) == {"backend": "iotable", "depends": []}
    assert dict(databases["biosphere"]) == LEGACY["biosphere"]


@bw2test
def test_migration_runs_only_once():
    _write_legacy_json(projects.dir)
    _migrate_from_version_2()
    DatabaseRecord.delete().execute()

    # Already at the current version: the file kept as a backup is not read again
    assert migrate(sqlite3_lci_db.db, projects.dir) == []
    assert len(databases) == 0


@bw2test
def test_unreadable_json_is_skipped_with_warning():
    (projects.dir / "databases.json").write_text("{not json", encoding="utf-8")
    with pytest.warns(UserWarning, match="Can't read"):
        _migrate_from_version_2()
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert len(databases) == 0


@bw2test
def test_entries_which_are_not_dicts_are_skipped_with_warning():
    _write_legacy_json(projects.dir, {"good": {"backend": "sqlite"}, "bad": ["sqlite"]})
    with pytest.warns(UserWarning, match="not dictionaries"):
        _migrate_from_version_2()
    assert sorted(databases) == ["good"]


@bw2test
def test_values_of_the_wrong_type_are_converted():
    """Values which SQLite can't store in a scalar column must not block opening the project."""
    _write_legacy_json(
        projects.dir,
        {
            "odd": {
                "backend": ["sqlite"],
                "version": [1, 2],
                "number": "7",
                "modified": {"x": 1},
                "dirty": "yes",
                "searchable": [],
            },
            "good": {"backend": "sqlite"},
        },
    )
    _migrate_from_version_2()

    assert sorted(databases) == ["good", "odd"]
    row = sqlite3_lci_db.execute_sql(
        "SELECT backend, version, number, modified, dirty, searchable FROM databaserecord "
        "WHERE name = 'odd'"
    ).fetchone()
    assert row == ("['sqlite']", "[\n  1,\n  2\n]", 7, "{'x': 1}", 1, 0)


@bw2test
def test_entries_which_cant_be_stored_are_skipped_with_warning():
    _write_legacy_json(projects.dir, {"good": {"backend": "sqlite"}, "bad": {"version": 2**70}})
    with pytest.warns(UserWarning, match="can't be stored"):
        _migrate_from_version_2()
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert sorted(databases) == ["good"]
