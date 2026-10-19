"""One-time migration of the legacy per-project ``databases.json`` into ``DatabaseRecord``."""

import json

import pytest

from bw2data import databases
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
        "version": 2,
    },
}


def _write_legacy_json():
    with open(projects.dir / "databases.json", "w", encoding="utf-8") as f:
        json.dump(LEGACY, f)
    # Same as a project switch: forget that this project was already checked.
    databases.__init__()


@bw2test
def test_migrates_json_on_first_access_and_keeps_backup():
    _write_legacy_json()
    assert DatabaseRecord.select().count() == 0

    with pytest.warns(UserWarning, match="Migrating"):
        assert "food" in databases

    assert DatabaseRecord.select().count() == 2
    assert dict(databases["biosphere"]) == LEGACY["biosphere"]
    assert dict(databases["food"]) == LEGACY["food"]
    # Unknown keys land in `extra`, known ones in their own columns
    row = DatabaseRecord.get(DatabaseRecord.name == "food")
    assert row.extra == {"format": "Custom importer"}
    assert row.version == 2
    assert row.depends == ["biosphere"]
    assert (projects.dir / "databases.json").exists()


@bw2test
def test_migration_skipped_when_table_already_populated():
    databases["existing"] = {"backend": "sqlite", "depends": []}
    _write_legacy_json()

    assert sorted(databases) == ["existing"]
    assert "food" not in databases


@bw2test
def test_migration_runs_again_after_project_switch():
    _write_legacy_json()
    assert "food" in databases

    projects.set_current("other")
    assert "food" not in databases
    _write_legacy_json()
    assert "food" in databases
