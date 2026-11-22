"""``methods``, ``weightings`` and ``normalizations`` stored in SQLite, and schema migration 4
which moves the legacy JSON files into it."""

import json
import sqlite3

import pytest

from bw2data import Method, Normalization, Weighting, methods, normalizations, projects, weightings
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import SCHEMA_VERSION, migrate, schema_version
from bw2data.backends.schema import MethodRecord, NormalizationRecord, WeightingRecord
from bw2data.errors import UnknownObject
from bw2data.tests import bw2test

NAME = ("ReCiPe", "climate change", "GWP100")


# ----------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------


@bw2test
def test_registries_start_empty():
    for registry in (methods, weightings, normalizations):
        assert len(registry) == 0
        assert list(registry) == []
        assert registry.list == []
        assert registry.random() is None
        assert registry.data == {}


@bw2test
def test_set_get_and_delete():
    methods[NAME] = {"unit": "kg CO2-eq", "abbreviation": "abc", "num_cfs": 3}
    assert NAME in methods
    assert list(NAME) in methods
    assert dict(methods[NAME]) == {"unit": "kg CO2-eq", "abbreviation": "abc", "num_cfs": 3}
    assert dict(methods[list(NAME)]) == dict(methods[NAME])
    assert methods.list == [NAME]
    assert methods.random() == NAME

    del methods[list(NAME)]
    assert NAME not in methods
    with pytest.raises(KeyError):
        methods[NAME]
    with pytest.raises(KeyError):
        del methods[NAME]


@bw2test
def test_names_must_be_non_empty_tuples():
    for name in ("foo", (), [], 1, None):
        with pytest.raises(TypeError, match="non-empty tuples"):
            methods[name] = {}
        assert name not in methods
        with pytest.raises(KeyError):
            methods[name]


@bw2test
def test_kinds_are_separate_tables():
    methods[("a",)] = {"unit": "m"}
    weightings[("a",)] = {"unit": "w"}
    normalizations[("b",)] = {"unit": "n"}
    assert methods[("a",)]["unit"] == "m"
    assert weightings[("a",)]["unit"] == "w"
    assert ("b",) not in methods
    assert MethodRecord.select().count() == 1
    assert WeightingRecord.select().count() == 1
    assert NormalizationRecord.select().count() == 1


@bw2test
def test_known_fields_have_columns_and_other_keys_go_to_extra():
    metadata = {
        "abbreviation": "rcc.123",
        "num_cfs": 2,
        "geocollections": ["world"],
        "unit": "kg",
        "description": "ü",
        "nested": {"a": [1, None]},
    }
    methods[NAME] = metadata
    row = MethodRecord.get_by_id(MethodRecord.get().id)
    assert row.name == NAME
    assert row.abbreviation == "rcc.123"
    assert row.num_cfs == 2
    assert row.geocollections == ["world"]
    assert row.extra == {"unit": "kg", "description": "ü", "nested": {"a": [1, None]}}
    assert dict(methods[NAME]) == metadata

    # Weightings and normalizations only have the ``abbreviation`` column
    weightings[NAME] = {"abbreviation": "x", "num_cfs": 1}
    assert WeightingRecord.get().extra == {"num_cfs": 1}


@bw2test
def test_names_are_stored_as_compact_json():
    methods[NAME] = {}
    stored = sqlite3_lci_db.execute_sql('SELECT name FROM "method"').fetchone()[0]
    assert stored == '["ReCiPe", "climate change", "GWP100"]'


@bw2test
def test_iteration_follows_insertion_order():
    names = [("z",), ("a",), ("m", "n")]
    for name in names:
        methods[name] = {}
    assert list(methods) == names
    assert methods.list == sorted(names)
    assert list(methods.data) == names


@bw2test
def test_entries_can_be_deleted_while_iterating():
    for name in [("a",), ("b",), ("c",)]:
        methods[name] = {}
    for name in methods:
        del methods[name]
    assert len(methods) == 0


@bw2test
def test_replacing_entry_keeps_id_and_clears_missing_keys():
    methods[NAME] = {"abbreviation": "a", "num_cfs": 1, "unit": "kg"}
    id_ = MethodRecord.get().id
    methods[NAME] = {"description": "new"}
    assert MethodRecord.get().id == id_
    assert dict(methods[NAME]) == {"description": "new"}


@bw2test
def test_ids_are_snowflake_ids():
    methods[("a",)] = {}
    methods[("b",)] = {}
    first, second = [row.id for row in MethodRecord.select().order_by(MethodRecord.id)]
    assert first > 2**40
    assert second > first


@bw2test
def test_changes_are_pending_until_save():
    methods[NAME] = {"unit": "kg"}
    methods[NAME]["unit"] = "g"
    methods[NAME]["num_cfs"] = 5
    # Same proxy for the same name, so pending changes are visible
    assert methods[NAME]["unit"] == "g"
    assert MethodRecord.get().extra == {"unit": "kg"}
    assert MethodRecord.get().num_cfs is None

    methods[NAME].save()
    assert MethodRecord.get().extra == {"unit": "g"}
    assert MethodRecord.get().num_cfs == 5


@bw2test
def test_data_store_metadata_shares_pending_changes():
    method = Method(NAME)
    method.register(unit="kg")
    method.metadata["unit"] = "g"
    assert methods[NAME]["unit"] == "g"
    method.metadata.save()
    methods.__init__()  # Forget the proxies, as on a project switch
    assert methods[NAME]["unit"] == "g"


@bw2test
def test_flush_is_deprecated_and_saves_pending_changes():
    weightings[NAME] = {"unit": "kg"}
    weightings[NAME]["unit"] = "g"
    with pytest.warns(DeprecationWarning, match=r"`weightings.flush\(\)` is deprecated"):
        weightings.flush()
    assert WeightingRecord.get().extra == {"unit": "g"}


@bw2test
def test_save_after_delete_raises():
    methods[NAME] = {"unit": "kg"}
    proxy = methods[NAME]
    proxy["unit"] = "g"
    MethodRecord.delete().execute()
    with pytest.raises(UnknownObject, match="Method"):
        proxy.save()


@bw2test
def test_str():
    assert str(methods) == "Methods dictionary with 0 objects"
    methods[("a", "b")] = {}
    assert str(normalizations) == "NormalizationMeta dictionary with 0 objects"
    assert str(methods) == "Methods dictionary with 1 object(s):\n\t('a', 'b')"


@bw2test
def test_register_write_and_copy():
    from bw2data import Database

    Database("bio").write({("bio", "co2"): {"name": "CO2", "type": "emission"}})
    method = Method(NAME)
    method.register(unit="kg")
    method.write([(("bio", "co2"), 1.0), (("bio", "co2"), 2.0, ("world", "CH"))])
    methods.__init__()
    metadata = dict(methods[NAME])
    assert metadata["num_cfs"] == 2
    assert metadata["geocollections"] == ["world"]
    assert metadata["unit"] == "kg"
    assert metadata["abbreviation"]

    copy = method.copy()
    assert copy.metadata["unit"] == "kg"
    assert copy.metadata["abbreviation"] != metadata["abbreviation"]
    assert copy.load() == method.load()

    weighting = Weighting(("w",))
    weighting.write([42])
    normalization = Normalization(("n",))
    normalization.write([(("bio", "co2"), 3)])
    assert weightings[("w",)]["abbreviation"]
    assert normalizations[("n",)]["abbreviation"]


@bw2test
def test_registries_follow_project_switch():
    start = projects.current
    methods[NAME] = {"unit": "kg"}
    projects.set_current("other")
    assert NAME not in methods
    methods[("other",)] = {}
    projects.set_current(start)
    assert methods.list == [NAME]


# ----------------------------------------------------------------------
# Migration 4
# ----------------------------------------------------------------------

LEGACY = {
    "methods.json": [
        [
            ["ReCiPe", "climate change", "GWP100"],
            {
                "unit": "kg CO2-eq",
                "abbreviation": "recipecc.1234",
                "num_cfs": 2,
                "geocollections": ["world"],
                "description": "ü",
            },
        ],
        [["IPCC"], {"abbreviation": "ipcc.5678", "num_cfs": "many"}],
    ],
    "weightings.json": [[["w"], {"abbreviation": "w.1", "unit": ""}]],
    "normalizations.json": [[["n", "1"], {"abbreviation": "n1.2"}]],
}


def _write_legacy_files(project_dir, files=LEGACY):
    for filename, data in files.items():
        with open(project_dir / filename, "w", encoding="utf-8") as f:
            json.dump(data, f)


def _migrate_from_version_3():
    """Run only the steps after migration 3, as for a project last opened before step 4."""
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 3")
    return migrate(sqlite3_lci_db.db, projects.dir)


@bw2test
def test_migrates_files_and_keeps_backups():
    _write_legacy_files(projects.dir)
    assert _migrate_from_version_3() == list(range(4, SCHEMA_VERSION + 1))

    assert methods.list == [("IPCC",), ("ReCiPe", "climate change", "GWP100")]
    assert dict(methods[("ReCiPe", "climate change", "GWP100")]) == LEGACY["methods.json"][0][1]
    # A ``num_cfs`` which isn't an integer is kept as it is, as peewee would store it
    assert dict(methods[("IPCC",)]) == {"abbreviation": "ipcc.5678", "num_cfs": "many"}
    assert dict(weightings[("w",)]) == {"abbreviation": "w.1", "unit": ""}
    assert dict(normalizations[("n", "1")]) == {"abbreviation": "n1.2"}
    assert all(row.id > 2**40 for row in MethodRecord.select())
    for filename in LEGACY:
        assert (projects.dir / filename).is_file()


@bw2test
def test_migrated_rows_match_rows_written_by_peewee():
    """The raw SQL in the migration stores values exactly as the model fields do."""
    files = {
        "methods.json": [LEGACY["methods.json"][0]],
        "weightings.json": LEGACY["weightings.json"],
    }
    _write_legacy_files(projects.dir, files)
    _migrate_from_version_3()

    def stored(table):
        # Ids are new snowflake ids either way
        return list(sqlite3_lci_db.execute_sql(f'SELECT * FROM "{table}" ORDER BY id'))

    migrated = {table: [row[1:] for row in stored(table)] for table in ("method", "weighting")}
    MethodRecord.delete().execute()
    WeightingRecord.delete().execute()
    for (name, metadata), registry in (
        (files["methods.json"][0], methods),
        (files["weightings.json"][0], weightings),
    ):
        registry[tuple(name)] = metadata
    written = {table: [row[1:] for row in stored(table)] for table in ("method", "weighting")}
    assert migrated == written


@bw2test
def test_migrates_files_on_project_open():
    projects.set_current("legacy")
    legacy_dir = projects.dir
    projects.set_current("other")

    _write_legacy_files(legacy_dir)
    with sqlite3.connect(legacy_dir / "lci" / "databases.db") as conn:
        conn.execute("PRAGMA user_version = 3")
    conn.close()

    projects.set_current("legacy")
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert len(methods) == 2
    assert len(weightings) == 1
    assert len(normalizations) == 1


@bw2test
def test_migration_skips_tables_which_have_rows():
    weightings[("existing",)] = {}
    _write_legacy_files(projects.dir)
    _migrate_from_version_3()
    assert weightings.list == [("existing",)]
    assert len(methods) == 2


@bw2test
def test_migration_runs_only_once():
    _write_legacy_files(projects.dir)
    _migrate_from_version_3()
    MethodRecord.delete().execute()
    assert migrate(sqlite3_lci_db.db, projects.dir) == []
    assert len(methods) == 0


@bw2test
def test_later_entry_for_same_name_wins():
    _write_legacy_files(
        projects.dir, {"methods.json": [[["a"], {"unit": "1"}], [["a"], {"unit": "2"}]]}
    )
    _migrate_from_version_3()
    assert dict(methods[("a",)]) == {"unit": "2"}


@bw2test
def test_unreadable_file_is_skipped_with_warning():
    (projects.dir / "methods.json").write_text("[not json", encoding="utf-8")
    _write_legacy_files(projects.dir, {"weightings.json": LEGACY["weightings.json"]})
    with pytest.warns(UserWarning, match="Can't read"):
        _migrate_from_version_3()
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert len(methods) == 0
    assert len(weightings) == 1


@bw2test
def test_invalid_entries_are_skipped_with_warning():
    _write_legacy_files(
        projects.dir,
        {
            "methods.json": [
                [["good"], {"unit": "kg"}],
                ["not a list", {}],
                [[], {}],
                [["bad metadata"], ["x"]],
                "junk",
            ]
        },
    )
    with pytest.warns(UserWarning, match="can't be migrated"):
        _migrate_from_version_3()
    assert methods.list == [("good",)]
