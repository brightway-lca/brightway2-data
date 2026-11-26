"""Attaching read-only LCI extension files to a project's ``lci/databases.db`` (extensions PR 3):
``SubstitutableDatabase(extensions=...)``, ``change_path(..., extensions=...)`` and the checks
of ``bw2data.backends.extensions.check_extensions``.

Extension files are made here by copying a project's file and keeping some databases, with the
two extension tables added by hand; building them properly is ``export_extension`` (PR 6)."""

import json
import sqlite3
import threading

import pytest

from bw2data import Database, config, databases, get_node, projects
from bw2data.backends import extensions as ext
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.extensions import (
    ExtensionSpec,
    attach_hook,
    check_extensions,
    database_fingerprint,
    view_name,
)
from bw2data.backends.migrations import MIN_EXTENSION_SCHEMA_VERSION, SCHEMA_VERSION
from bw2data.backends.schema import ActivityDataset, DatabaseRecord, ExchangeDataset
from bw2data.errors import ExtensionDependencyError, ExtensionError
from bw2data.search.indices import index_table
from bw2data.tests import bw2test

BIO = {("bio", str(i)): {"name": f"flow {i}", "type": "emission", "unit": "kg"} for i in range(3)}
INV = {
    ("inv", "a"): {
        "name": "a",
        "unit": "kg",
        "exchanges": [
            {"input": ("inv", "a"), "amount": 1, "type": "production"},
            {"input": ("bio", "0"), "amount": 2, "type": "biosphere"},
        ],
    },
    ("inv", "b"): {
        "name": "b",
        "unit": "kg",
        "exchanges": [
            {"input": ("inv", "b"), "amount": 1, "type": "production"},
            {"input": ("inv", "a"), "amount": 0.5, "type": "technosphere"},
        ],
    },
}


def lci_path():
    return projects.dir / "lci" / "databases.db"


def export(path, names, extension_id=None, requires=(), version=SCHEMA_VERSION):
    """Copy the current project's file to ``path``, keeping only the databases ``names``, and
    add the extension tables. ``requires``: ``(database, extension id, fingerprint)``.
    Returns the fingerprints of ``names``."""
    sqlite3_lci_db.execute_sql("VACUUM INTO ?", (str(path),))
    conn = sqlite3.connect(path)
    marks = ", ".join("?" * len(names))
    conn.execute(f"DELETE FROM activitydataset WHERE database NOT IN ({marks})", names)
    conn.execute(f"DELETE FROM exchangedataset WHERE output_database NOT IN ({marks})", names)
    conn.execute(f"DELETE FROM databaserecord WHERE name NOT IN ({marks})", names)
    # The search indexes of the databases which are kept stay, with the data
    kept = {index_table(name) for name in names}
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'searchindex%' "
        "AND sql LIKE 'CREATE VIRTUAL TABLE%'"
    ).fetchall()
    for (table,) in tables:
        if table not in kept:
            conn.execute(f'DROP TABLE "{table}"')
    fingerprints = {name: database_fingerprint(conn, "main", name) for name in names}
    conn.execute("CREATE TABLE extension_info (key TEXT PRIMARY KEY, value TEXT)")
    conn.executemany(
        "INSERT INTO extension_info VALUES (?, ?)",
        [
            ("id", extension_id or f"{path.stem}-id"),
            ("version", "1.0"),
            ("fingerprints", json.dumps(fingerprints)),
        ],
    )
    if requires:
        conn.execute(
            "CREATE TABLE extension_requires "
            "(database TEXT PRIMARY KEY, extension_id TEXT, fingerprint TEXT)"
        )
        conn.executemany("INSERT INTO extension_requires VALUES (?, ?, ?)", requires)
    conn.execute(f"PRAGMA user_version = {int(version)}")
    conn.commit()
    conn.close()
    return fingerprints


def edit(path, *statements):
    conn = sqlite3.connect(path)
    for statement in statements:
        conn.execute(*statement) if isinstance(statement, tuple) else conn.execute(statement)
    conn.commit()
    conn.close()


def attach(*specs):
    sqlite3_lci_db.change_path(lci_path(), extensions=list(specs))


@bw2test
def _temporary_directory():
    """Move bw2data to a new temporary directory, as ``bw2test`` does for a test."""


@pytest.fixture
def files(tmp_path):
    """``bio.db`` (database ``bio``) and ``inv.db`` (``inv``, requires ``bio``), built in a
    project ``source``; then a new, empty project ``target`` is current.

    Tests which use this fixture must not use ``bw2test`` as well: fixtures run before the
    decorator, which would then move to another temporary directory, leaving the test with
    other projects than the ones made here."""
    _temporary_directory()
    projects.set_current("source")
    Database("bio").write(BIO)
    Database("inv").write(INV)
    bio, inv = tmp_path / "bio.db", tmp_path / "inv.db"
    fingerprints = export(bio, ["bio"])
    export(inv, ["inv"], requires=[("bio", "bio-id", fingerprints["bio"])])
    projects.set_current("target")
    yield bio, inv
    # The models must point at their tables again for the tests which come next
    sqlite3_lci_db.change_path(lci_path())


def specs(files):
    bio, inv = files
    return ExtensionSpec("bio", bio), ExtensionSpec("inv", inv)


def main_objects():
    """Names of the tables, views and triggers in the project's file."""
    conn = sqlite3.connect(lci_path())
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master")}
    conn.close()
    return names


# Reading and writing through the overlay


def test_attached_databases_are_read(files):
    attach(*specs(files))
    assert sorted(databases) == ["bio", "inv"]
    assert len(Database("bio")) == 3
    node = get_node(database="inv", code="b")
    assert {(e.input["name"], e["amount"]) for e in node.exchanges()} == {("b", 1), ("a", 0.5)}
    assert [e.output.key for e in get_node(database="inv", code="a").upstream()] == [("inv", "b")]
    assert [e.output.key for e in get_node(database="bio", code="0").upstream(kinds=None)] == [
        ("inv", "a")
    ]
    assert Database("inv").load()[("inv", "a")]["exchanges"][1]["input"] == ("bio", "0")


def test_introspection(files):
    bio, inv = files
    attach(*specs(files))
    attached = sqlite3_lci_db.extensions
    assert [e.name for e in attached] == ["bio", "inv"]
    assert [e.schema for e in attached] == ["ext_1", "ext_2"]
    assert attached[0].id == "bio-id"
    assert attached[0].version == "1.0"
    assert attached[0].path == bio
    assert attached[0].databases == ("bio",)
    assert attached[1].requirements == (("bio", "bio-id", attached[0].fingerprints["bio"]),)
    assert {k: v.name for k, v in sqlite3_lci_db.extension_databases().items()} == {
        "bio": "bio",
        "inv": "inv",
    }
    assert str(attached[0]) == f"extension 'bio' ({bio})"


def test_models_point_at_views_only_while_attached(files):
    attach(*specs(files))
    assert ActivityDataset._meta.table_name == "lci_activitydataset"
    assert ExchangeDataset._meta.table_name == "lci_exchangedataset"
    assert DatabaseRecord._meta.table_name == "lci_databaserecord"
    assert view_name(ActivityDataset) == "lci_activitydataset"
    sqlite3_lci_db.change_path(lci_path())
    assert ActivityDataset._meta.table_name == "activitydataset"
    assert DatabaseRecord._meta.table_name == "databaserecord"
    assert sqlite3_lci_db.extensions == ()
    assert databases.__len__() == 0


def test_editable_database_links_to_extension(files):
    attach(*specs(files))
    data = {
        ("mine", str(i)): {
            "name": f"mine {i}",
            "exchanges": [
                {"input": ("mine", str(i)), "amount": 1, "type": "production"},
                {"input": ("inv", "a"), "amount": i, "type": "technosphere"},
                {"input": ("bio", "1"), "amount": 1, "type": "biosphere"},
            ],
        }
        for i in range(120)  # at least 100: the indices are dropped and created again
    }
    Database("mine").write(data)
    assert len(Database("mine")) == 120
    assert sorted(databases) == ["bio", "inv", "mine"]
    node = get_node(database="mine", code="5")
    assert {e.input.key for e in node.exchanges()} == {("mine", "5"), ("inv", "a"), ("bio", "1")}
    assert len(get_node(database="inv", code="a").upstream()) == 121
    # Rows were written to the project's file only
    conn = sqlite3.connect(lci_path())
    assert conn.execute("SELECT count(*) FROM activitydataset").fetchone() == (120,)
    assert conn.execute("SELECT DISTINCT output_database FROM exchangedataset").fetchall() == [
        ("mine",)
    ]
    assert conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE name = 'activitydataset_key'"
    ).fetchone() == (1,)
    conn.close()
    Database("mine").process()


def test_single_saves_and_metadata_saves(files):
    attach(*specs(files))
    Database("mine").register()
    node = Database("mine").new_node(code="x", name="x", unit="kg")
    node.save()
    node.new_edge(input=("inv", "a"), amount=2, type="technosphere").save()
    assert len(list(get_node(database="mine", code="x").technosphere())) == 1
    # Through the view, an update reports no changed rows; the save must still work
    databases["mine"]["comment"] = "mine"
    databases["mine"].save()
    assert databases["mine"]["comment"] == "mine"
    # A project-local field of an extension database is stored in the project
    databases["bio"]["dirty"] = True
    databases["bio"].save()
    assert databases["bio"]["dirty"] is True
    conn = sqlite3.connect(lci_path())
    assert conn.execute("SELECT name, dirty FROM extensiondatabasestate").fetchall() == [("bio", 1)]
    conn.close()


def test_extension_data_is_read_only(files):
    from peewee import IntegrityError

    from bw2data.errors import ReadOnlyExtensionError

    attach(*specs(files))
    # Refused by the triggers; the error is also an ``IntegrityError``
    with pytest.raises(ReadOnlyExtensionError, match="bw2data-extension") as caught:
        ActivityDataset.delete().where(ActivityDataset.database == "bio").execute()
    assert isinstance(caught.value, IntegrityError)
    # Refused before any SQL runs
    with pytest.raises(IntegrityError, match="only its keys .* can be changed, not comment"):
        databases["bio"]["comment"] = "changed"
        databases["bio"].save()
    assert "comment" not in databases["bio"]
    assert len(Database("bio")) == 3


def test_switching_to_a_project_without_extensions_leaves_its_file_clean(files):
    assert projects.current == "target"
    attach(*specs(files))
    Database("mine").write({("mine", "1"): {"name": "x"}})
    projects.set_current("clean")
    assert sqlite3_lci_db.extensions == ()
    assert ActivityDataset._meta.table_name == "activitydataset"
    assert not any(name.startswith("lci_") for name in main_objects())
    Database("clean").write({("clean", "1"): {"name": "x"}})
    assert sorted(databases) == ["clean"]
    # Neither in the file which had the extensions. They are not attached again: the project
    # doesn't store them yet.
    projects.set_current("target")
    assert not any(name.startswith("lci_") for name in main_objects())
    assert sorted(databases) == ["mine"]


def test_tables_created_late_get_their_own_names(files):
    # As when the file was locked while it was opened: the tables are created before a later
    # statement, while the models point at the views
    attach(*specs(files))
    conn = sqlite3.connect(lci_path())
    conn.execute("DROP TABLE preference")
    conn.commit()
    conn.close()
    assert sqlite3_lci_db._create_tables(sqlite3_lci_db.db) is None
    assert "preference" in main_objects()
    assert not any(name.startswith("lci_") for name in main_objects())
    assert ActivityDataset._meta.table_name == "lci_activitydataset"


def test_other_threads_see_the_overlay(files):
    attach(*specs(files))
    result = {}

    def count():
        try:
            result["count"] = ActivityDataset.select().count()
        finally:
            sqlite3_lci_db.db.close()

    thread = threading.Thread(target=count)
    thread.start()
    thread.join()
    assert result == {"count": 5}


def test_vacuum(files):
    attach(*specs(files))
    Database("mine").write({("mine", "1"): {"name": "x"}})
    del databases["mine"]
    previous = config.vacuum
    config.vacuum = True
    try:
        assert sqlite3_lci_db.vacuum()
    finally:
        config.vacuum = previous
    assert sorted(databases) == ["bio", "inv"]
    assert len(Database("inv")) == 2


@bw2test
def test_values_compressed_with_an_extension_dictionary(tmp_path):
    # At least 500 datasets: a zstd dictionary is trained and stored in the source file only
    projects.set_current("source")
    Database("big").write(
        {("big", str(i)): {"name": f"activity {i}", "comment": "same " * 20} for i in range(500)}
    )
    path = tmp_path / "big.db"
    export(path, ["big"])
    projects.set_current("target")
    try:
        attach(ExtensionSpec("big", path))
        conn = sqlite3.connect(lci_path())
        assert conn.execute("SELECT count(*) FROM zstddictionary").fetchone() == (0,)
        conn.close()
        assert get_node(database="big", code="7")["comment"] == "same " * 20
    finally:
        sqlite3_lci_db.change_path(lci_path())


def test_immutable(files):
    bio, inv = files
    attach(ExtensionSpec("bio", bio, immutable=True), ExtensionSpec("inv", inv))
    assert "immutable=1" in sqlite3_lci_db.extensions[0].uri
    assert len(Database("bio")) == 3


# Refusals


def test_missing_requirement(files):
    bio, inv = files
    with pytest.raises(ExtensionDependencyError, match="'bio'.*bio-id, which is not attached"):
        attach(ExtensionSpec("inv", inv))


def test_requirement_with_other_fingerprint(files, tmp_path):
    bio, inv = files
    other = tmp_path / "other-bio.db"
    projects.set_current("other source")
    Database("bio").write({**BIO, ("bio", "extra"): {"name": "extra", "type": "emission"}})
    export(other, ["bio"], extension_id="bio-id")
    projects.set_current("target")
    with pytest.raises(ExtensionDependencyError, match="has a different version of it"):
        attach(ExtensionSpec("bio", other), ExtensionSpec("inv", inv))


def test_requirement_provider_lacks_database(files, tmp_path):
    bio, inv = files
    edit(bio, "UPDATE extension_info SET value = 'other-id' WHERE key = 'id'")
    projects.set_current("source")
    Database("x").write({("x", "1"): {"name": "x"}})
    other = tmp_path / "x.db"
    export(other, ["x"], extension_id="bio-id")
    projects.set_current("target")
    with pytest.raises(ExtensionDependencyError, match="which doesn't have it"):
        attach(ExtensionSpec("bio", bio), ExtensionSpec("x", other), ExtensionSpec("inv", inv))


def test_requirement_met_by_main_only_with_opt_in(files):
    bio, inv = files
    Database("bio").write(BIO)
    with pytest.raises(ExtensionDependencyError, match="not attached"):
        attach(ExtensionSpec("inv", inv))
    attach(ExtensionSpec("inv", inv, allow_main_requirements=True))
    assert sorted(databases) == ["bio", "inv"]
    node = get_node(database="inv", code="a")
    assert [e.input["name"] for e in node.biosphere()] == ["flow 0"]


def test_requirement_met_by_main_checks_the_fingerprint(files):
    bio, inv = files
    Database("bio").write({("bio", "0"): {"name": "flow 0", "type": "emission"}})
    with pytest.raises(ExtensionDependencyError, match="in the project has a different"):
        attach(ExtensionSpec("inv", inv, allow_main_requirements=True))


def test_database_in_main_and_extension(files):
    Database("bio").write(BIO)
    with pytest.raises(ExtensionError, match="'bio' is in extension 'bio'.*also in the project"):
        attach(*specs(files))


def test_database_in_two_extensions(files, tmp_path):
    bio, inv = files
    copy = tmp_path / "copy.db"
    copy.write_bytes(bio.read_bytes())
    edit(copy, "UPDATE extension_info SET value = 'copy-id' WHERE key = 'id'")
    with pytest.raises(ExtensionError, match="'bio' is in extension 'bio'.*also in extension"):
        attach(ExtensionSpec("bio", bio), ExtensionSpec("copy", copy))


def test_rows_without_a_record(files):
    bio, inv = files
    edit(bio, "DELETE FROM databaserecord")
    with pytest.raises(ExtensionError, match="no database record"):
        attach(ExtensionSpec("bio", bio))


def test_same_ids_in_two_files(files, tmp_path):
    bio, inv = files
    renamed = tmp_path / "renamed.db"
    renamed.write_bytes(bio.read_bytes())
    edit(
        renamed,
        "UPDATE extension_info SET value = 'renamed-id' WHERE key = 'id'",
        "UPDATE activitydataset SET database = 'renamed'",
        "UPDATE databaserecord SET name = 'renamed'",
    )
    with pytest.raises(ExtensionError, match="3 activity ids are used in both extension 'bio'"):
        attach(ExtensionSpec("bio", bio), ExtensionSpec("renamed", renamed))


def test_same_extension_twice(files, tmp_path):
    bio, inv = files
    copy = tmp_path / "copy.db"
    copy.write_bytes(bio.read_bytes())
    with pytest.raises(ExtensionError, match="are the same extension"):
        attach(ExtensionSpec("bio", bio), ExtensionSpec("copy", copy))
    with pytest.raises(ExtensionError, match="are the same file"):
        attach(ExtensionSpec("bio", bio), ExtensionSpec("again", bio))
    with pytest.raises(ExtensionError, match="Two extensions are named 'bio'"):
        attach(ExtensionSpec("bio", bio), ExtensionSpec("bio", copy))


def test_different_dictionaries_with_the_same_id(files):
    bio, inv = files
    sqlite3_lci_db.execute_sql(
        "INSERT INTO zstddictionary (id, content, created, active) VALUES (7, x'01', '', 0)"
    )
    edit(bio, "INSERT INTO zstddictionary (id, content, created, active) VALUES (7, x'02', '', 0)")
    with pytest.raises(ExtensionError, match="different zstd dictionaries with the same id 7"):
        attach(ExtensionSpec("bio", bio))
    # The same dictionary in both files is fine
    edit(bio, "UPDATE zstddictionary SET content = x'01' WHERE id = 7")
    attach(ExtensionSpec("bio", bio))


@pytest.mark.parametrize(
    "version, message",
    [(MIN_EXTENSION_SCHEMA_VERSION - 1, "needs at least"), (SCHEMA_VERSION + 1, "newer")],
)
def test_schema_version(files, version, message):
    bio, inv = files
    edit(bio, f"PRAGMA user_version = {version}")
    with pytest.raises(ExtensionError, match=message):
        attach(ExtensionSpec("bio", bio))


def test_oldest_and_newest_schema_versions_are_accepted(files):
    bio, inv = files
    for version in (MIN_EXTENSION_SCHEMA_VERSION, SCHEMA_VERSION):
        edit(bio, f"PRAGMA user_version = {version}")
        attach(ExtensionSpec("bio", bio))
        assert len(Database("bio")) == 3


def test_not_an_extension(files, tmp_path):
    bio, inv = files
    with pytest.raises(ExtensionError, match="the file doesn't exist"):
        attach(ExtensionSpec("missing", tmp_path / "missing.db"))
    text = tmp_path / "text.db"
    text.write_text("not a database " * 100)
    with pytest.raises(ExtensionError, match="text.db"):
        attach(ExtensionSpec("text", text))
    edit(bio, "DROP TABLE extension_info")
    with pytest.raises(ExtensionError, match="not a bw2data extension"):
        attach(ExtensionSpec("bio", bio))


def test_extension_info_needs_an_id_and_valid_fingerprints(files):
    bio, inv = files
    edit(bio, "UPDATE extension_info SET value = '[1]' WHERE key = 'fingerprints'")
    with pytest.raises(ExtensionError, match="not a JSON object"):
        attach(ExtensionSpec("bio", bio))
    edit(bio, "DELETE FROM extension_info WHERE key = 'id'")
    with pytest.raises(ExtensionError, match="has no 'id'"):
        attach(ExtensionSpec("bio", bio))


def test_too_many_extensions(files, monkeypatch):
    monkeypatch.setattr(ext, "_attach_limit", lambda connection: 1)
    with pytest.raises(ExtensionError, match="at most 1 files"):
        attach(*specs(files))


def test_specs_must_be_extension_specs(files):
    bio, inv = files
    with pytest.raises(TypeError):
        attach(bio)


def test_failed_attach_keeps_the_previous_file_and_extensions(files):
    bio, inv = files
    attach(*specs(files))
    previous = sqlite3_lci_db.extensions
    projects_dir = lci_path()
    with pytest.raises(ExtensionDependencyError):
        sqlite3_lci_db.change_path(projects_dir, extensions=[ExtensionSpec("inv", inv)])
    assert sqlite3_lci_db.extensions == previous
    assert ActivityDataset._meta.table_name == "lci_activitydataset"
    assert sorted(databases) == ["bio", "inv"]
    assert len(Database("bio")) == 3


def test_check_extensions_leaves_the_files_alone(files):
    bio, inv = files
    before = bio.read_bytes(), lci_path().read_bytes()
    attached = check_extensions(lci_path(), specs(files))
    assert [a.schema for a in attached] == ["ext_1", "ext_2"]
    assert (bio.read_bytes(), lci_path().read_bytes()) == before
    assert callable(attach_hook(attached))


def test_fingerprint_covers_codes_and_types_only(files):
    conn = sqlite3.connect(files[0])
    first = database_fingerprint(conn, "main", "bio")
    conn.execute("UPDATE activitydataset SET name = 'renamed'")
    assert database_fingerprint(conn, "main", "bio") == first
    conn.execute("UPDATE activitydataset SET type = 'process' WHERE code = '0'")
    assert database_fingerprint(conn, "main", "bio") != first
    conn.close()
    assert first.startswith("sha256:")


def test_extension_table_with_other_columns(files):
    bio, inv = files
    edit(
        bio,
        "DROP TABLE extension_info",
        "CREATE TABLE extension_info (name TEXT, data TEXT)",
    )
    with pytest.raises(ExtensionError, match="Can't attach extension 'bio'.*no such column"):
        attach(ExtensionSpec("bio", bio))


def test_overlaid_table_with_missing_column(files):
    bio, inv = files
    edit(bio, "ALTER TABLE activitydataset DROP COLUMN product")
    with pytest.raises(ExtensionError, match="activitydataset lacks the columns product"):
        attach(ExtensionSpec("bio", bio))


def test_id_cache_is_cleared_when_extensions_change(files, tmp_path):
    from bw2data.backends.schema import get_id

    bio, inv = files
    rebuilt = tmp_path / "rebuilt.db"
    rebuilt.write_bytes(bio.read_bytes())
    edit(rebuilt, "UPDATE activitydataset SET id = id + 1000")
    attach(ExtensionSpec("bio", bio))
    first = get_id(("bio", "0"))
    attach(ExtensionSpec("bio", rebuilt))
    assert get_id(("bio", "0")) == first + 1000


@bw2test
def test_other_attached_files_are_left_alone(tmp_path):
    # A file attached by a connection hook of the user has no dictionary table. At least 500
    # datasets, so that a dictionary is trained, stored and used.
    other = tmp_path / "other.db"
    sqlite3.connect(other).close()

    def hook(connection):
        connection.execute("ATTACH DATABASE ? AS other", (str(other),))

    sqlite3_lci_db.add_connection_hook(hook)
    try:
        Database("big").write(
            {
                ("big", str(i)): {"name": f"activity {i}", "comment": "same " * 20}
                for i in range(500)
            }
        )
        assert get_node(database="big", code="7")["comment"] == "same " * 20
        cursor = sqlite3_lci_db.execute_sql("SELECT count(*) FROM zstddictionary")
        assert cursor.fetchone() == (1,)
    finally:
        sqlite3_lci_db.remove_connection_hook(hook)
        sqlite3_lci_db.db.close()


def test_new_snowflake_id_tries_again(monkeypatch):
    from bw2data import snowflake_ids

    # ``None``: too many ids in one millisecond, or the clock was set back
    monkeypatch.setattr(snowflake_ids, "snowflake_id_generator", iter([None, None, 42]))
    assert snowflake_ids.new_snowflake_id() == 42
