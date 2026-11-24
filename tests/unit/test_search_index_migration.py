"""Schema migration 7: the search indexes move from one file per database in ``search/`` into
contentless FTS5 tables in ``lci/databases.db`` (storage S7)."""

import sqlite3

import pytest

from bw2data import databases, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import SCHEMA_VERSION, migrate, schema_version
from bw2data.database import DatabaseChooser
from bw2data.search.indices import FIELDS, index_table
from bw2data.tests import bw2test


def tables():
    return {row[0] for row in sqlite3_lci_db.execute_sql("SELECT name FROM sqlite_master")}


def _as_before(*names):
    """The project as bw2data stored it before step 7: no index tables, and one index file
    per database in ``search/``."""
    for name in names:
        sqlite3_lci_db.execute_sql(f'DROP TABLE IF EXISTS "{index_table(name)}"')
    directory = projects.dir / "search"
    directory.mkdir(exist_ok=True)
    for name in names:
        conn = sqlite3.connect(directory / DatabaseChooser(name).filename)
        conn.execute("CREATE VIRTUAL TABLE bw2schema USING fts5(name, code)")
        conn.execute("INSERT INTO bw2schema VALUES ('old index', 'x')")
        conn.commit()
        conn.close()
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 6")


def _write():
    DatabaseChooser("first").write(
        {
            ("first", "a"): {
                "name": "Lollipop",
                "comment": "Sweet",
                "reference product": "Candy",
                "categories": ("air", "urban"),
                "synonyms": ["Sucker"],
                "location": "CH",
                "type": "process",
            },
            ("first", "b"): {"name": "ice cream", "location": ("world", "Here")},
        }
    )
    DatabaseChooser("second").write({("second", "c"): {"name": "lollipop"}})
    DatabaseChooser("unsearchable").write({("unsearchable", "d"): {"name": "lollipop"}})
    DatabaseChooser("unsearchable").make_unsearchable()


@bw2test
def test_builds_the_indexes_and_removes_the_files():
    _write()
    _as_before("first", "second")
    assert not [name for name in tables() if name.startswith("searchindex_")]

    assert migrate(sqlite3_lci_db.db, projects.dir) == [7]
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION == 7

    assert index_table("first") in tables()
    assert index_table("second") in tables()
    assert index_table("unsearchable") not in tables()
    assert list((projects.dir / "search").iterdir()) == []

    first = DatabaseChooser("first")
    for term in ("lollipop", "sweet", "candy", "urban", "sucker", "ch"):
        assert [node.key for node in first.search(term)] == [("first", "a")], term
    assert [node.key for node in first.search("here")] == [("first", "b")]
    assert not first.search("old")
    assert [node.key for node in DatabaseChooser("second").search("lollipop")] == [("second", "c")]


@bw2test
def test_gives_the_same_index_as_bw2data_builds():
    _write()

    def segments(name):
        table = index_table(name)
        columns = ", ".join(FIELDS)
        sqlite3_lci_db.execute_sql(f'INSERT INTO "{table}" ("{table}") VALUES (\'optimize\')')
        return (
            sqlite3_lci_db.execute_sql(f'SELECT block FROM "{table}_data" ORDER BY id').fetchall(),
            columns,
        )

    expected = segments("first")
    _as_before("first", "second")
    migrate(sqlite3_lci_db.db, projects.dir)
    assert segments("first") == expected

    # So a node indexed by the migration can be changed and deleted
    node = DatabaseChooser("first").get("a")
    node["name"] = "toffee"
    node.save()
    first = DatabaseChooser("first")
    assert not first.search("lollipop")
    assert [n.key for n in first.search("toffee")] == [("first", "a")]
    node.delete()
    assert not first.search("toffee")
    assert [n.key for n in first.search("ice")] == [("first", "b")]


@bw2test
def test_unreadable_nodes_make_the_database_unsearchable():
    _write()
    _as_before("first", "second")
    sqlite3_lci_db.execute_sql(
        "UPDATE activitydataset SET data = ? WHERE code = 'a'", (b"\x28\xb5\x2f\xfdnot zstd",)
    )
    with pytest.warns(UserWarning, match="Can't build the search index of database 'first'"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == [7]
    assert index_table("first") not in tables()
    assert index_table("second") in tables()
    searchable = dict(sqlite3_lci_db.execute_sql("SELECT name, searchable FROM databaserecord"))
    assert searchable == {"first": 0, "second": 1, "unsearchable": 0}


@bw2test
def test_project_without_search_directory():
    _write()
    _as_before("first")
    for path in (projects.dir / "search").iterdir():
        path.unlink()
    (projects.dir / "search").rmdir()
    assert migrate(sqlite3_lci_db.db, projects.dir) == [7]
    assert DatabaseChooser("first").search("lollipop")
