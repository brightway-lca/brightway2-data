"""Schema migration 8: the indexes of the activity and exchange tables always exist (#357)."""

import pytest

from bw2data import projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import LCI_INDEXES, SCHEMA_VERSION, migrate, schema_version
from bw2data.database import DatabaseChooser
from bw2data.tests import bw2test

NAMES = {name for name, _ in LCI_INDEXES}


def indexes():
    return {
        row[0]
        for row in sqlite3_lci_db.execute_sql(
            "SELECT name FROM main.sqlite_master WHERE type = 'index'"
        ).fetchall()
    } & NAMES


def as_version_7_without_indexes():
    """As a file of the previous schema version which never had a write of 100 datasets."""
    for name in NAMES:
        sqlite3_lci_db.execute_sql(f'DROP INDEX IF EXISTS main."{name}"')
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 7")


@bw2test
def test_new_files_have_the_indexes():
    assert indexes() == NAMES
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION == 8


@bw2test
def test_creates_missing_indexes():
    DatabaseChooser("db").write({("db", "a"): {"name": "a"}, ("db", "b"): {"name": "b"}})
    as_version_7_without_indexes()
    assert indexes() == set()
    assert migrate(sqlite3_lci_db.db, projects.dir) == [8]
    assert indexes() == NAMES
    assert len(DatabaseChooser("db")) == 2


@bw2test
def test_keeps_existing_indexes():
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 7")
    assert migrate(sqlite3_lci_db.db, projects.dir) == [8]
    assert indexes() == NAMES


@bw2test
def test_duplicate_keys_postpone_the_step():
    DatabaseChooser("db").write({("db", "a"): {"name": "a"}})
    as_version_7_without_indexes()
    # Without the unique index, nothing kept a second activity with the same key out
    sqlite3_lci_db.execute_sql(
        'INSERT INTO main."activitydataset" (id, data, code, database) '
        "SELECT id + 1, data, code, database FROM main.activitydataset"
    )
    with pytest.warns(UserWarning, match=r"\('db', 'a'\) 2 times.*unique"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == []
    assert schema_version(sqlite3_lci_db.db) == 7
    assert indexes() == set()
    # Once the duplicate is gone, the next opening creates them
    sqlite3_lci_db.execute_sql(
        'DELETE FROM main."activitydataset" WHERE id = (SELECT max(id) FROM main.activitydataset)'
    )
    assert migrate(sqlite3_lci_db.db, projects.dir) == [8]
    assert indexes() == NAMES


@bw2test
def test_writes_keep_the_indexes():
    data = {("db", str(i)): {"name": str(i)} for i in range(150)}
    DatabaseChooser("db").write(data)
    assert indexes() == NAMES
    # The database refuses a second activity with the same key (the node proxies check this
    # too, before they save)
    with pytest.raises(Exception, match="UNIQUE"):
        sqlite3_lci_db.execute_sql(
            'INSERT INTO main."activitydataset" (id, data, code, database) '
            "SELECT id + 1000000, data, code, database FROM main.activitydataset LIMIT 1"
        )
    assert len(DatabaseChooser("db")) == 150
