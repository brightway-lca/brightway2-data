"""Schema migration 8: the indexes of the activity and exchange tables always exist (#357)."""

import warnings

import pytest

from bw2data import projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import (
    LCI_INDEXES,
    SCHEMA_VERSION,
    make_activity_keys_unique,
    migrate,
    schema_version,
)
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
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION


@bw2test
def test_creates_missing_indexes():
    DatabaseChooser("db").write({("db", "a"): {"name": "a"}, ("db", "b"): {"name": "b"}})
    as_version_7_without_indexes()
    assert indexes() == set()
    assert migrate(sqlite3_lci_db.db, projects.dir) == [8, 9, 10, 11]
    assert indexes() == NAMES
    assert len(DatabaseChooser("db")) == 2


@bw2test
def test_keeps_existing_indexes():
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 7")
    assert migrate(sqlite3_lci_db.db, projects.dir) == [8, 9, 10, 11]
    assert indexes() == NAMES


def key_index_is_unique():
    return {
        row[1]: bool(row[2])
        for row in sqlite3_lci_db.execute_sql('PRAGMA main.index_list("activitydataset")')
    }["activitydataset_key"]


def add_duplicate():
    sqlite3_lci_db.execute_sql(
        'INSERT INTO main."activitydataset" (id, data, code, database) '
        "SELECT id + 1, data, code, database FROM main.activitydataset"
    )


@bw2test
def test_duplicate_keys_get_an_index_which_is_not_unique():
    DatabaseChooser("db").write({("db", "a"): {"name": "a"}})
    as_version_7_without_indexes()
    # Without the unique index, nothing kept a second activity with the same key out
    add_duplicate()
    with pytest.warns(UserWarning, match=r"\('db', 'a'\) 2 times.*unique"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == [8, 9, 10, 11]
    # The step is not postponed: the exchange indexes exist, and later steps can run
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert indexes() == NAMES
    assert not key_index_is_unique()


@bw2test
def test_index_becomes_unique_once_the_duplicates_are_gone():
    DatabaseChooser("db").write({("db", "a"): {"name": "a"}})
    as_version_7_without_indexes()
    add_duplicate()
    with pytest.warns(UserWarning, match="2 times"):
        migrate(sqlite3_lci_db.db, projects.dir)
    # Opening the file again warns again, and changes nothing
    with pytest.warns(UserWarning, match=r"\('db', 'a'\) 2 times.*delete_instance"):
        sqlite3_lci_db.change_path(sqlite3_lci_db._filepath)
    assert not key_index_is_unique()

    sqlite3_lci_db.execute_sql(
        'DELETE FROM main."activitydataset" WHERE id = (SELECT max(id) FROM main.activitydataset)'
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        sqlite3_lci_db.change_path(sqlite3_lci_db._filepath)
    assert key_index_is_unique()
    assert indexes() == NAMES
    assert make_activity_keys_unique(sqlite3_lci_db.db)
    with pytest.raises(Exception, match="UNIQUE"):
        add_duplicate()


@bw2test
def test_unique_index_is_left_alone():
    assert key_index_is_unique()
    assert make_activity_keys_unique(sqlite3_lci_db.db)
    assert key_index_is_unique()


@bw2test
def test_index_helpers_are_deprecated_and_do_nothing():
    database = DatabaseChooser("db")
    database.register()
    with pytest.warns(DeprecationWarning, match="_drop_indices"):
        database._drop_indices()
    assert indexes() == NAMES
    with pytest.warns(DeprecationWarning, match="_add_indices"):
        database._add_indices()
    with pytest.warns(DeprecationWarning, match="indices=False"):
        database._efficient_write_many_data([{"database": "db", "code": "a"}], indices=False)
    assert indexes() == NAMES
    assert len(database) == 1


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
