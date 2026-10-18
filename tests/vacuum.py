from unittest.mock import patch

import pytest

from bw2data import config
from bw2data.backends import sqlite3_lci_db
from bw2data.database import DatabaseChooser
from bw2data.tests import bw2test

SMALL = {
    ("testy", "A"): {"name": "A"},
    ("testy", "B"): {"name": "B"},
}


def big_data(n=400):
    # Enough rows (with some bulk in ``data``) to occupy many SQLite pages, so that
    # deleting them leaves a large freelist.
    return {("testy", str(i)): {"name": "node " + ("x" * 500), "code": str(i)} for i in range(n)}


@pytest.fixture
def vacuum_spy():
    with patch.object(sqlite3_lci_db, "execute_sql", wraps=sqlite3_lci_db.execute_sql) as spy:
        spy.vacuums = lambda: sum(
            "VACUUM" in str(call.args[0]).upper() for call in spy.call_args_list
        )
        yield spy


@bw2test
def test_write_into_empty_database_skips_vacuum(vacuum_spy):
    with patch.object(config, "vacuum", None):
        DatabaseChooser("testy").write(SMALL)
    assert vacuum_spy.vacuums() == 0


@bw2test
def test_write_vacuums_when_previous_contents_freed_pages(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    with patch.object(config, "vacuum", None):
        database.write(SMALL)
    assert vacuum_spy.vacuums() == 1
    assert sqlite3_lci_db.freelist_count() == 0


@bw2test
def test_rewrite_of_small_database_skips_vacuum(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(SMALL)
    with patch.object(config, "vacuum", None):
        database.write(SMALL)
    assert vacuum_spy.vacuums() == 0


@bw2test
def test_config_vacuum_false_never_vacuums_on_write(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    with patch.object(config, "vacuum", False):
        database.write(SMALL)
    assert vacuum_spy.vacuums() == 0
    assert sqlite3_lci_db.freelist_count() > 0


@bw2test
def test_config_vacuum_false_never_vacuums_on_delete(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    with patch.object(config, "vacuum", False):
        database.delete(warn=False)
    assert vacuum_spy.vacuums() == 0


@bw2test
def test_config_vacuum_true_always_vacuums(vacuum_spy):
    with patch.object(config, "vacuum", True):
        DatabaseChooser("testy").write(SMALL)
    assert vacuum_spy.vacuums() == 1


@bw2test
def test_delete_vacuums_when_pages_freed(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    with patch.object(config, "vacuum", None):
        database.delete(warn=False)
    assert vacuum_spy.vacuums() == 1
    assert sqlite3_lci_db.freelist_count() == 0


@bw2test
def test_delete_vacuum_false_argument_skips_vacuum(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    with patch.object(config, "vacuum", True):
        database.delete(warn=False, vacuum=False)
    assert vacuum_spy.vacuums() == 0


@bw2test
def test_failed_write_honours_vacuum_policy(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    with patch.object(config, "vacuum", False):
        with patch.object(database, "_efficient_write_many_data", side_effect=ValueError):
            with pytest.raises(ValueError):
                database.write(SMALL)
    assert vacuum_spy.vacuums() == 0
