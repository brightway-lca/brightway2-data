import sqlite3
from unittest.mock import patch

import pytest

from bw2data import config, databases
from bw2data.backends import sqlite3_lci_db
from bw2data.database import DatabaseChooser
from bw2data.errors import InvalidExchange
from bw2data.sqlite import AUTO_VACUUM_INCREMENTAL, zstd_dictionaries
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
    """``vacuums()``: the number of full ``VACUUM`` statements; ``incremental()``: the number
    of ``PRAGMA incremental_vacuum`` statements."""

    def statements():
        return [str(call.args[0]).strip().upper() for call in spy.call_args_list]

    with patch.object(sqlite3_lci_db, "execute_sql", wraps=sqlite3_lci_db.execute_sql) as spy:
        spy.vacuums = lambda: sum(sql.startswith("VACUUM") for sql in statements())
        spy.incremental = lambda: sum("INCREMENTAL_VACUUM" in sql for sql in statements())
        yield spy


def make_file_without_auto_vacuum():
    """As LCI files written by earlier versions: ``auto_vacuum`` off. Reopening applies the
    connection pragmas again, which only take effect at the next ``VACUUM``."""
    sqlite3_lci_db.execute_sql("PRAGMA main.auto_vacuum = 0")
    sqlite3_lci_db.execute_sql("VACUUM")
    sqlite3_lci_db.change_path(sqlite3_lci_db._filepath)
    assert sqlite3_lci_db.auto_vacuum() == 0


@bw2test
def test_write_into_empty_database_skips_vacuum(vacuum_spy):
    with patch.object(config, "vacuum", None):
        DatabaseChooser("testy").write(SMALL)
    assert vacuum_spy.vacuums() == 0


@bw2test
def test_new_lci_file_releases_free_pages_incrementally():
    assert sqlite3_lci_db.auto_vacuum() == AUTO_VACUUM_INCREMENTAL


@bw2test
def test_smaller_rewrite_releases_free_pages_incrementally(vacuum_spy):
    # The new rows use the pages which the old ones freed; the rest is released
    database = DatabaseChooser("testy")
    database.write(big_data())
    pages = sqlite3_lci_db.page_count()
    with patch.object(config, "vacuum", None):
        database.write(SMALL)
    assert vacuum_spy.vacuums() == 0
    assert vacuum_spy.incremental() == 1
    assert sqlite3_lci_db.freelist_count() == 0
    assert sqlite3_lci_db.page_count() < pages


@bw2test
def test_smaller_rewrite_vacuums_an_older_file_and_converts_it(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    make_file_without_auto_vacuum()
    vacuum_spy.reset_mock()
    with patch.object(config, "vacuum", None):
        database.write(SMALL)
    assert vacuum_spy.vacuums() == 1
    assert sqlite3_lci_db.freelist_count() == 0
    assert sqlite3_lci_db.auto_vacuum() == AUTO_VACUUM_INCREMENTAL


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
def test_delete_releases_free_pages_incrementally(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    with patch.object(config, "vacuum", None):
        database.delete(warn=False)
    assert vacuum_spy.vacuums() == 0
    assert vacuum_spy.incremental() == 1
    assert sqlite3_lci_db.freelist_count() == 0


@bw2test
def test_delete_vacuums_an_older_file_and_converts_it(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(big_data())
    make_file_without_auto_vacuum()
    vacuum_spy.reset_mock()
    with patch.object(config, "vacuum", None):
        database.delete(warn=False)
    assert vacuum_spy.vacuums() == 1
    assert vacuum_spy.incremental() == 0
    assert sqlite3_lci_db.freelist_count() == 0
    # From now on, free pages are released incrementally
    assert sqlite3_lci_db.auto_vacuum() == AUTO_VACUUM_INCREMENTAL


@bw2test
def test_small_delete_in_an_older_file_skips_vacuum(vacuum_spy):
    # Deleting two nodes frees less than 10% of a file which holds 400 others
    other = {("other", str(i)): {"name": "node " + ("x" * 500)} for i in range(400)}
    DatabaseChooser("other").write(other)
    database = DatabaseChooser("testy")
    database.write(SMALL)
    make_file_without_auto_vacuum()
    vacuum_spy.reset_mock()
    with patch.object(config, "vacuum", None):
        database.delete(warn=False)
    assert vacuum_spy.vacuums() == 0


@bw2test
def test_delete_warns_if_free_pages_cant_be_released():
    # The rows are deleted by then: a failure must not make the delete look like it failed
    DatabaseChooser("testy").write(big_data())
    with patch.object(
        sqlite3_lci_db, "vacuum", side_effect=sqlite3.OperationalError("database is locked")
    ):
        with pytest.warns(UserWarning, match="was deleted, but .*could not be compacted"):
            del databases["testy"]
    assert "testy" not in databases
    assert len(DatabaseChooser("testy")) == 0


def dictionary_ids():
    return zstd_dictionaries(sqlite3_lci_db.db).ids()


@bw2test
def test_rewrite_removes_the_dictionary_it_replaces():
    # Each write of this size trains a dictionary. The rewrite frees no pages to speak of,
    # so nothing else would remove the dictionary of the rows it replaced.
    database = DatabaseChooser("testy")
    database.write(linked_data("testy", 600, per_activity=2))
    first = dictionary_ids()
    assert len(first) == 1
    with patch.object(config, "vacuum", None):
        database.write(linked_data("testy", 600, per_activity=2))
    second = dictionary_ids()
    assert len(second) == 1
    assert second != first


@bw2test
def test_first_write_keeps_the_dictionaries_of_other_databases():
    DatabaseChooser("other").write(linked_data("other", 600, per_activity=2))
    with patch.object(config, "vacuum", None):
        DatabaseChooser("testy").write(linked_data("testy", 600, per_activity=2))
    assert len(dictionary_ids()) == 2
    assert len(DatabaseChooser("other")) == 600
    assert DatabaseChooser("other").get("0")["name"] == "node 0"


@bw2test
def test_large_delete_removes_unused_dictionaries_incrementally(vacuum_spy):
    database = DatabaseChooser("testy")
    database.write(linked_data("testy", 600, per_activity=2))
    DatabaseChooser("other").write(linked_data("other", 600, per_activity=2))
    assert len(dictionary_ids()) == 2
    with patch.object(config, "vacuum", None):
        database.delete(warn=False)
    assert vacuum_spy.vacuums() == 0
    assert vacuum_spy.incremental() == 1
    assert dictionary_ids() == [zstd_dictionaries(sqlite3_lci_db.db).active_id()]
    assert DatabaseChooser("other").get("0")["name"] == "node 0"


def cache_size():
    return sqlite3_lci_db.execute_sql("PRAGMA main.cache_size").fetchone()[0]


@bw2test
def test_write_raises_the_cache_size_and_sets_it_back(vacuum_spy):
    before = cache_size()
    with patch.object(config, "write_cache_size", 1024):
        DatabaseChooser("testy").write(SMALL)
    statements = [str(call.args[0]) for call in vacuum_spy.call_args_list]
    assert "PRAGMA main.cache_size = -1024" in statements
    assert cache_size() == before


@bw2test
def test_write_cache_size_zero_keeps_the_cache_size(vacuum_spy):
    with patch.object(config, "write_cache_size", 0):
        DatabaseChooser("testy").write(SMALL)
    assert not any("cache_size" in str(call.args[0]) for call in vacuum_spy.call_args_list)


@bw2test
def test_failure_to_set_the_cache_size_back_keeps_the_error_of_the_write():
    execute_sql = sqlite3_lci_db.execute_sql
    restored = []

    def fail_to_restore(sql, *args, **kwargs):
        if "cache_size =" in str(sql) and "-1024" not in str(sql):
            restored.append(sql)
            raise sqlite3.OperationalError("disk I/O error")
        return execute_sql(sql, *args, **kwargs)

    bad = {("testy", "A"): {"name": "A", "exchanges": [{"input": ("testy", "A")}]}}
    with patch.object(config, "write_cache_size", 1024):
        with patch.object(sqlite3_lci_db, "execute_sql", side_effect=fail_to_restore):
            with pytest.raises(InvalidExchange):
                DatabaseChooser("testy").write(bad)
    assert len(restored) == 1


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


def linked_data(name, n, per_activity=10):
    """``n`` activities with ``per_activity`` exchanges each, between activities of ``name``:
    many exchange rows, so that the indexes are a large share of the file."""
    return {
        (name, str(i)): {
            "name": f"node {i}",
            "exchanges": [
                {"input": (name, str((i + j) % n)), "amount": 1, "type": "technosphere"}
                for j in range(per_activity)
            ],
        }
        for i in range(n)
    }


@bw2test
def test_bulk_write_next_to_a_large_database_skips_vacuum(vacuum_spy):
    # The indexes are kept during the write, and nothing was deleted: no free space to
    # reclaim
    DatabaseChooser("background").write(linked_data("background", 2_000))
    with patch.object(config, "vacuum", None):
        DatabaseChooser("foreground").write(linked_data("foreground", 100))
    assert vacuum_spy.vacuums() == 0
    assert sqlite3_lci_db.freelist_count() == 0
