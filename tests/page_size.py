from unittest.mock import patch

from bw2data import config
from bw2data.backends import LCI_PAGE_SIZE, sqlite3_lci_db
from bw2data.database import DatabaseChooser
from bw2data.tests import bw2test


def page_size() -> int:
    return sqlite3_lci_db.execute_sql("PRAGMA main.page_size").fetchone()[0]


def data(n=200):
    return {
        ("testy", str(i)): {
            "name": f"node {i}",
            "exchanges": [{"input": ("testy", str(i)), "amount": 1.0, "type": "production"}],
        }
        for i in range(n)
    }


def make_old_file():
    """Turn the current project's LCI file into one with SQLite's default 4 KB pages, as
    written by earlier bw2data versions, and open it again as bw2data would."""
    sqlite3_lci_db.execute_sql("PRAGMA main.page_size = 4096")
    sqlite3_lci_db.execute_sql("VACUUM")
    # Reopen the file, which applies the connection pragmas again
    sqlite3_lci_db.change_path(sqlite3_lci_db._filepath)


@bw2test
def test_new_lci_file_uses_lci_page_size():
    assert LCI_PAGE_SIZE == 16384
    assert page_size() == LCI_PAGE_SIZE


@bw2test
def test_lci_file_does_not_use_wal_journal():
    # ``page_size`` can't be changed in WAL mode, so old files would never be converted
    mode = sqlite3_lci_db.execute_sql("PRAGMA main.journal_mode").fetchone()[0]
    assert mode.lower() != "wal"


@bw2test
def test_old_lci_file_keeps_its_page_size_until_vacuumed():
    DatabaseChooser("testy").write(data())
    make_old_file()
    assert page_size() == 4096
    assert len(DatabaseChooser("testy")) == 200


@bw2test
def test_vacuum_converts_old_lci_file_to_lci_page_size():
    DatabaseChooser("testy").write(data())
    make_old_file()
    assert page_size() == 4096

    with patch.object(config, "vacuum", True):
        assert sqlite3_lci_db.vacuum()

    assert page_size() == LCI_PAGE_SIZE
    database = DatabaseChooser("testy")
    assert len(database) == 200
    assert database.get("17")["name"] == "node 17"
    assert len(list(database.get("17").exchanges())) == 1


@bw2test
def test_database_write_vacuum_converts_old_lci_file():
    # ``Database.write()`` vacuums under the default policy when the previous contents
    # leave enough free pages, which converts the file as a side effect
    database = DatabaseChooser("testy")
    database.write(data())
    make_old_file()
    with patch.object(config, "vacuum", None):
        database.write(data(10))
    assert page_size() == LCI_PAGE_SIZE
    assert len(database) == 10
