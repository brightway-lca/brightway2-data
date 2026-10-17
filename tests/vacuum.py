from unittest.mock import patch

from bw2data import config
from bw2data.backends import sqlite3_lci_db
from bw2data.database import DatabaseChooser
from bw2data.tests import bw2test

DATA = {
    ("testy", "A"): {"name": "A"},
    ("testy", "B"): {"name": "B"},
}


def write_and_count_vacuums(policy, prefill=False):
    database = DatabaseChooser("testy")
    if prefill:
        database.write(DATA)
    with patch.object(config, "vacuum", policy), patch.object(sqlite3_lci_db, "vacuum") as spy:
        database.write(DATA)
        return spy.call_count


@bw2test
def test_write_skips_vacuum_when_database_was_empty():
    assert write_and_count_vacuums(None) == 0


@bw2test
def test_write_vacuums_when_database_was_not_empty():
    assert write_and_count_vacuums(None, prefill=True) == 1


@bw2test
def test_config_vacuum_false_never_vacuums():
    assert write_and_count_vacuums(False, prefill=True) == 0


@bw2test
def test_config_vacuum_true_vacuums_empty_database():
    assert write_and_count_vacuums(True) == 1
