from bw2data.tests import bw2test
from bw2data import projects
from bw2data.backends import sqlite3_lci_db as db
from bw2data.database import DatabaseChooser
from copy import copy


@bw2test
def test_switch_project_correctly_switches_database_objects():
    database = DatabaseChooser("testy")
    data = {
        ("testy", "A"): {},
        ("testy", "C"): {"type": "biosphere"},
        ("testy", "B"): {
            "exchanges": [
                {"input": ("testy", "A"), "amount": 1, "type": "technosphere"},
                {"input": ("testy", "B"), "amount": 1, "type": "production"},
                {"input": ("testy", "C"), "amount": 1, "type": "biosphere"},
            ]
        },
    }
    database.write(data)

    table = db._tables[0]
    current_db_location = copy(db.db.database)
    assert table.select().count()

    projects.set_current("new one")
    assert not table.select().count()
    assert current_db_location != db.db.database
