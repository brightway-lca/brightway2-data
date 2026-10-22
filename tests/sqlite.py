from copy import copy

from bw2data import projects
from bw2data.backends import sqlite3_lci_db as db
from bw2data.database import DatabaseChooser
from bw2data.tests import bw2test


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


@bw2test
def test_write_closes_its_cursors(monkeypatch):
    # An unclosed cursor holds its statement, and so the file, until garbage collection.
    import peewee
    import pytest

    from bw2data.backends import base

    sqlite3 = peewee.sqlite3  # pysqlite3 when installed
    opened, cursors, helper_cursors, calls = [], [], [], []

    class RecordingConnection(sqlite3.Connection):
        def cursor(self, *args, **kwargs):
            cursors.append(super().cursor(*args, **kwargs))
            return cursors[-1]

    def recording_connect(*args, **kwargs):
        opened.append(connect(*args, factory=RecordingConnection, **kwargs))
        return opened[-1]

    def recording_insert_many(model, rows):
        start = len(cursors)
        insert_many(model, rows)
        helper_cursors.extend(cursors[start:])
        calls.append(model)

    connect, insert_many = sqlite3.connect, base._insert_many
    monkeypatch.setattr(sqlite3, "connect", recording_connect)
    monkeypatch.setattr(base, "_insert_many", recording_insert_many)
    # Small batches, so the write also flushes mid-way
    monkeypatch.setattr(base, "WRITE_BATCH_SIZE", 7)

    projects.set_current("leak check")  # reconnects through recording_connect
    data = {
        ("testy", str(i)): {
            "name": str(i),
            "exchanges": [{"input": ("testy", "0"), "amount": 1, "type": "technosphere"}],
        }
        # 100 or more takes the bulk path that drops and rebuilds the indices
        for i in range(150)
    }
    DatabaseChooser("testy").write(data)

    assert opened and helper_cursors
    assert calls.count(base.ActivityDataset) > 1 and calls.count(base.ExchangeDataset) > 1
    for cursor in helper_cursors:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            cursor.execute("SELECT 1")
    assert db._tables[0].select().count() == 150
    assert db._tables[1].select().count() == 150


@bw2test
def test_write_stores_columns_as_peewee_does():
    import numpy as np

    from bw2data.backends.schema import ActivityDataset, ExchangeDataset
    from bw2data.backends.utils import dict_as_activitydataset, dict_as_exchangedataset

    data = {
        ("testy", np.int64(1)): {
            "name": 1 / 3,
            "reference product": True,
            "location": ("foo", "bar"),
            "nested": {"a": [1, {"b": (2,)}]},
            "exchanges": [
                {"input": ("testy", "b"), "amount": 1, "type": "technosphere", "extra": [1]}
            ],
        },
        ("testy", "b"): {
            "name": ["x"],
            "location": None,
            "exchanges": [{"input": ("testy", np.int64(1)), "amount": 2, "type": "technosphere"}],
        },
        ("testy", "c"): {"name": "café 日本", "reference product": 2**64},
    }
    # Search indexing needs string names, and is not what this test is about
    DatabaseChooser("testy").write(data, searchable=False)

    def check(model, row, expected):
        assert row.data == expected.pop("data")
        for column, value in expected.items():
            assert getattr(row, column) == getattr(model, column).db_value(value), column

    for ds in data.values():
        code = ActivityDataset.code.db_value(ds["code"])
        activity = {k: v for k, v in ds.items() if k != "exchanges"}
        check(
            ActivityDataset,
            ActivityDataset.get(ActivityDataset.code == code),
            dict_as_activitydataset(activity),
        )
        for exc in ds.get("exchanges", []):
            row = ExchangeDataset.get(ExchangeDataset.output_code == code)
            check(ExchangeDataset, row, dict_as_exchangedataset(exc))


@bw2test
def test_write_duplicate_key_raises_peewee_integrity_error():
    import peewee
    import pytest

    # A write of 100 or more datasets creates the unique (database, code) index
    DatabaseChooser("other").write({("other", str(i)): {} for i in range(100)})
    data = [{"database": "testy", "code": "a"}, {"database": "testy", "code": "a"}]
    with pytest.raises(peewee.IntegrityError):
        DatabaseChooser("testy").write(data)
