"""Schema migration 6: pickled activity and exchange data to compressed tagged JSON (storage
S5), and the parts of ``migrate()`` it needs: postponed steps and actions after the commit."""

import datetime
import json
import pickle
from fractions import Fraction

import numpy as np
import orjson
import pytest

from bw2data import config, get_node, projects
from bw2data.backends import migrations, sqlite3_lci_db
from bw2data.backends.migrations import SCHEMA_VERSION, migrate, schema_version
from bw2data.backends.schema import ActivityDataset, ExchangeDataset
from bw2data.database import DatabaseChooser
from bw2data.sqlite import ZSTD_MAGIC, zstd, zstd_dictionaries
from bw2data.tests import bw2test

COMMENT = "Calculated from the annual production volume of the market, " * 4
TABLES = ("activitydataset", "exchangedataset")
TD_JSON = json.dumps(
    {
        "__loader__": "bw_temporalis.TemporalDistribution",
        "date": [-1, 0, 1],
        "amount": [0.25, 0.5, 0.25],
        "date_dtype": "timedelta64[Y]",
    }
)


def _bulk_data(n: int) -> dict:
    return {
        ("bulk", str(i)): {
            "name": f"activity {i}",
            "unit": "kilogram",
            "location": "CH",
            "type": "process",
            "comment": COMMENT,
            "exchanges": [
                {
                    "input": ("bulk", str((i + j) % n)),
                    "amount": 0.5 + i + j,
                    "type": "technosphere",
                    "uncertainty type": 2,
                    "loc": 0.1,
                    "scale": 0.2,
                    "comment": COMMENT,
                }
                for j in range(3)
            ],
        }
        for i in range(n)
    }


def _rows(table: str) -> dict[int, bytes]:
    return {
        id_: bytes(data)
        for id_, data in sqlite3_lci_db.execute_sql(f'SELECT "id", "data" FROM "{table}"')
    }


def _stored_json(raw: bytes):
    """The tagged JSON in a stored frame, without decoding the tags."""
    assert raw[:4] == ZSTD_MAGIC
    return orjson.loads(zstd_dictionaries(sqlite3_lci_db.db).decompress(raw))


def _make_legacy() -> None:
    """Store every row as bw2data 4 did: pickled, with ``input`` and ``output`` in the
    exchange data, and no zstd dictionary."""
    for model, table in ((ActivityDataset, TABLES[0]), (ExchangeDataset, TABLES[1])):
        for row in model.select():
            sqlite3_lci_db.execute_sql(
                f'UPDATE "{table}" SET "data" = ? WHERE "id" = ?',
                (pickle.dumps(row.data, protocol=4), row.id),
            )
    sqlite3_lci_db.execute_sql('UPDATE "zstddictionary" SET "active" = 0')
    sqlite3_lci_db.execute_sql('DELETE FROM "zstddictionary"')
    del sqlite3_lci_db.db._bw_zstd_dictionaries


def _set_pickle(table: str, row_id: int, value) -> None:
    sqlite3_lci_db.execute_sql(
        f'UPDATE "{table}" SET "data" = ? WHERE "id" = ?',
        (pickle.dumps(value, protocol=4), row_id),
    )


def _migrate_from(version: int) -> list[int]:
    sqlite3_lci_db.execute_sql(f"PRAGMA main.user_version = {version}")
    return migrate(sqlite3_lci_db.db, projects.dir)


def _small():
    db = DatabaseChooser("small")
    db.write(
        {
            ("small", "a"): {
                "name": "a",
                "type": "process",
                "exchanges": [
                    {"input": ("small", "b"), "amount": 2.0, "type": "technosphere"},
                    {"input": ("small", "a"), "amount": 1.0, "type": "production"},
                ],
            },
            ("small", "b"): {"name": "b", "type": "process"},
        }
    )
    return db


@bw2test
def test_converts_pickles_and_reads_the_same():
    db = DatabaseChooser("bulk")
    db.write(_bulk_data(600))
    before = db.load()
    arrays = {
        name: db.datapackage().get_resource(name)[0].copy()
        for name in ("bulk_technosphere_matrix.indices", "bulk_technosphere_matrix.data")
    }
    _make_legacy()
    assert all(raw[:1] == b"\x80" for table in TABLES for raw in _rows(table).values())

    assert _migrate_from(5) == [6]
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION

    for table in TABLES:
        for raw in _rows(table).values():
            stored = _stored_json(raw)
            assert "input" not in stored and "output" not in stored
    assert db.load() == before
    db.process()
    for name, array in arrays.items():
        assert np.array_equal(db.datapackage().get_resource(name)[0], array)


@bw2test
def test_trains_one_dictionary_and_makes_it_active():
    DatabaseChooser("bulk").write(_bulk_data(600), process=False)
    _make_legacy()
    _migrate_from(5)

    store = zstd_dictionaries(sqlite3_lci_db.db)
    assert len(store.ids()) == 1
    dict_id = store.active_id()
    assert dict_id
    for table in TABLES:
        assert {zstd.get_frame_info(raw).dictionary_id for raw in _rows(table).values()} == {
            dict_id
        }


@bw2test
def test_small_projects_get_no_dictionary():
    _small()
    _make_legacy()
    _migrate_from(5)
    assert zstd_dictionaries(sqlite3_lci_db.db).ids() == []
    for table in TABLES:
        assert {zstd.get_frame_info(raw).dictionary_id for raw in _rows(table).values()} == {0}
    assert get_node(code="a")["name"] == "a"


@bw2test
def test_uses_the_active_dictionary_without_training_a_new_one():
    DatabaseChooser("bulk").write(_bulk_data(600), process=False)
    dict_id = zstd_dictionaries(sqlite3_lci_db.db).active_id()
    node = get_node(database="bulk", code="1")
    _set_pickle("activitydataset", node.id, node._data)
    _migrate_from(5)
    assert zstd_dictionaries(sqlite3_lci_db.db).ids() == [dict_id]
    assert zstd.get_frame_info(_rows("activitydataset")[node.id]).dictionary_id == dict_id


@bw2test
def test_only_pickled_rows_are_rewritten():
    _small()
    before = _rows("activitydataset")
    node = get_node(code="a")
    _set_pickle("activitydataset", node.id, {**node._data, "when": datetime.date(2020, 1, 2)})
    _migrate_from(5)
    after = _rows("activitydataset")
    assert after[node.id] != before[node.id]
    assert {k: v for k, v in after.items() if k != node.id} == {
        k: v for k, v in before.items() if k != node.id
    }
    assert get_node(id=node.id)["when"] == datetime.date(2020, 1, 2)


@bw2test
def test_numpy_scalars_become_python_numbers():
    _small()
    exc = next(iter(get_node(code="a").technosphere()))
    _set_pickle("exchangedataset", exc.id, {**exc._data, "loc": np.float64(1.5)})
    _migrate_from(5)
    stored = _stored_json(_rows("exchangedataset")[exc.id])
    assert stored["loc"] == 1.5
    assert type(next(iter(get_node(code="a").technosphere()))["loc"]) is float


@bw2test
def test_temporal_distribution_strings_become_tagged_values():
    _small()
    exc = next(iter(get_node(code="a").technosphere()))
    _set_pickle("exchangedataset", exc.id, {**exc._data, "temporal_distribution": TD_JSON})
    other = next(iter(get_node(code="a").production()))
    _set_pickle("exchangedataset", other.id, {**other._data, "temporal_distribution": "not a TD"})
    _migrate_from(5)
    stored = _stored_json(_rows("exchangedataset")[exc.id])
    assert stored["temporal_distribution"] == {
        "__bw__": "bw_temporalis.TemporalDistribution",
        "v": json.loads(TD_JSON),
    }
    assert _stored_json(_rows("exchangedataset")[other.id])["temporal_distribution"] == "not a TD"


@bw2test
def test_refused_pickle_postpones_and_changes_nothing():
    _small()
    _make_legacy()
    node = get_node(code="a")
    _set_pickle("activitydataset", node.id, {**node._data, "f": Fraction(1, 3)})
    before = {table: _rows(table) for table in TABLES}

    with pytest.warns(UserWarning, match="(?s)migration 6.*postponed.*fractions.Fraction"):
        assert _migrate_from(5) == []
    assert schema_version(sqlite3_lci_db.db) == 5
    assert {table: _rows(table) for table in TABLES} == before
    assert zstd_dictionaries(sqlite3_lci_db.db).ids() == []
    # The other rows can still be read
    assert get_node(code="b")["name"] == "b"


@bw2test
def test_value_which_cant_be_stored_postpones(monkeypatch):
    _small()
    node = get_node(code="a")
    _set_pickle("activitydataset", node.id, {**node._data, "f": Fraction(1, 3)})
    monkeypatch.setattr(config, "trusted_pickle_classes", ["fractions.Fraction"])
    with pytest.warns(UserWarning, match="(?s)postponed.*Fraction"):
        assert _migrate_from(5) == []
    assert schema_version(sqlite3_lci_db.db) == 5


@bw2test
def test_postponed_step_keeps_the_earlier_ones():
    _small()
    node = get_node(code="a")
    _set_pickle("activitydataset", node.id, {**node._data, "f": Fraction(1, 3)})
    with pytest.warns(UserWarning, match="migration 6"):
        assert _migrate_from(4) == [5]
    assert schema_version(sqlite3_lci_db.db) == 5

    # Tried again, and applied once the row is fixed
    _set_pickle("activitydataset", node.id, {**node._data, "f": 1 / 3})
    assert migrate(sqlite3_lci_db.db, projects.dir) == [6]
    assert get_node(id=node.id)["f"] == 1 / 3


@bw2test
def test_not_enough_disk_space_postpones(monkeypatch):
    _small()
    _make_legacy()
    before = {table: _rows(table) for table in TABLES}
    monkeypatch.setattr(migrations, "_free_space", lambda path: 0)
    with pytest.warns(UserWarning, match="free disk space"):
        assert _migrate_from(5) == []
    assert schema_version(sqlite3_lci_db.db) == 5
    assert {table: _rows(table) for table in TABLES} == before


@bw2test
def test_vacuums_after_the_commit_and_switches_page_size(monkeypatch):
    monkeypatch.setattr(config, "vacuum", None)
    DatabaseChooser("bulk").write(_bulk_data(600), process=False)
    _make_legacy()
    sqlite3_lci_db.execute_sql("PRAGMA page_size = 4096")
    sqlite3_lci_db.execute_sql("VACUUM")
    sqlite3_lci_db.execute_sql("PRAGMA page_size = 16384")
    assert sqlite3_lci_db.execute_sql("PRAGMA page_size").fetchone()[0] == 4096

    _migrate_from(5)
    assert sqlite3_lci_db.execute_sql("PRAGMA page_size").fetchone()[0] == 16384
    assert sqlite3_lci_db.freelist_count() == 0


@bw2test
def test_no_vacuum_if_disabled(monkeypatch):
    monkeypatch.setattr(config, "vacuum", False)
    DatabaseChooser("bulk").write(_bulk_data(600), process=False)
    _make_legacy()
    db = sqlite3_lci_db.db
    statements, execute_sql = [], db.execute_sql

    def spy(sql, *args, **kwargs):
        statements.append(sql)
        return execute_sql(sql, *args, **kwargs)

    monkeypatch.setattr(db, "execute_sql", spy)
    assert _migrate_from(5) == [6]
    assert any('"data"' in sql for sql in statements)
    assert not any("VACUUM" in sql.upper() for sql in statements)


@bw2test
def test_no_vacuum_without_pickled_rows(monkeypatch):
    calls = []
    monkeypatch.setattr(migrations, "_m6_vacuum", lambda db: calls.append(db))
    _small()
    assert _migrate_from(5) == [6]
    assert calls == []


@bw2test
def test_legacy_project_is_migrated_on_open():
    _small()
    _make_legacy()
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 5")
    name = projects.current
    projects.set_current("other")
    projects.set_current(name)
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert all(raw[:4] == ZSTD_MAGIC for table in TABLES for raw in _rows(table).values())
    assert get_node(code="a")["name"] == "a"


@bw2test
def test_active_dictionary_is_kept_with_many_pickled_rows():
    DatabaseChooser("bulk").write(_bulk_data(600), process=False)
    dict_id = zstd_dictionaries(sqlite3_lci_db.db).active_id()
    for row in ActivityDataset.select():
        _set_pickle("activitydataset", row.id, row.data)
    _migrate_from(5)
    assert zstd_dictionaries(sqlite3_lci_db.db).ids() == [dict_id]
    assert {
        zstd.get_frame_info(raw).dictionary_id for raw in _rows("activitydataset").values()
    } == {dict_id}


@bw2test
def test_warning_names_the_rows_which_cant_be_converted():
    _small()
    node = get_node(code="a")
    _set_pickle("activitydataset", node.id, {**node._data, "f": Fraction(1, 3)})
    excs = list(ExchangeDataset.select())
    for exc in excs:
        _set_pickle("exchangedataset", exc.id, {**exc.data, "f": Fraction(1, 3)})
    with pytest.warns(UserWarning) as record:
        _migrate_from(5)
    message = "".join(str(w.message) for w in record)
    # One line for the error, with the number of rows and which they are
    assert message.count("* UnsafePickleError") == 1
    assert "3 row(s)" in message
    assert "activity ('small', 'a')" in message
    assert f"exchange {min(exc.id for exc in excs)} from " in message


@bw2test
def test_space_needed_depends_on_the_pickled_rows(monkeypatch):
    DatabaseChooser("bulk").write(_bulk_data(600), process=False)
    node = get_node(database="bulk", code="1")
    _set_pickle("activitydataset", node.id, node._data)
    size = projects.dir.joinpath("lci", "databases.db").stat().st_size
    # Far less than the file, but enough for the one page which changes
    monkeypatch.setattr(migrations, "_free_space", lambda path: size // 10)
    assert _migrate_from(5) == [6]
    assert _rows("activitydataset")[node.id][:4] == ZSTD_MAGIC


@bw2test
def test_nothing_is_logged_as_converted_when_postponed(capsys):
    _small()
    _make_legacy()
    node = get_node(code="b")
    _set_pickle("activitydataset", node.id, {**node._data, "f": Fraction(1, 3)})
    with pytest.warns(UserWarning, match="postponed"):
        _migrate_from(5)
    assert "Converted" not in capsys.readouterr().out
