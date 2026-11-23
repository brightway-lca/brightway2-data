"""Exchange ``data`` blobs don't store ``input`` and ``output`` (storage S4)."""

import pickle

from bw2data import codec, get_node
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.base import DICTIONARY_MIN_DATASETS
from bw2data.backends.schema import ExchangeDataset
from bw2data.backends.utils import exchange_row_as_dict
from bw2data.database import DatabaseChooser
from bw2data.revisions import RevisionedEdge
from bw2data.sqlite import ZSTD_MAGIC, zstd_dictionaries
from bw2data.tests import bw2test


def _stored(row_id: int):
    """The value in the ``data`` column of exchange ``row_id``, decoded but without the keys
    the reader adds."""
    raw = bytes(
        sqlite3_lci_db.execute_sql(
            "SELECT data FROM exchangedataset WHERE id = ?", (row_id,)
        ).fetchone()[0]
    )
    assert raw[:4] == ZSTD_MAGIC
    return zstd_dictionaries(sqlite3_lci_db.db).decompress(raw)


def _database(name: str = "db", n: int = 2) -> DatabaseChooser:
    db = DatabaseChooser(name)
    db.write(
        {
            (name, str(i)): {
                "name": str(i),
                "type": "process",
                "exchanges": [
                    {"input": (name, str(i)), "amount": 1.0, "type": "production"},
                    {
                        "input": (name, str((i + 1) % n)),
                        "amount": 0.5,
                        "type": "technosphere",
                        "pedigree": {"reliability": 2},
                    },
                ],
            }
            for i in range(n)
        },
        process=False,
    )
    return db


def _all_exchange_ids() -> list[int]:
    return [row.id for row in ExchangeDataset.select(ExchangeDataset.id)]


@bw2test
def test_bulk_write_leaves_keys_out_and_untagged():
    _database()
    for row_id in _all_exchange_ids():
        raw = _stored(row_id)
        assert b'"input"' not in raw and b'"output"' not in raw
        # Nothing left to tag, so the row is decoded on the fast path
        assert b"__bw__" not in raw


@bw2test
def test_bulk_write_with_dictionary_leaves_keys_out(monkeypatch):
    samples = []
    store_cls = type(zstd_dictionaries(sqlite3_lci_db.db))
    original = store_cls.train

    def train(self, values, activate=True):
        samples.extend(values)
        return original(self, values, activate)

    monkeypatch.setattr(store_cls, "train", train)
    _database(n=DICTIONARY_MIN_DATASETS)
    exchange_samples = [s for s in samples if b'"amount"' in s]
    assert exchange_samples
    # The dictionary is trained on the rows as they are stored
    assert not any(b'"input"' in s or b'"output"' in s for s in exchange_samples)
    assert not any(b'"input"' in _stored(row_id) for row_id in _all_exchange_ids()[:20])


@bw2test
def test_readers_get_keys_as_tuples():
    db = _database()
    node = get_node(database="db", code="0")

    for exc in node.exchanges():
        assert isinstance(exc["input"], tuple) and isinstance(exc["output"], tuple)
        assert exc["output"] == ("db", "0")
    assert {exc["input"] for exc in node.technosphere()} == {("db", "1")}

    loaded = db.load()
    assert [exc["input"] for exc in loaded[("db", "0")]["exchanges"]] == [
        ("db", "0"),
        ("db", "1"),
    ]
    assert all(exc["output"] == ("db", "0") for exc in loaded[("db", "0")]["exchanges"])

    row = ExchangeDataset.get(ExchangeDataset.type == "technosphere")
    assert row.data["input"] == (row.input_database, row.input_code)
    assert row.data["output"] == (row.output_database, row.output_code)

    rows = ExchangeDataset.select().dicts()
    for row in rows:
        assert "input" not in row["data"]
        data = exchange_row_as_dict(row)
        assert data["input"] == (row["input_database"], row["input_code"])
        assert data["output"] == (row["output_database"], row["output_code"])


@bw2test
def test_model_instance_without_key_columns_gives_data_as_stored():
    _database()
    row = ExchangeDataset.select(ExchangeDataset.data).first()
    assert "input" not in row.data and "output" not in row.data


@bw2test
def test_single_saves_leave_keys_out():
    _database()
    node = get_node(database="db", code="0")
    edge = node.new_edge(input=("db", "1"), amount=3, type="technosphere")
    edge.save()
    assert b'"input"' not in _stored(edge.id)
    # The data in memory is unchanged
    assert edge["input"] == ("db", "1") and edge["output"] == ("db", "0")

    edge["amount"] = 4
    edge.save()
    assert b'"input"' not in _stored(edge.id)

    data = {"amount": 5, "type": "biosphere", "input": ("db", "1"), "output": ("db", "0")}
    row = ExchangeDataset(
        data=data,
        input_database="db",
        input_code="1",
        output_database="db",
        output_code="0",
        type="biosphere",
    )
    row.save()
    assert b'"input"' not in _stored(row.id)

    ExchangeDataset.update(data={**data, "amount": 6}).where(ExchangeDataset.id == row.id).execute()
    assert b'"input"' not in _stored(row.id)
    assert ExchangeDataset.get_by_id(row.id).data == {**data, "amount": 6}


@bw2test
def test_copy_gives_new_keys():
    _database()
    node = get_node(database="db", code="0")
    new = node.copy(code="copy")
    assert {exc["input"] for exc in new.production()} == {("db", "copy")}
    assert {exc["output"] for exc in new.exchanges()} == {("db", "copy")}
    for exc in new.exchanges():
        assert b'"input"' not in _stored(exc.id)


@bw2test
def test_old_rows_with_keys_read_the_same():
    _database()
    exc = next(iter(get_node(database="db", code="0").technosphere()))
    data = dict(exc._data)

    # A pickle from an older version, and a compressed row written by S3
    for value in (
        pickle.dumps(data, protocol=4),
        zstd_dictionaries(sqlite3_lci_db.db).compress(codec.dumps(data)),
    ):
        sqlite3_lci_db.execute_sql(
            "UPDATE exchangedataset SET data = ? WHERE id = ?", (value, exc.id)
        )
        assert next(iter(get_node(database="db", code="0").technosphere()))._data == data
        assert ExchangeDataset.get_by_id(exc.id).data == data


@bw2test
def test_key_columns_win_over_keys_in_old_rows():
    _database()
    exc = next(iter(get_node(database="db", code="0").technosphere()))
    stale = {**exc._data, "input": ("db", "gone"), "output": ("elsewhere", "0")}
    sqlite3_lci_db.execute_sql(
        "UPDATE exchangedataset SET data = ? WHERE id = ?",
        (pickle.dumps(stale, protocol=4), exc.id),
    )
    assert ExchangeDataset.get_by_id(exc.id).data["input"] == ("db", "1")
    assert ExchangeDataset.get_by_id(exc.id).data["output"] == ("db", "0")
    assert next(iter(get_node(database="db", code="0").technosphere()))["input"] == ("db", "1")


@bw2test
def test_changed_code_reaches_edges():
    _database()
    node = get_node(database="db", code="1")
    node["code"] = "renamed"
    node.save()
    edges = list(get_node(database="db", code="0").technosphere())
    assert [edge["input"] for edge in edges] == [("db", "renamed")]


@bw2test
def test_revisions_keep_keys_in_edge_data():
    _database()
    exc = next(iter(get_node(database="db", code="0").technosphere()))
    state = RevisionedEdge.current_state_as_dict(ExchangeDataset.get_by_id(exc.id))
    assert state["data"]["input"] == ("db", "1")
    assert state["data"]["output"] == ("db", "0")
    assert state["input_code"] == "1"
