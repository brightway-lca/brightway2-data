"""``CompressedJSONField`` and zstd dictionaries for activity and exchange data (storage S3)."""

import datetime
import json
import pickle
import sqlite3
import threading
from decimal import Decimal
from fractions import Fraction

import numpy as np
import pytest

from bw2data import codec, config, get_node, projects
from bw2data.backends import base as backend_base
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.schema import (
    COMPRESSED_COLUMNS,
    ActivityDataset,
    ExchangeDataset,
    ZstdDictionary,
)
from bw2data.database import DatabaseChooser
from bw2data.errors import UnsafePickleError
from bw2data.sqlite import (
    ZSTD_MAGIC,
    UnknownDictionaryError,
    zstd,
    zstd_dictionaries,
)
from bw2data.tests import bw2test

# Repeated in every row, as properties and comments are in ecoinvent
COMMENT = "Calculated from the annual production volume of the market, " * 4


def _dataset(i: int, n: int) -> dict:
    return {
        "name": f"activity {i}",
        "reference product": f"product {i % 17}",
        "unit": "kilogram",
        "location": "CH",
        "type": "process",
        "comment": COMMENT,
        "classifications": [("ISIC rev.4 ecoinvent", "2011:Manufacture of basic chemicals")],
        "exchanges": [
            {
                "input": ("bulk", str((i + j) % n)),
                "amount": 0.5 + i + j,
                "type": "technosphere",
                "uncertainty type": 2,
                "loc": 0.1,
                "scale": 0.2,
                "pedigree": {"reliability": 2, "completeness": 3},
                "comment": COMMENT,
                "properties": {"carbon content": {"amount": 0.5, "unit": "dimensionless"}},
            }
            for j in range(3)
        ],
    }


def _bulk_data(n: int = 600) -> dict:
    return {("bulk", str(i)): _dataset(i, n) for i in range(n)}


def _raw(table: str, row_id: int) -> bytes:
    return bytes(
        sqlite3_lci_db.execute_sql(f"SELECT data FROM {table} WHERE id = ?", (row_id,)).fetchone()[
            0
        ]
    )


def _dictionary_id(raw: bytes) -> int:
    assert raw[:4] == ZSTD_MAGIC
    return zstd.get_frame_info(raw).dictionary_id


def _store_pickle(table: str, row_id: int, value) -> None:
    sqlite3_lci_db.execute_sql(
        f"UPDATE {table} SET data = ? WHERE id = ?", (pickle.dumps(value, protocol=4), row_id)
    )


def _store():
    return zstd_dictionaries(sqlite3_lci_db.db)


def _small_database():
    db = DatabaseChooser("small")
    db.write(
        {
            ("small", "a"): {
                "name": "a",
                "type": "process",
                "exchanges": [{"input": ("small", "b"), "amount": 2.0, "type": "technosphere"}],
            },
            ("small", "b"): {"name": "b", "type": "process"},
        },
        process=False,
    )
    return db


@bw2test
def test_values_are_stored_as_zstd_frames_and_keep_their_types():
    db = DatabaseChooser("db")
    db.register()
    value = {
        "tuple": ("a", 1),
        "set": {1, 2},
        "date": datetime.date(2026, 11, 23),
        "decimal": Decimal("1.10"),
        "int keys": {1: "a", (2, 3): "b"},
        "nan": float("nan"),
        "big": 2**70,
        "array": np.arange(3),
        "numpy scalar": np.float64(1.5),
    }
    node = db.new_node(code="a", name="a", type="process", **value)
    node.save()

    raw = _raw("activitydataset", node.id)
    assert raw[:4] == ZSTD_MAGIC
    assert b"activitydataset" not in raw

    data = get_node(id=node.id)._data
    assert data["tuple"] == ("a", 1)
    assert data["set"] == {1, 2}
    assert data["date"] == datetime.date(2026, 11, 23)
    assert data["decimal"] == Decimal("1.10")
    assert data["int keys"] == {1: "a", (2, 3): "b"}
    assert data["nan"] != data["nan"]
    assert data["big"] == 2**70
    assert np.array_equal(data["array"], np.arange(3))
    assert type(data["numpy scalar"]) is float


@bw2test
def test_unstorable_value_raises_type_error_naming_the_key():
    db = DatabaseChooser("db")
    db.register()
    node = db.new_node(code="a", name="a", type="process", thing=object())
    with pytest.raises(TypeError, match=r"\['thing'\]"):
        node.save()


@bw2test
def test_small_write_without_dictionary():
    db = _small_database()
    assert ZstdDictionary.select().count() == 0
    for act in ActivityDataset.select():
        assert _dictionary_id(_raw("activitydataset", act.id)) == 0
    node = get_node(database="small", code="a")
    assert [exc.input.key for exc in node.technosphere()] == [("small", "b")]
    assert db.load()[("small", "a")]["exchanges"][0]["amount"] == 2.0


@bw2test
def test_bulk_write_trains_and_uses_a_dictionary():
    data = _bulk_data()
    DatabaseChooser("bulk").write(data, process=False)

    (dictionary,) = ZstdDictionary.select()
    assert dictionary.active
    assert _store().active_id() == dictionary.id
    for model, table in ((ActivityDataset, "activitydataset"), (ExchangeDataset, "exchangedataset")):
        ids = {_dictionary_id(_raw(table, row.id)) for row in model.select(model.id)}
        assert ids == {dictionary.id}

    loaded = DatabaseChooser("bulk").load()
    assert loaded[("bulk", "5")]["classifications"] == data[("bulk", "5")]["classifications"]
    assert loaded[("bulk", "5")]["exchanges"][0]["pedigree"] == {"reliability": 2, "completeness": 3}


@bw2test
def test_dictionary_makes_rows_smaller():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    with_dictionary = sum(len(_raw("exchangedataset", e.id)) for e in ExchangeDataset.select())
    plain = zstd.ZstdCompressor(level=3)
    without = sum(
        len(plain.compress(codec.dumps(e.data), mode=zstd.ZstdCompressor.FLUSH_FRAME))
        for e in ExchangeDataset.select()
    )
    assert with_dictionary < without / 2


@bw2test
def test_rows_keep_decoding_after_a_new_dictionary_is_trained(monkeypatch):
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    first = _store().active_id()

    other = {
        ("other", str(i)): {**_dataset(i, 600), "comment": f"Different text {i % 7} " * 20}
        for i in range(600)
    }
    for ds in other.values():
        for exc in ds["exchanges"]:
            exc["input"] = ("other", exc["input"][1])
    DatabaseChooser("other").write(other, process=False)
    second = _store().active_id()

    assert first != second
    assert {d.id for d in ZstdDictionary.select()} == {first, second}
    assert ZstdDictionary.get_by_id(first).active is False

    # A fresh store, as after switching projects, reads both dictionaries from the file
    del sqlite3_lci_db.db._bw_zstd_dictionaries
    assert len(DatabaseChooser("bulk").load()) == 600
    assert len(DatabaseChooser("other").load()) == 600
    node = get_node(database="bulk", code="1")
    assert _dictionary_id(_raw("activitydataset", node.id)) == first
    node["name"] = "changed"
    node.save()
    assert _dictionary_id(_raw("activitydataset", node.id)) == second
    assert get_node(id=node.id)["name"] == "changed"


@bw2test
def test_pickled_and_compressed_rows_side_by_side():
    db = _small_database()
    node = get_node(database="small", code="a")
    exc = next(iter(node.technosphere()))
    legacy = {**node._data, "when": datetime.datetime(2020, 1, 1, 12), "loc": np.float64(0.5)}
    _store_pickle("activitydataset", node.id, legacy)
    _store_pickle("exchangedataset", exc.id, {**exc._data, "loc": np.float64(1.5)})

    assert _raw("activitydataset", node.id)[:1] == b"\x80"
    assert _raw("activitydataset", get_node(database="small", code="b").id)[:4] == ZSTD_MAGIC
    loaded = db.load()
    assert loaded[("small", "a")]["when"] == datetime.datetime(2020, 1, 1, 12)
    assert loaded[("small", "a")]["exchanges"][0]["loc"] == 1.5
    assert loaded[("small", "b")]["name"] == "b"

    # Saving rewrites the row in the new format
    node = get_node(id=node.id)
    node.save()
    assert _raw("activitydataset", node.id)[:4] == ZSTD_MAGIC
    assert get_node(id=node.id)["when"] == datetime.datetime(2020, 1, 1, 12)


@bw2test
def test_pickle_with_untrusted_class_is_refused(monkeypatch):
    _small_database()
    node = get_node(database="small", code="a")
    _store_pickle("activitydataset", node.id, {**node._data, "f": Fraction(1, 3)})
    with pytest.raises(UnsafePickleError, match="fractions.Fraction"):
        get_node(id=node.id)
    monkeypatch.setattr(config, "trusted_pickle_classes", ["fractions.Fraction"])
    assert get_node(id=node.id)["f"] == Fraction(1, 3)


@bw2test
def test_unknown_dictionary_raises():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    dict_id = _store().active_id()
    sqlite3_lci_db.execute_sql("UPDATE zstddictionary SET active = 0")
    sqlite3_lci_db.execute_sql("DELETE FROM zstddictionary")
    del sqlite3_lci_db.db._bw_zstd_dictionaries
    with pytest.raises(UnknownDictionaryError, match=str(dict_id)):
        get_node(database="bulk", code="1")


@bw2test
def test_process_gives_the_same_arrays_as_pickled_rows():
    data = _bulk_data()
    db = DatabaseChooser("bulk")
    db.write(data)
    compressed = db.datapackage()
    arrays = {
        name: compressed.get_resource(name)[0].copy()
        for name in ("bulk_technosphere_matrix.indices", "bulk_technosphere_matrix.data")
    }

    for model, table in ((ActivityDataset, "activitydataset"), (ExchangeDataset, "exchangedataset")):
        for row in model.select():
            _store_pickle(table, row.id, row.data)
    assert _raw("exchangedataset", ExchangeDataset.select().first().id)[:1] == b"\x80"
    db.process()
    pickled = db.datapackage()
    for name, array in arrays.items():
        assert np.array_equal(pickled.get_resource(name)[0], array)


@bw2test
def test_each_thread_gets_its_own_compressor():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    store = _store()
    dict_id = store.active_id()
    field = ActivityDataset.data
    compressors, errors = {}, []

    def work(n):
        try:
            # The dictionary is only used inside a transaction
            with sqlite3_lci_db.atomic():
                for i in range(200):
                    value = {"thread": n, "i": i, "comment": COMMENT, "key": ("bulk", str(i))}
                    raw = bytes(field.db_value(value))
                    assert _dictionary_id(raw) == dict_id
                    assert field.python_value(raw) == value
            compressors[n] = store._local.compressors[dict_id]
        except Exception:  # pragma: no cover - reported below
            import traceback

            errors.append(traceback.format_exc())
        finally:
            sqlite3_lci_db.db.close()

    threads = [threading.Thread(target=work, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len({id(c) for c in compressors.values()}) == 4


@bw2test
def test_dictionary_activated_by_another_connection_is_used():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    first = _store().active_id()
    node = get_node(database="bulk", code="1")
    node.save()

    samples = [json.dumps({"other": i, "text": f"something else {i % 9}" * 9}).encode() for i in range(2000)]
    dictionary = zstd.train_dict(samples, 16_384)
    assert dictionary.dict_id != first
    with sqlite3.connect(sqlite3_lci_db.db.database) as other:
        other.execute("UPDATE zstddictionary SET active = 0")
        other.execute(
            "INSERT INTO zstddictionary (id, content, created, active) VALUES (?, ?, NULL, 1)",
            (dictionary.dict_id, dictionary.dict_content),
        )
    other.close()

    node["name"] = "after"
    node.save()
    assert _dictionary_id(_raw("activitydataset", node.id)) == dictionary.dict_id
    assert get_node(id=node.id)["name"] == "after"


def _other_dictionary(first: int):
    samples = [
        json.dumps({"other": i, "text": f"something else {i % 9}" * 9}).encode()
        for i in range(2000)
    ]
    dictionary = zstd.train_dict(samples, 16_384)
    assert dictionary.dict_id != first
    return dictionary


def _replace_dictionary(connection, dictionary) -> None:
    """What another process does when it writes a database and then vacuums: activate its
    own dictionary and delete the others."""
    connection.execute("BEGIN IMMEDIATE")
    connection.execute("DELETE FROM zstddictionary")
    connection.execute(
        "INSERT INTO zstddictionary (id, content, created, active) VALUES (?, ?, NULL, 1)",
        (dictionary.dict_id, dictionary.dict_content),
    )
    connection.execute("COMMIT")


@bw2test
def test_no_dictionary_outside_of_a_transaction():
    # Such a value can be stored at any later time, when another process may have removed
    # the dictionary which was active when it was compressed
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    store = _store()
    dict_id = store.active_id()
    field = ActivityDataset.data
    value = {"comment": COMMENT, "key": ("bulk", "1")}

    assert store.write_id() == 0
    raw = bytes(field.db_value(value))
    assert _dictionary_id(raw) == 0
    assert field.python_value(raw) == value

    with sqlite3_lci_db.atomic():
        assert store.write_id() == dict_id
        assert _dictionary_id(bytes(field.db_value(value))) == dict_id
    assert store.write_id() == 0

    # A bare ``begin()``, as in ``Database.write()``, is a transaction as well
    sqlite3_lci_db.db.begin()
    try:
        assert store.write_id() == dict_id
    finally:
        sqlite3_lci_db.db.rollback()
    assert store.write_id() == 0

    # Rows created through the ORM without a transaction: no dictionary, but readable
    node = get_node(database="bulk", code="1")
    ActivityDataset.update(data=value).where(ActivityDataset.id == node.id).execute()
    assert _dictionary_id(_raw("activitydataset", node.id)) == 0
    assert ActivityDataset.get_by_id(node.id).data == value


@bw2test
def test_save_uses_the_dictionary_and_keeps_the_callers_transaction():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    dict_id = _store().active_id()

    node = get_node(database="bulk", code="1")
    node["name"] = "on its own"
    node.save()
    assert not sqlite3_lci_db.db.in_transaction()
    assert sqlite3_lci_db.db.transaction_token() is None
    assert _dictionary_id(_raw("activitydataset", node.id)) == dict_id

    exc = next(iter(node.technosphere()))
    with sqlite3_lci_db.atomic() as txn:
        token = sqlite3_lci_db.db.transaction_token()
        exc["amount"] = 42.0
        exc.save()
        # Nothing was committed or begun by ``save``
        assert sqlite3_lci_db.db.transaction_token() is token
        assert _dictionary_id(_raw("exchangedataset", exc.id)) == dict_id
        txn.rollback()
    assert next(iter(get_node(id=node.id).technosphere()))["amount"] != 42.0


@bw2test
def test_dictionary_used_in_a_transaction_cannot_be_removed_before_it_ends():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    first = _store().active_id()
    dictionary = _other_dictionary(first)
    node = get_node(database="bulk", code="1")
    # Leaves only this row to use the first dictionary
    ActivityDataset.delete().where(ActivityDataset.id != node.id).execute()
    ExchangeDataset.delete().execute()

    other = sqlite3.connect(sqlite3_lci_db.db.database, isolation_level=None, timeout=0)
    try:
        with sqlite3_lci_db.atomic():
            raw = bytes(ActivityDataset.data.db_value({"name": "pending", "comment": COMMENT}))
            assert _dictionary_id(raw) == first
            # The value is compressed but not stored yet. The other process can't commit
            # the removal of its dictionary now.
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                _replace_dictionary(other, dictionary)
            other.execute("ROLLBACK")
            sqlite3_lci_db.execute_sql(
                "UPDATE activitydataset SET data = ? WHERE id = ?", (raw, node.id)
            )
        # Afterwards the row is there, so the dictionary is in use and kept
        assert _store().remove_unused(COMPRESSED_COLUMNS) == []
    finally:
        other.close()
    assert get_node(id=node.id)["name"] == "pending"


@bw2test
def test_dictionary_is_read_again_after_commit_or_rollback_inside_a_transaction():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    store = _store()
    first = store.active_id()
    dictionary = _other_dictionary(first)
    ActivityDataset.delete().execute()
    ExchangeDataset.delete().execute()

    other = sqlite3.connect(sqlite3_lci_db.db.database, isolation_level=None, timeout=0)
    try:
        with sqlite3_lci_db.atomic() as txn:
            assert store.write_id() == first
            token = sqlite3_lci_db.db.transaction_token()
            # Ends the transaction and begins a new one, which holds no lock yet
            txn.rollback()
            assert sqlite3_lci_db.db.transaction_token() is not token
            _replace_dictionary(other, dictionary)
            assert store.write_id() == dictionary.dict_id
            with sqlite3_lci_db.atomic():
                # A savepoint is part of the same transaction
                assert store.write_id() == dictionary.dict_id
    finally:
        other.close()
    assert store.ids() == [dictionary.dict_id]


@bw2test
def test_add_dictionary_refuses_transactions_and_clashing_ids():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    store = _store()
    stored = zstd.ZstdDict(ZstdDictionary.get().content)
    with sqlite3_lci_db.atomic():
        with pytest.raises(RuntimeError, match="transaction"):
            store.add(stored)
    sqlite3_lci_db.db.begin()
    try:
        with pytest.raises(RuntimeError, match="transaction"):
            store.add(stored)
    finally:
        sqlite3_lci_db.db.rollback()
    # Adding the same dictionary again only activates it
    assert store.add(stored) == stored.dict_id
    assert ZstdDictionary.select().count() == 1

    content = bytearray(stored.dict_content)
    content[-1] ^= 0xFF
    with pytest.raises(ValueError, match="different zstd dictionary"):
        store.add(zstd.ZstdDict(bytes(content)))


@bw2test
def test_only_one_active_dictionary():
    from peewee import IntegrityError

    ZstdDictionary.create(id=1, content=b"x", active=True)
    with pytest.raises(IntegrityError):
        ZstdDictionary.create(id=2, content=b"y", active=True)
    ZstdDictionary.create(id=3, content=b"z", active=False)


@bw2test
def test_training_needs_enough_samples():
    assert _store().train([b"a", b"b", b"c"]) is None
    assert _store().train([]) is None
    assert _store().train([b"", b""]) is None
    assert ZstdDictionary.select().count() == 0


@bw2test
def test_write_with_only_unstorable_datasets_raises_type_error_naming_the_key():
    # No sample is left to train a dictionary on; the error must be the one of the write
    data = _bulk_data()
    for ds in data.values():
        ds["thing"] = object()
        for exc in ds["exchanges"]:
            exc["thing"] = object()
    with pytest.raises(TypeError, match=r"\['thing'\]"):
        DatabaseChooser("bulk").write(data, process=False)
    assert ZstdDictionary.select().count() == 0
    assert ActivityDataset.select().count() == 0


@bw2test
def test_training_is_skipped_for_small_writes(monkeypatch):
    monkeypatch.setattr(backend_base, "DICTIONARY_MIN_DATASETS", 601)
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    assert ZstdDictionary.select().count() == 0


@bw2test
@pytest.mark.parametrize("kind", ["tuple", "dict view"])
def test_training_accepts_exchanges_which_are_not_a_list(kind):
    # Writing only iterates over the exchanges (more than once, so not an iterator);
    # training must not need more than that
    data = _bulk_data()
    for ds in data.values():
        exchanges = ds["exchanges"]
        if kind == "tuple":
            ds["exchanges"] = tuple(exchanges)
        else:
            ds["exchanges"] = dict(enumerate(exchanges)).values()
    DatabaseChooser("bulk").write(data, process=False)

    assert ZstdDictionary.select().count() == 1
    assert ActivityDataset.select().count() == 600
    assert ExchangeDataset.select().count() == 1800


@bw2test
def test_training_ignores_unstorable_samples():
    data = _bulk_data()
    data[("bulk", "0")]["exchanges"][0]["bad"] = object()
    with pytest.raises(TypeError):
        DatabaseChooser("bulk").write(data, process=False)
    # The dictionary was trained and stored before the write failed
    assert ZstdDictionary.select().count() == 1
    assert ActivityDataset.select().count() == 0


@bw2test
def test_remove_unused_dictionaries_before_vacuum(monkeypatch):
    # Otherwise the second write vacuums, which already removes the first dictionary
    monkeypatch.setattr(config, "vacuum", False)
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    first = _store().active_id()
    other = {key: {**ds, "comment": "x" * 300} for key, ds in _bulk_data().items()}
    DatabaseChooser("bulk").write(other, process=False)
    second = _store().active_id()
    assert {d.id for d in ZstdDictionary.select()} == {first, second}

    assert _store().remove_unused(COMPRESSED_COLUMNS) == [first]
    assert [d.id for d in ZstdDictionary.select()] == [second]

    # The active dictionary is kept even when nothing uses it
    DatabaseChooser("bulk").delete(warn=False)
    assert _store().remove_unused(COMPRESSED_COLUMNS) == []

    # ``vacuum`` removes unused dictionaries first
    ZstdDictionary.update(active=False).execute()
    del sqlite3_lci_db.db._bw_zstd_dictionaries
    _small_database()
    monkeypatch.setattr(config, "vacuum", True)
    assert sqlite3_lci_db.vacuum()
    assert ZstdDictionary.select().count() == 0
    assert get_node(database="small", code="a")["name"] == "a"


@bw2test
def test_dictionaries_belong_to_their_project():
    DatabaseChooser("bulk").write(_bulk_data(), process=False)
    first = _store().active_id()
    projects.set_current("another one")
    assert _store().active_id() == 0
    _small_database()
    assert _dictionary_id(_raw("activitydataset", ActivityDataset.select().first().id)) == 0
    assert first not in _store().ids()
