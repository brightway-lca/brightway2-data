"""Tests for the SQL of the read-only extension overlay (``bw2data.backends.extensions``).

Runs on plain ``sqlite3`` connections with small files; nothing here goes through bw2data's
project machinery."""

import json
import pickle
import sqlite3

import pytest
from peewee import SqliteDatabase

from bw2data.backends.extensions import (
    DUPLICATE_ID,
    EXTENSION_DATABASE_NAME,
    EXTENSION_ERROR_PREFIX,
    NOT_LOCAL_STATE,
    READ_ONLY_DATABASE,
    REGISTRY_TABLE,
    drop_overlay_sql,
    overlay_sql,
    view_name,
)
from bw2data.backends.schema import (
    ActivityDataset,
    DatabaseRecord,
    ExchangeDataset,
    ExtensionDatabaseState,
)
from bw2data.sqlite import _sqlite_uri
from bw2data.tests import bw2test

MODELS = [ActivityDataset, ExchangeDataset, DatabaseRecord, ExtensionDatabaseState]
INDICES = [
    'CREATE UNIQUE INDEX "activitydataset_key" ON "activitydataset" ("database", "code")',
    'CREATE INDEX "exchangedataset_input" ON "exchangedataset" ("input_database", "input_code")',
    'CREATE INDEX "exchangedataset_output" ON "exchangedataset" ("output_database", "output_code")',
]
BLOB = pickle.dumps({}, protocol=4)

ACTIVITIES = "lci_activitydataset"
EXCHANGES = "lci_exchangedataset"
RECORDS = "lci_databaserecord"


def make_file(path, activities=(), exchanges=(), records=()):
    """A file with the bw2data LCI schema, as peewee creates it, and the given rows.

    ``activities``: ``(id, database, code)``; ``exchanges``: ``(id, input key, output key)``;
    ``records``: dicts of ``DatabaseRecord`` columns."""
    db = SqliteDatabase(str(path))
    with db.bind_ctx(MODELS):
        db.create_tables(MODELS)
    db.close()
    conn = sqlite3.connect(path)
    for sql in INDICES:
        conn.execute(sql)
    conn.executemany(
        "INSERT INTO activitydataset (id, data, database, code, name, type) "
        "VALUES (?, ?, ?, ?, ?, 'process')",
        [(i, BLOB, db_, code, code) for i, db_, code in activities],
    )
    conn.executemany(
        "INSERT INTO exchangedataset (id, data, input_database, input_code, output_database, "
        "output_code, type) VALUES (?, ?, ?, ?, ?, ?, 'technosphere')",
        [(i, BLOB, *inp, *out) for i, inp, out in exchanges],
    )
    for record in records:
        columns = ", ".join(record)
        conn.execute(
            f"INSERT INTO databaserecord ({columns}) VALUES ({', '.join('?' * len(record))})",
            list(record.values()),
        )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def files(tmp_path):
    """``main`` with database ``mine``; ``ext_1`` with ``bio``; ``ext_2`` with ``inv``, whose
    exchanges link to ``bio``."""
    main = make_file(
        tmp_path / "main.db",
        activities=[(1, "mine", "a"), (2, "mine", "b")],
        exchanges=[(11, ("inv", "x"), ("mine", "a")), (12, ("mine", "b"), ("mine", "a"))],
        records=[{"name": "mine", "backend": "sqlite", "depends": '["inv"]'}],
    )
    bio = make_file(
        tmp_path / "bio ext#1.db",
        activities=[(101, "bio", "co2"), (102, "bio", "ch4")],
        records=[
            {
                "name": "bio",
                "backend": "sqlite",
                "depends": "[]",
                "number": 2,
                "searchable": 1,
                # Indented, as JSONField writes it
                "extra": json.dumps({"format": "test", "nested": {"a": [1, 2]}}, indent=2),
            }
        ],
    )
    inv = make_file(
        tmp_path / "inv.db",
        activities=[(201, "inv", "x"), (202, "inv", "y")],
        exchanges=[
            (211, ("bio", "co2"), ("inv", "x")),
            (212, ("inv", "y"), ("inv", "x")),
        ],
        records=[{"name": "inv", "backend": "sqlite", "depends": '["bio"]'}],
    )
    return main, bio, inv


def connect(main, *extensions, foreign_keys=True):
    conn = sqlite3.connect(_sqlite_uri(main), uri=True, isolation_level=None)
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys = 1")
    schemas = []
    for number, path in enumerate(extensions, start=1):
        schema = f"ext_{number}"
        conn.execute("ATTACH DATABASE ? AS " + schema, (_sqlite_uri(path, mode="ro"),))
        schemas.append(schema)
    if schemas:
        for statement in overlay_sql(schemas):
            conn.execute(statement)
    return conn


@pytest.fixture
def conn(files):
    main, bio, inv = files
    connection = connect(main, bio, inv)
    yield connection
    connection.close()


def refused(conn, sql, params=(), message=None):
    with pytest.raises(sqlite3.IntegrityError) as excinfo:
        conn.execute(sql, params)
    assert str(excinfo.value).startswith(EXTENSION_ERROR_PREFIX)
    if message is not None:
        assert str(excinfo.value) == message


def ids(conn, table):
    return sorted(row[0] for row in conn.execute(f"SELECT id FROM {table}"))


def record(conn, name, table=RECORDS):
    row = conn.execute(
        f"SELECT name, backend, dirty, searchable, modified, extra FROM {table} WHERE name = ?",
        (name,),
    ).fetchone()
    if row is None:
        return None
    name, backend, dirty, searchable, modified, extra = row
    return {
        "backend": backend,
        "dirty": dirty,
        "searchable": searchable,
        "modified": modified,
        "extra": None if extra is None else json.loads(extra),
    }


def insert_activity(conn, id_, database, code, table=ACTIVITIES):
    conn.execute(
        f"INSERT INTO {table} (id, data, database, code, type) VALUES (?, ?, ?, ?, 'process')",
        (id_, BLOB, database, code),
    )


def insert_exchange(conn, id_, inp, out, table=EXCHANGES):
    conn.execute(
        f"INSERT INTO {table} (id, data, input_database, input_code, output_database, "
        "output_code, type) VALUES (?, ?, ?, ?, ?, ?, 'technosphere')",
        (id_, BLOB, *inp, *out),
    )


# Reading


def test_view_names():
    assert view_name(ActivityDataset) == ACTIVITIES
    assert view_name(ExchangeDataset) == EXCHANGES
    assert view_name(DatabaseRecord) == RECORDS


def test_pinned_table_names():
    assert DatabaseRecord._meta.table_name == "databaserecord"
    assert ExtensionDatabaseState._meta.table_name == "extensiondatabasestate"


def test_reads_see_all_files(conn):
    assert ids(conn, ACTIVITIES) == [1, 2, 101, 102, 201, 202]
    assert ids(conn, EXCHANGES) == [11, 12, 211, 212]
    assert sorted(r[0] for r in conn.execute(f"SELECT name FROM {RECORDS}")) == [
        "bio",
        "inv",
        "mine",
    ]
    row = conn.execute(
        f"SELECT id FROM {ACTIVITIES} WHERE database = 'bio' AND code = 'co2'"
    ).fetchone()
    assert row == (101,)


def test_registry(conn):
    rows = sorted(conn.execute(f"SELECT database, schema FROM temp.{REGISTRY_TABLE}"))
    assert rows == [("bio", "ext_1"), ("inv", "ext_2")]


def test_registry_includes_databases_without_record(files, tmp_path):
    main, _, _ = files
    orphan = make_file(tmp_path / "orphan.db", activities=[(301, "orphan", "z")])
    conn = connect(main, orphan)
    assert list(conn.execute(f"SELECT database FROM temp.{REGISTRY_TABLE}")) == [("orphan",)]
    refused(conn, f"DELETE FROM {ACTIVITIES} WHERE id = 301", message=READ_ONLY_DATABASE)


def test_edges_across_files(conn):
    # main -> ext_2 -> ext_1, joined through the views
    rows = conn.execute(
        f"SELECT a.database, a.code FROM {EXCHANGES} e JOIN {ACTIVITIES} a "
        "ON a.database = e.input_database AND a.code = e.input_code "
        "WHERE e.output_database = 'inv' AND e.output_code = 'x' ORDER BY a.code"
    ).fetchall()
    assert rows == [("bio", "co2"), ("inv", "y")]


def test_overlay_creates_nothing_in_main(files):
    main, bio, inv = files
    before = sqlite3.connect(main).execute("SELECT * FROM sqlite_master").fetchall()
    conn = connect(main, bio, inv)
    insert_activity(conn, 3, "mine", "c")
    conn.close()
    after = sqlite3.connect(main).execute("SELECT * FROM sqlite_master").fetchall()
    assert before == after


def test_overlay_can_be_run_again(files):
    main, bio, inv = files
    conn = connect(main, bio)
    assert ids(conn, ACTIVITIES) == [1, 2, 101, 102]
    conn.execute("ATTACH DATABASE ? AS ext_2", (_sqlite_uri(inv, mode="ro"),))
    for statement in overlay_sql(["ext_1", "ext_2"]):
        conn.execute(statement)
    assert ids(conn, ACTIVITIES) == [1, 2, 101, 102, 201, 202]


def test_drop_overlay(conn):
    for statement in drop_overlay_sql():
        conn.execute(statement)
    temp = conn.execute("SELECT name FROM temp.sqlite_master").fetchall()
    assert temp == []
    # Safe to run twice
    for statement in drop_overlay_sql():
        conn.execute(statement)


@pytest.mark.parametrize(
    "schemas",
    [[], ["main"], ["TEMP"], ["ext 1"], ['ext_1"; DROP TABLE x; --'], ["ext_1", "ext_1"], [1]],
)
def test_invalid_schemas(schemas):
    with pytest.raises(ValueError):
        overlay_sql(schemas)


# Activities


def test_insert_activity_goes_to_main(conn):
    insert_activity(conn, 3, "mine", "c")
    assert ids(conn, "main.activitydataset") == [1, 2, 3]
    assert 3 in ids(conn, ACTIVITIES)


def test_insert_activity_executemany(conn):
    # As ``bw2data.backends.schema._insert_many`` does
    conn.executemany(
        f"INSERT INTO {ACTIVITIES} (id, data, database, code, type) VALUES (?, ?, ?, ?, ?)",
        [(i, BLOB, "new", str(i), "process") for i in range(1000, 1200)],
    )
    assert conn.execute("SELECT COUNT(*) FROM main.activitydataset").fetchone() == (202,)


def test_insert_or_replace_activity(conn):
    # The conflict policy of the outer statement applies to the trigger's insert
    conn.execute(
        f"INSERT OR REPLACE INTO {ACTIVITIES} (id, data, database, code, name, type) "
        "VALUES (1, ?, 'mine', 'a', 'renamed', 'process')",
        (BLOB,),
    )
    assert conn.execute("SELECT name FROM main.activitydataset WHERE id = 1").fetchone() == (
        "renamed",
    )


def test_insert_activity_in_extension_database_refused(conn):
    with pytest.raises(sqlite3.IntegrityError, match=EXTENSION_DATABASE_NAME):
        insert_activity(conn, 3, "bio", "new")
    assert ids(conn, "main.activitydataset") == [1, 2]


def test_insert_activity_with_extension_id_refused(conn):
    with pytest.raises(sqlite3.IntegrityError, match=DUPLICATE_ID):
        insert_activity(conn, 201, "mine", "c")


def test_update_main_activity(conn):
    conn.execute(f"UPDATE {ACTIVITIES} SET name = 'new name', code = 'aa' WHERE id = 1")
    assert conn.execute("SELECT name, code FROM main.activitydataset WHERE id = 1").fetchone() == (
        "new name",
        "aa",
    )


def test_rename_main_database(conn):
    conn.execute(f"UPDATE {ACTIVITIES} SET database = 'renamed' WHERE database = 'mine'")
    assert conn.execute("SELECT DISTINCT database FROM main.activitydataset").fetchall() == [
        ("renamed",)
    ]


def test_update_extension_activity_refused(conn):
    refused(conn, f"UPDATE {ACTIVITIES} SET name = 'x' WHERE id = 101", message=READ_ONLY_DATABASE)
    refused(
        conn,
        f"UPDATE {ACTIVITIES} SET database = 'mine' WHERE database = 'bio'",
        message=READ_ONLY_DATABASE,
    )


def test_move_activity_into_extension_database_refused(conn):
    refused(
        conn, f"UPDATE {ACTIVITIES} SET database = 'inv' WHERE id = 1", message=EXTENSION_DATABASE_NAME
    )


def test_change_activity_id_to_extension_id_refused(conn):
    refused(conn, f"UPDATE {ACTIVITIES} SET id = 102 WHERE id = 1", message=DUPLICATE_ID)


def test_delete_main_activities(conn):
    conn.execute(f"DELETE FROM {ACTIVITIES} WHERE database = 'mine'")
    assert ids(conn, ACTIVITIES) == [101, 102, 201, 202]


def test_delete_extension_activity_refused(conn):
    refused(conn, f"DELETE FROM {ACTIVITIES} WHERE id = 202", message=READ_ONLY_DATABASE)
    # A statement which touches rows in both files changes nothing
    conn.execute("BEGIN")
    refused(conn, f"DELETE FROM {ACTIVITIES}")
    conn.execute("ROLLBACK")
    assert ids(conn, ACTIVITIES) == [1, 2, 101, 102, 201, 202]


def test_raw_write_to_extension_refused_by_sqlite(conn):
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        conn.execute("DELETE FROM ext_1.activitydataset")
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        conn.execute("UPDATE ext_2.databaserecord SET backend = 'x'")


# Exchanges


def test_insert_exchange_into_extension_node_allowed(conn):
    # An editable node links to an extension node: the cross-file edge
    insert_exchange(conn, 13, ("bio", "co2"), ("mine", "b"))
    assert ids(conn, "main.exchangedataset") == [11, 12, 13]


def test_insert_exchange_from_extension_node_refused(conn):
    with pytest.raises(sqlite3.IntegrityError, match=EXTENSION_DATABASE_NAME):
        insert_exchange(conn, 13, ("mine", "a"), ("inv", "x"))


def test_insert_exchange_with_extension_id_refused(conn):
    with pytest.raises(sqlite3.IntegrityError, match=DUPLICATE_ID):
        insert_exchange(conn, 211, ("mine", "a"), ("mine", "b"))


def test_update_exchanges(conn):
    conn.execute(f"UPDATE {EXCHANGES} SET input_code = 'y' WHERE id = 11")
    refused(conn, f"UPDATE {EXCHANGES} SET input_code = 'ch4' WHERE id = 211", message=READ_ONLY_DATABASE)
    refused(
        conn,
        f"UPDATE {EXCHANGES} SET output_database = 'inv' WHERE id = 12",
        message=EXTENSION_DATABASE_NAME,
    )
    assert conn.execute("SELECT input_code FROM main.exchangedataset WHERE id = 11").fetchone() == (
        "y",
    )


def test_delete_exchanges(conn):
    refused(conn, f"DELETE FROM {EXCHANGES} WHERE id = 212", message=READ_ONLY_DATABASE)
    # Edges from main into an extension are main's own
    conn.execute(f"DELETE FROM {EXCHANGES} WHERE input_database = 'inv' AND output_database = 'mine'")
    assert ids(conn, EXCHANGES) == [12, 211, 212]


# Database records


def test_insert_record(conn):
    conn.execute(f"INSERT INTO {RECORDS} (name, backend) VALUES ('other', 'sqlite')")
    assert record(conn, "other", "main.databaserecord")["backend"] == "sqlite"


def test_insert_record_with_extension_name_refused(conn):
    refused(
        conn, f"INSERT INTO {RECORDS} (name) VALUES ('bio')", message=EXTENSION_DATABASE_NAME
    )


def test_replace_record(conn):
    # ``databases[name] = {...}`` uses ``DatabaseRecord.replace``
    conn.execute(f"REPLACE INTO {RECORDS} (name, backend) VALUES ('mine', 'iotable')")
    assert record(conn, "mine")["backend"] == "iotable"
    refused(
        conn,
        f"REPLACE INTO {RECORDS} (name, backend) VALUES ('bio', 'iotable')",
        message=EXTENSION_DATABASE_NAME,
    )


def test_update_main_record(conn):
    conn.execute(
        f"UPDATE {RECORDS} SET backend = 'iotable', extra = ? WHERE name = 'mine'",
        (json.dumps({"anything": 1}),),
    )
    assert record(conn, "mine") == {
        "backend": "iotable",
        "dirty": None,
        "searchable": None,
        "modified": None,
        "extra": {"anything": 1},
    }
    refused(conn, f"UPDATE {RECORDS} SET name = 'bio' WHERE name = 'mine'", message=EXTENSION_DATABASE_NAME)


def test_extension_record_local_state(conn):
    extra = record(conn, "bio")["extra"]
    extra["processed"] = "2026-11-22T10:00:00"
    conn.execute(
        f"UPDATE {RECORDS} SET dirty = 1, searchable = 0, modified = 'now', extra = ? "
        "WHERE name = 'bio'",
        (json.dumps(extra),),
    )
    assert record(conn, "bio") == {
        "backend": "sqlite",
        "dirty": 1,
        "searchable": 0,
        "modified": "now",
        "extra": {"format": "test", "nested": {"a": [1, 2]}, "processed": "2026-11-22T10:00:00"},
    }
    # Stored in the project, with only the local keys of ``extra``
    state = conn.execute(
        "SELECT name, dirty, searchable, modified, extra FROM main.extensiondatabasestate"
    ).fetchall()
    assert [s[:4] for s in state] == [("bio", 1, 0, "now")]
    assert json.loads(state[0][4]) == {"processed": "2026-11-22T10:00:00"}
    # The extension's own row is unchanged
    assert record(conn, "bio", "ext_1.databaserecord")["dirty"] is None


def test_extension_record_local_state_updated_again(conn):
    conn.execute(f"UPDATE {RECORDS} SET dirty = 1 WHERE name = 'bio'")
    conn.execute(f"UPDATE {RECORDS} SET dirty = 0 WHERE name = 'bio'")
    assert record(conn, "bio")["dirty"] == 0
    assert conn.execute("SELECT COUNT(*) FROM main.extensiondatabasestate").fetchone() == (1,)


def test_extension_record_processed_removed(conn):
    extra = record(conn, "bio")["extra"]
    conn.execute(
        f"UPDATE {RECORDS} SET extra = ? WHERE name = 'bio'",
        (json.dumps({**extra, "processed": "then"}),),
    )
    conn.execute(f"UPDATE {RECORDS} SET extra = ? WHERE name = 'bio'", (json.dumps(extra),))
    assert record(conn, "bio")["extra"] == {"format": "test", "nested": {"a": [1, 2]}}


def test_extension_record_extra_formatting_and_order_ignored(conn):
    # Same content as stored in the extension, but minified and in another key order
    extra = json.dumps({"processed": "x", "nested": {"a": [1, 2]}, "format": "test"})
    conn.execute(f"UPDATE {RECORDS} SET extra = ? WHERE name = 'bio'", (extra,))
    assert record(conn, "bio")["extra"]["processed"] == "x"


def test_extension_record_without_extra(conn):
    conn.execute(
        f"UPDATE {RECORDS} SET extra = ? WHERE name = 'inv'", (json.dumps({"processed": "x"}),)
    )
    assert record(conn, "inv")["extra"] == {"processed": "x"}


@pytest.mark.parametrize(
    "assignment, params",
    [
        ("backend = 'iotable'", ()),
        ("number = 5", ()),
        ("version = 3", ()),
        ("depends = '[\"other\"]'", ()),
        ("name = 'renamed'", ()),
        ("extra = ?", (json.dumps({"format": "changed", "nested": {"a": [1, 2]}}),)),
        ("extra = ?", (json.dumps({"format": "test", "nested": {"a": [1, 2]}, "new": 1}),)),
        ("extra = ?", (json.dumps({"format": "test"}),)),
        ("extra = NULL", ()),
    ],
)
def test_extension_record_other_changes_refused(conn, assignment, params):
    refused(
        conn, f"UPDATE {RECORDS} SET {assignment} WHERE name = 'bio'", params, NOT_LOCAL_STATE
    )
    assert conn.execute("SELECT COUNT(*) FROM main.extensiondatabasestate").fetchone() == (0,)


def test_extension_record_same_json_allowed(conn):
    # Writing back what was read, as a metadata save of only local fields does
    conn.execute(f"UPDATE {RECORDS} SET depends = '[ ]', searchable = 0 WHERE name = 'bio'")
    assert record(conn, "bio")["searchable"] == 0


def test_delete_records(conn):
    refused(conn, f"DELETE FROM {RECORDS} WHERE name = 'inv'", message=READ_ONLY_DATABASE)
    conn.execute(f"DELETE FROM {RECORDS} WHERE name = 'mine'")
    assert record(conn, "mine") is None


# Indices and query plans


def plan(conn, sql):
    return " | ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql))


@pytest.mark.parametrize(
    "sql, index",
    [
        (f"SELECT * FROM {ACTIVITIES} WHERE database = 'x' AND code = 'y'", "activitydataset_key"),
        (
            f"SELECT * FROM {EXCHANGES} WHERE input_database = 'x' AND input_code = 'y'",
            "exchangedataset_input",
        ),
        (
            f"SELECT * FROM {EXCHANGES} WHERE output_database = 'x' AND output_code = 'y'",
            "exchangedataset_output",
        ),
    ],
)
def test_lookups_use_indices_in_every_file(conn, sql, index):
    text = plan(conn, sql)
    assert text.count(f"USING INDEX {index}") == 3, text


def test_id_lookup_uses_primary_keys(conn):
    text = plan(conn, f"SELECT * FROM {ACTIVITIES} WHERE id = 5")
    assert text.count("USING INTEGER PRIMARY KEY") == 3, text


def test_without_foreign_keys(files):
    main, bio, inv = files
    conn = connect(main, bio, inv, foreign_keys=False)
    insert_activity(conn, 3, "mine", "c")
    with pytest.raises(sqlite3.IntegrityError, match=EXTENSION_DATABASE_NAME):
        insert_activity(conn, 4, "bio", "c")


# Project wiring


@bw2test
def test_state_table_created_in_projects():
    from bw2data.backends import sqlite3_lci_db

    tables = {
        row[0]
        for row in sqlite3_lci_db.execute_sql(
            "SELECT name FROM main.sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    assert "extensiondatabasestate" in tables
    assert "databaserecord" in tables
