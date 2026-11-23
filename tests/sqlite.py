import os
import sys
from copy import copy

import pytest

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
        data = expected.pop("data")
        if model is ExchangeDataset:
            # `input` and `output` come from the key columns, as stored
            data = {
                **data,
                "input": (
                    model.input_database.db_value(expected["input_database"]),
                    model.input_code.db_value(expected["input_code"]),
                ),
                "output": (
                    model.output_database.db_value(expected["output_database"]),
                    model.output_code.db_value(expected["output_code"]),
                ),
            }
        assert row.data == data
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


def test_bw_sqlite_database_runs_hooks_on_every_connection(tmp_path):
    import threading

    from bw2data.sqlite import BWSqliteDatabase

    seen = []
    db = BWSqliteDatabase(tmp_path / "hooks.db")
    db.add_connection_hook(lambda conn: seen.append(threading.get_ident()))

    db.connect()
    assert seen == [threading.get_ident()]

    def other_thread():
        db.connect()
        db.execute_sql("SELECT 1")
        db.close()

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join()
    assert len(seen) == 2
    assert seen[1] != seen[0]

    db.close()
    db.connect()
    assert len(seen) == 3
    db.close()


def test_bw_sqlite_database_hook_runs_after_pragmas(tmp_path):
    from bw2data.sqlite import BWSqliteDatabase

    seen = []
    db = BWSqliteDatabase(tmp_path / "hooks.db", pragmas={"foreign_keys": 1})
    db.add_connection_hook(
        lambda conn: seen.append(conn.execute("PRAGMA foreign_keys").fetchone()[0])
    )
    db.connect()
    assert seen == [1]
    db.close()


def test_bw_sqlite_database_remove_hook(tmp_path):
    from bw2data.sqlite import BWSqliteDatabase

    seen = []
    db = BWSqliteDatabase(tmp_path / "hooks.db")
    hook = lambda conn: seen.append(1)  # noqa: E731
    db.add_connection_hook(hook)
    assert db.connection_hooks == (hook,)
    db.remove_connection_hook(hook)
    assert db.connection_hooks == ()
    db.connect()
    assert seen == []
    db.close()


def test_bw_sqlite_database_remove_unregistered_hook_is_noop(tmp_path):
    from bw2data.sqlite import BWSqliteDatabase

    db = BWSqliteDatabase(tmp_path / "hooks.db")
    db.remove_connection_hook(lambda conn: None)
    assert db.connection_hooks == ()


def test_bw_sqlite_database_hooks_from_constructor(tmp_path):
    from bw2data.sqlite import BWSqliteDatabase

    seen = []
    hook = lambda conn: seen.append(1)  # noqa: E731
    db = BWSqliteDatabase(tmp_path / "hooks.db", connection_hooks=[hook])
    assert db.connection_hooks == (hook,)
    db.connect()
    assert seen == [1]
    db.close()


def test_bw_sqlite_database_hook_added_while_connected_runs_immediately(tmp_path):
    from bw2data.sqlite import BWSqliteDatabase

    seen = []
    db = BWSqliteDatabase(tmp_path / "hooks.db")
    db.connect()
    try:
        # The hook must not call ``db.connection()``: on later connections it runs while
        # peewee holds its connection lock
        db.add_connection_hook(seen.append)
        assert seen == [db.connection()]
    finally:
        db.close()
    db.connect()
    assert len(seen) == 2 and seen[1] is db.connection()
    db.close()


def test_bw_sqlite_database_failing_hook_is_not_registered(tmp_path):
    from bw2data.sqlite import BWSqliteDatabase

    def hook(conn):
        raise RuntimeError("nope")

    db = BWSqliteDatabase(tmp_path / "hooks.db")
    db.connect()
    try:
        with pytest.raises(RuntimeError):
            db.add_connection_hook(hook)
        assert db.connection_hooks == ()
    finally:
        db.close()


def test_bw_sqlite_database_deferred_init_keeps_uri_mode(tmp_path):
    from bw2data.sqlite import BWSqliteDatabase

    db = BWSqliteDatabase(None)
    db.init(tmp_path / "deferred.db")
    assert db.connect_params.get("uri") is True
    db.init(tmp_path / "deferred.db", uri=False)
    assert db.connect_params.get("uri") is False


def test_substitutable_database_uses_bw_sqlite_database():
    from bw2data.sqlite import BWSqliteDatabase

    assert isinstance(db.db, BWSqliteDatabase)
    assert db.db.connect_params.get("uri") is True


def test_substitutable_database_hook_applies_to_open_connection_and_survives_change_path(
    tmp_path,
):
    from peewee import Model, TextField

    from bw2data.sqlite import SubstitutableDatabase

    class Thing(Model):
        name = TextField()

    seen = []
    hook = lambda conn: seen.append(  # noqa: E731
        conn.execute("PRAGMA database_list").fetchone()[2]
    )
    sub = SubstitutableDatabase(tmp_path / "first.db", [Thing])
    try:
        # ``SubstitutableDatabase`` connects on creation; the hook must still reach that
        # connection
        sub.add_connection_hook(hook)
        assert sub.connection_hooks == (hook,)
        assert len(seen) == 1 and seen[0].endswith("first.db")

        first_db = sub.db
        sub.change_path(tmp_path / "second.db")
        assert sub.db is not first_db
        assert sub.connection_hooks == (hook,)
        assert len(seen) == 2 and seen[1].endswith("second.db")

        # Hooks registered on the inner database object are carried over as well
        other = lambda conn: seen.append("other")  # noqa: E731
        sub.db.add_connection_hook(other)
        sub.change_path(tmp_path / "third.db")
        assert sub.connection_hooks == (hook, other)

        sub.remove_connection_hook(hook)
        sub.remove_connection_hook(other)
        sub.remove_connection_hook(other)
        assert sub.connection_hooks == ()
    finally:
        sub.db.close()


def test_substitutable_database_hooks_from_constructor_run_on_first_connection(tmp_path):
    from peewee import Model, TextField

    from bw2data.sqlite import SubstitutableDatabase

    class Thing(Model):
        name = TextField()

    seen = []
    sub = SubstitutableDatabase(
        tmp_path / "first.db", [Thing], connection_hooks=[lambda conn: seen.append(1)]
    )
    try:
        assert seen == [1]
    finally:
        sub.db.close()


@pytest.mark.parametrize(
    "dirname",
    [
        "plain",
        "with space",
        "hash#dir",
        pytest.param(
            "q?mark",
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="'?' is not allowed in Windows file names"
            ),
        ),
        "pct%41",
    ],
)
def test_sqlite_uri_escapes_paths(tmp_path, dirname):
    import sqlite3

    from bw2data.sqlite import BWSqliteDatabase, _sqlite_uri

    directory = tmp_path / dirname
    directory.mkdir()
    path = directory / "x.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.execute("INSERT INTO t VALUES (42)")
    conn.close()

    # A plain path is still opened correctly in URI mode
    main = BWSqliteDatabase(path)
    try:
        assert main.execute_sql("SELECT v FROM t").fetchone() == (42,)

        # The escaped URI attaches the same file, read-only
        uri = _sqlite_uri(path, mode="ro")
        assert uri.startswith("file:") and uri.endswith("?mode=ro")
        main.execute_sql("ATTACH ? AS other", (uri,))
        assert main.execute_sql("SELECT v FROM other.t").fetchone() == (42,)
        with pytest.raises(Exception, match="readonly"):
            main.execute_sql("INSERT INTO other.t VALUES (1)")
    finally:
        # Always release the file handles, otherwise a failure here also breaks the cleanup
        # of ``tmp_path`` on Windows
        main.close()


def test_sqlite_uri_without_params(tmp_path):
    from bw2data.sqlite import _sqlite_uri

    assert _sqlite_uri(tmp_path / "a b.db") == (tmp_path / "a b.db").resolve().as_uri()
    assert "%20" in _sqlite_uri(tmp_path / "a b.db")


def test_path_as_uri_windows_paths():
    from pathlib import PureWindowsPath

    from bw2data.sqlite import _path_as_uri

    # Drive letter: empty authority, drive in the path
    assert _path_as_uri(PureWindowsPath(r"C:\data\a b.db")) == "file:///C:/data/a%20b.db"
    # UNC: SQLite rejects a server name as URI authority, so it stays in the path
    assert (
        _path_as_uri(PureWindowsPath(r"\\server\share\dir#1\a b.db"))
        == "file:////server/share/dir%231/a%20b.db"
    )


def test_schema_change_20_sql_migrates_old_schema(tmp_path):
    import sqlite3

    from bw2data.updates import UPDATE_ACTIVITYDATASET, UPDATE_EXCHANGEDATASET

    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE "activitydataset" ("id" INTEGER NOT NULL PRIMARY KEY, "data" BLOB NOT NULL,
                "database" TEXT NOT NULL, "key" TEXT NOT NULL, "location" TEXT, "name" TEXT,
                "product" TEXT, "type" TEXT);
            CREATE UNIQUE INDEX "activitydataset_key" ON "activitydataset" ("key");
            CREATE TABLE "exchangedataset" ("id" INTEGER NOT NULL PRIMARY KEY, "data" BLOB NOT NULL,
                "database" TEXT NOT NULL, "input" TEXT NOT NULL, "output" TEXT NOT NULL,
                "type" TEXT NOT NULL);
            CREATE INDEX "exchangedataset_database" ON "exchangedataset" ("database");
            CREATE INDEX "exchangedataset_input" ON "exchangedataset" ("input");
            CREATE INDEX "exchangedataset_output" ON "exchangedataset" ("output");
            INSERT INTO activitydataset ("data", "database", "key", "type")
                VALUES (x'00', 'db', 'db⊡a', 'process');
            INSERT INTO exchangedataset ("data", "database", "input", "output", "type")
                VALUES (x'00', 'db', 'db⊡a', 'db⊡b', 'technosphere');
            """
        )
        conn.executescript(UPDATE_ACTIVITYDATASET)
        conn.executescript(UPDATE_EXCHANGEDATASET)

        assert conn.execute('SELECT "database", "code" FROM activitydataset').fetchall() == [
            ("db", "a")
        ]
        assert conn.execute(
            'SELECT "input_database", "input_code", "output_database", "output_code" '
            "FROM exchangedataset"
        ).fetchall() == [("db", "a", "db", "b")]
        names = {
            row[0]
            for row in conn.execute("SELECT name FROM main.sqlite_master WHERE type != 'table'")
        }
        assert names == {"activitydataset_key", "exchangedataset_input", "exchangedataset_output"}
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM main.sqlite_master WHERE type = 'table'")
        }
        assert tables == {"activitydataset", "exchangedataset"}
    finally:
        conn.close()


@bw2test
def test_index_sql_targets_main_schema_only(tmp_path):
    # With a file attached that has the same index names, unqualified ``DROP INDEX`` would
    # fall through to the attached (read-only) file when ``main`` lacks the index.
    import sqlite3

    from bw2data.sqlite import _sqlite_uri

    other = tmp_path / "other.db"
    with sqlite3.connect(other) as conn:
        conn.execute("CREATE TABLE activitydataset (database TEXT, code TEXT)")
        conn.execute('CREATE UNIQUE INDEX "activitydataset_key" ON activitydataset (database, code)')
        conn.execute("CREATE TABLE exchangedataset (input_database TEXT, input_code TEXT, output_database TEXT, output_code TEXT)")
        conn.execute('CREATE INDEX "exchangedataset_input" ON exchangedataset (input_database, input_code)')
        conn.execute('CREATE INDEX "exchangedataset_output" ON exchangedataset (output_database, output_code)')
    conn.close()

    database = DatabaseChooser("testy")
    db.execute_sql("ATTACH ? AS other", (_sqlite_uri(other, mode="ro"),))
    try:
        database._drop_indices()
        # Second drop: ``main`` has no indices any more, must not touch ``other``
        database._drop_indices()
        database._add_indices()
        names = {
            row[0]
            for row in db.execute_sql("SELECT name FROM main.sqlite_master WHERE type='index'")
        }
        assert {"activitydataset_key", "exchangedataset_input", "exchangedataset_output"} <= names
    finally:
        db.execute_sql("DETACH other")


def test_lci_table_names_are_pinned():
    from bw2data.backends.schema import (
        ACTIVITY_TABLE,
        EXCHANGE_TABLE,
        ActivityDataset,
        ExchangeDataset,
    )

    assert ActivityDataset._meta.table_name == ACTIVITY_TABLE == "activitydataset"
    assert ExchangeDataset._meta.table_name == EXCHANGE_TABLE == "exchangedataset"


def _file_with_one_table(path):
    from peewee import Model, TextField

    from bw2data.sqlite import SubstitutableDatabase

    class Old(Model):
        name = TextField()

        class Meta:
            table_name = "old"

    class New(Model):
        name = TextField()

        class Meta:
            table_name = "new"

    sub = SubstitutableDatabase(path, [Old])
    Old.create(name="kept")
    sub.db.close()
    return Old, New


def _tables(db):
    return {
        row[0]
        for row in db.execute_sql(
            "SELECT name FROM main.sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


class _Locked:
    """Hold a lock on ``path`` from a second connection, as another process would:
    ``IMMEDIATE`` leaves the file readable, ``EXCLUSIVE`` doesn't."""

    def __init__(self, path, mode):
        import sqlite3

        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute(f"BEGIN {mode}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.conn.execute("ROLLBACK")
        self.conn.close()


def _table_warnings(recwarn):
    return [w for w in recwarn if "create the tables" in str(w.message)]


def test_substitutable_database_opens_read_only_file_without_new_table(tmp_path):
    from bw2data.sqlite import SubstitutableDatabase, _sqlite_uri

    path = tmp_path / "old.db"
    Old, New = _file_with_one_table(path)

    opened = []
    with pytest.warns(UserWarning, match="Can't create the tables new in") as record:
        sub = SubstitutableDatabase(
            _sqlite_uri(path, mode="ro"), [Old, New], on_open=opened.append, postpone_tables=True
        )
    try:
        # Reported where the file was opened, not inside bw2data
        assert record[0].filename == __file__
        assert "next time the file is opened" in str(record[0].message)
        # The file is open and readable, and ``on_open`` still ran
        assert [row.name for row in Old.select()] == ["kept"]
        assert opened == [sub.db]
        assert _tables(sub.db) == {"old"}
    finally:
        sub.db.close()

    # Created the next time the file is opened and can be written
    sub = SubstitutableDatabase(path, [Old, New], postpone_tables=True)
    try:
        assert _tables(sub.db) == {"old", "new"}
    finally:
        sub.db.close()


def test_substitutable_database_opens_up_to_date_read_only_file_without_warning(
    tmp_path, recwarn
):
    from bw2data.sqlite import SubstitutableDatabase, _sqlite_uri

    path = tmp_path / "current.db"
    Old, _ = _file_with_one_table(path)

    sub = SubstitutableDatabase(_sqlite_uri(path, mode="ro"), [Old], postpone_tables=True)
    try:
        assert [row.name for row in Old.select()] == ["kept"]
        assert not _table_warnings(recwarn)
    finally:
        sub.db.close()


@pytest.mark.skipif(
    sys.platform == "win32" or getattr(os, "geteuid", lambda: 1)() == 0,
    reason="needs a directory in which files can't be created",
)
def test_substitutable_database_opens_file_in_read_only_directory(tmp_path):
    from bw2data.sqlite import SubstitutableDatabase

    directory = tmp_path / "ro"
    directory.mkdir()
    path = directory / "old.db"
    Old, New = _file_with_one_table(path)

    # The file itself can be written, but SQLite can't create its journal
    directory.chmod(0o555)
    try:
        with pytest.warns(UserWarning, match="Can't create the tables new in"):
            sub = SubstitutableDatabase(path, [Old, New], postpone_tables=True)
        try:
            assert [row.name for row in Old.select()] == ["kept"]
        finally:
            sub.db.close()
    finally:
        directory.chmod(0o755)


def test_substitutable_database_tables_are_not_postponed_by_default(tmp_path):
    # ``projects.db`` and ``parameters.db``: the code using them needs their tables
    from peewee import OperationalError

    from bw2data.sqlite import SubstitutableDatabase, _sqlite_uri

    path = tmp_path / "old.db"
    Old, New = _file_with_one_table(path)

    with pytest.raises(OperationalError, match="readonly"):
        SubstitutableDatabase(_sqlite_uri(path, mode="ro"), [Old, New])
    with _Locked(path, "IMMEDIATE"):
        with pytest.raises(OperationalError, match="locked"):
            SubstitutableDatabase(path, [Old, New], pragmas={"busy_timeout": 0})


def test_substitutable_database_finishes_setup_of_write_locked_file_when_unlocked(tmp_path):
    from bw2data.sqlite import SubstitutableDatabase

    path = tmp_path / "locked.db"
    Old, New = _file_with_one_table(path)

    opened = []
    with _Locked(path, "IMMEDIATE"):
        with pytest.warns(UserWarning, match="Can't create the tables new in") as record:
            sub = SubstitutableDatabase(
                path,
                [Old, New],
                pragmas={"busy_timeout": 0},
                on_open=opened.append,
                postpone_tables=True,
            )
        assert "no longer locked" in str(record[0].message)
        # Readable, but ``on_open`` (the migrations) must not run without the tables
        assert [row.name for row in Old.select()] == ["kept"]
        assert _tables(sub.db) == {"old"}
        assert opened == []
    try:
        # No reopening needed: done before the first statement once the lock is gone
        assert list(New.select()) == []
        assert _tables(sub.db) == {"old", "new"}
        assert opened == [sub.db]
        # ... and only once, with the busy timeout as it was
        Old.create(name="more")
        assert opened == [sub.db]
        assert sub.db.execute_sql("PRAGMA busy_timeout").fetchone() == (0,)
    finally:
        sub.db.close()


@pytest.mark.parametrize("missing", [False, True])
def test_substitutable_database_opens_file_locked_for_reading(tmp_path, recwarn, missing):
    from peewee import OperationalError

    from bw2data.sqlite import SubstitutableDatabase

    path = tmp_path / "locked.db"
    Old, New = _file_with_one_table(path)
    tables = [Old, New] if missing else [Old]

    opened = []
    with _Locked(path, "EXCLUSIVE"):
        sub = SubstitutableDatabase(
            path,
            tables,
            pragmas={"busy_timeout": 0},
            on_open=opened.append,
            postpone_tables=True,
        )
        # Which tables the file has can't be known, so there is nothing to warn about
        assert not _table_warnings(recwarn)
        assert opened == []
        with pytest.raises(OperationalError, match="locked"):
            Old.select().count()
        assert opened == []
    try:
        assert [row.name for row in Old.select()] == ["kept"]
        assert opened == [sub.db]
        assert _tables(sub.db) == {model._meta.table_name for model in tables}
    finally:
        sub.db.close()


def test_substitutable_database_deferred_on_open_error_is_raised_and_tried_again(tmp_path):
    from bw2data.sqlite import SubstitutableDatabase

    path = tmp_path / "locked.db"
    Old, New = _file_with_one_table(path)

    calls = []

    def on_open(db):
        calls.append(db)
        if len(calls) == 1:
            raise RuntimeError("migration failed")

    with _Locked(path, "EXCLUSIVE"):
        sub = SubstitutableDatabase(
            path, [Old, New], pragmas={"busy_timeout": 0}, on_open=on_open, postpone_tables=True
        )
    try:
        with pytest.raises(RuntimeError, match="migration failed"):
            Old.select().count()
        assert Old.select().count() == 1
        assert calls == [sub.db, sub.db]
    finally:
        sub.db.close()


def test_substitutable_database_deferred_setup_waits_for_end_of_transaction(tmp_path):
    from bw2data.sqlite import SubstitutableDatabase

    path = tmp_path / "locked.db"
    Old, New = _file_with_one_table(path)

    opened = []
    lock = _Locked(path, "IMMEDIATE")
    with pytest.warns(UserWarning, match="Can't create the tables new in"):
        sub = SubstitutableDatabase(
            path,
            [Old, New],
            pragmas={"busy_timeout": 0},
            on_open=opened.append,
            postpone_tables=True,
        )
    try:
        # A read transaction started while the file is locked
        with sub.db.atomic() as txn:
            assert Old.select().count() == 1
            lock.__exit__()
            assert Old.select().count() == 1
            # Not created here, where they would be rolled back
            assert opened == []
            txn.rollback()
        assert _tables(sub.db) == {"old", "new"}
        assert opened == [sub.db]
    finally:
        sub.db.close()


def test_substitutable_database_deferred_setup_waits_for_end_of_bare_begin(tmp_path):
    # ``Database.write()`` begins its transaction with ``db.begin()``, which peewee's
    # ``in_transaction()`` doesn't know about
    from bw2data.sqlite import SubstitutableDatabase

    path = tmp_path / "locked.db"
    Old, New = _file_with_one_table(path)

    opened = []
    lock = _Locked(path, "IMMEDIATE")
    with pytest.warns(UserWarning, match="Can't create the tables new in"):
        sub = SubstitutableDatabase(
            path,
            [Old, New],
            pragmas={"busy_timeout": 0},
            on_open=opened.append,
            postpone_tables=True,
        )
    try:
        sub.db.begin()
        assert not sub.db.in_transaction() and sub.db.transaction_token() is not None
        lock.__exit__()
        assert Old.select().count() == 1
        assert opened == []
        sub.db.rollback()
        assert _tables(sub.db) == {"old", "new"}
        assert opened == [sub.db]
    finally:
        sub.db.close()


@pytest.mark.parametrize("message", ["disk I/O error", "database table is locked"])
def test_substitutable_database_other_create_errors_are_raised(tmp_path, monkeypatch, message):
    from peewee import OperationalError

    from bw2data.sqlite import BWSqliteDatabase, SubstitutableDatabase

    path = tmp_path / "broken.db"
    Old, New = _file_with_one_table(path)

    def broken(self, models, **options):
        raise OperationalError(message)

    monkeypatch.setattr(BWSqliteDatabase, "create_tables", broken)
    with pytest.raises(OperationalError, match=message):
        SubstitutableDatabase(path, [Old, New], postpone_tables=True)


def test_write_obstacle_uses_sqlite_error_codes(tmp_path):
    from peewee import OperationalError

    from bw2data.sqlite import BWSqliteDatabase, _cannot_write, _sqlite_uri, _write_obstacle

    path = tmp_path / "old.db"
    _file_with_one_table(path)

    def error(db, sql):
        with pytest.raises(OperationalError) as excinfo:
            db.execute_sql(sql)
        return excinfo.value

    db = BWSqliteDatabase(path, pragmas={"busy_timeout": 0})
    try:
        with _Locked(path, "EXCLUSIVE"):
            assert _write_obstacle(error(db, "SELECT * FROM old")) == "locked"
        err = error(db, "SELECT * FROM missing")
        assert _write_obstacle(err) is None and not _cannot_write(err)
    finally:
        db.close()

    db = BWSqliteDatabase(_sqlite_uri(path, mode="ro"))
    try:
        err = error(db, "INSERT INTO old (name) VALUES ('x')")
        assert _write_obstacle(err) == "readonly" and _cannot_write(err)
    finally:
        db.close()


@pytest.mark.parametrize(
    "message, expected",
    [
        ("database is locked", "locked"),
        ("attempt to write a readonly database", "readonly"),
        ("unable to open database file", "readonly"),
        # A conflict inside one connection, which doesn't go away by waiting
        ("database table is locked", None),
        ("database table is locked: old", None),
        ("no such table: old", None),
    ],
)
def test_write_obstacle_from_message(message, expected):
    from peewee import OperationalError

    from bw2data.sqlite import _write_obstacle

    assert _write_obstacle(OperationalError(message)) == expected
