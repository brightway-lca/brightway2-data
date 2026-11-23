import datetime
import json
import os
import pickle
import sys
import threading
import warnings
from collections.abc import Callable, Iterable
from contextlib import closing, contextmanager
from pathlib import Path, PurePath
from urllib.parse import quote, urlencode

from peewee import BlobField, OperationalError, SqliteDatabase, TextField, sqlite3

from bw2data import codec
from bw2data.configuration import config
from bw2data.logs import stdout_feedback_logger

if sys.version_info >= (3, 14):
    from compression import zstd
else:
    from backports import zstd


class PickleField(BlobField):
    def db_value(self, value):
        return super(PickleField, self).db_value(pickle.dumps(value, protocol=4))

    def python_value(self, value):
        return pickle.loads(bytes(value))


#: Table of zstd dictionaries used by ``CompressedJSONField``. Every file with such a field
#: needs it; see ``bw2data.backends.schema.ZstdDictionary``.
ZSTD_DICTIONARY_TABLE = "zstddictionary"
#: First four bytes of every zstd frame
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
#: Size of a trained dictionary, in bytes (zstd's default)
ZSTD_DICTIONARY_SIZE = 112_640
#: Compression level. Higher levels make writing slower but files barely smaller, because
#: most of the gain comes from the dictionary.
ZSTD_LEVEL = 3


class UnknownDictionaryError(ValueError):
    """A stored value was compressed with a zstd dictionary which isn't in the file."""


class ZstdDictionaries:
    """The zstd dictionaries of one SQLite file, and the compressors that use them.

    Dictionaries are stored in the ``zstddictionary`` table, keyed by the dictionary id that
    zstd writes in the header of every frame. At most one of them is *active*: new values are
    compressed with it, or without a dictionary if there is none. Older dictionaries stay, so
    that the values compressed with them can still be read.

    Dictionaries are read from the file the first time a frame needs them, and kept.

    A value must never be stored with a dictionary which another process has replaced and
    removed in the meantime (``remove_unused``): it could not be read again. The active
    dictionary is therefore only used for values compressed inside a transaction, where it
    is read from the file once, in that transaction (see ``write_id``). From then on SQLite
    lets no other connection commit a change before this transaction has ended, or else
    refuses the write of this one. Values compressed outside of a transaction can be stored
    at any later time, so they are compressed without a dictionary: larger, but always
    readable. ``ActivityDataset.save()`` and ``ExchangeDataset.save()`` open a transaction
    for this reason.

    One instance belongs to each ``BWSqliteDatabase``; get it with ``zstd_dictionaries(db)``.
    Compressors can't be shared between threads, so each thread gets its own."""

    def __init__(self, database: SqliteDatabase):
        self._database = database
        self._dictionaries: dict[int, zstd.ZstdDict] = {}
        self._lock = threading.Lock()
        #: Per thread: ``compressors``, and ``pinned``, the ``(transaction token, dictionary
        #: id)`` of the last transaction which compressed a value
        self._local = threading.local()

    # Reading the table

    def _execute(self, sql: str, params=()) -> list:
        """Rows returned by ``sql``. The cursor is closed straight away: an open cursor holds
        the file until garbage collection, which on Windows blocks deleting it."""
        with closing(self._database.execute_sql(sql, params)) as cursor:
            return cursor.fetchall()

    def _load(self, dict_id: int) -> zstd.ZstdDict:
        rows = self._execute(
            f'SELECT "content" FROM main."{ZSTD_DICTIONARY_TABLE}" WHERE "id" = ?', (dict_id,)
        )
        if not rows:
            raise UnknownDictionaryError(
                f"A stored value was compressed with zstd dictionary {dict_id}, which isn't in "
                f"the {ZSTD_DICTIONARY_TABLE} table of {self._database.database}"
            )
        dictionary = zstd.ZstdDict(bytes(rows[0][0]))
        with self._lock:
            self._dictionaries[dict_id] = dictionary
        return dictionary

    def get(self, dict_id: int) -> zstd.ZstdDict:
        """The dictionary with id ``dict_id``. Raises ``UnknownDictionaryError`` if the file
        doesn't have it."""
        try:
            return self._dictionaries[dict_id]
        except KeyError:
            return self._load(dict_id)

    def active_id(self) -> int:
        """Id of the active dictionary in the file right now, or 0 if there is none. Outside
        of a transaction, another process can change it at any time."""
        rows = self._execute(f'SELECT "id" FROM main."{ZSTD_DICTIONARY_TABLE}" WHERE "active" = 1')
        return rows[0][0] if rows else 0

    def write_id(self) -> int:
        """Id of the dictionary to compress a value with which the calling thread is about to
        store: the active dictionary inside a transaction, and 0 (none) outside of one.

        The first call in a transaction reads the active dictionary from the file, and the
        following ones return the same id. That read is what makes the dictionary safe to
        use: it happens in the transaction which stores the value, so no other connection can
        replace and remove the dictionary in between.

        Transactions are those of ``BWSqliteDatabase.transaction_token``, i.e. started through
        peewee and not with a raw ``BEGIN``."""
        token = getattr(self._database, "transaction_token", lambda: None)()
        if token is None:
            return 0
        pinned = getattr(self._local, "pinned", None)
        if pinned is None or pinned[0] is not token:
            pinned = self._local.pinned = (token, self.active_id())
        return pinned[1]

    def ids(self) -> list[int]:
        """Ids of all dictionaries in the file."""
        return [
            row[0]
            for row in self._execute(
                f'SELECT "id" FROM main."{ZSTD_DICTIONARY_TABLE}" ORDER BY "id"'
            )
        ]

    # Compressing

    def _compressor(self, dict_id: int) -> zstd.ZstdCompressor:
        compressors = getattr(self._local, "compressors", None)
        if compressors is None:
            compressors = self._local.compressors = {}
        try:
            return compressors[dict_id]
        except KeyError:
            # One compressor per dictionary, reused: preparing a dictionary for every value
            # (as ``zstd.compress(data, zstd_dict=...)`` does) is about 100 times slower
            compressor = zstd.ZstdCompressor(
                level=ZSTD_LEVEL, zstd_dict=self.get(dict_id) if dict_id else None
            )
            compressors[dict_id] = compressor
            return compressor

    def compress(self, data: bytes) -> bytes:
        """``data`` as one zstd frame, compressed with the active dictionary inside a
        transaction and without a dictionary outside of one; see ``write_id``."""
        return self._compressor(self.write_id()).compress(
            data, mode=zstd.ZstdCompressor.FLUSH_FRAME
        )

    def decompress(self, frame: bytes) -> bytes:
        """The content of a zstd frame, using the dictionary named in its header."""
        dict_id = zstd.get_frame_info(frame).dictionary_id
        return zstd.decompress(frame, zstd_dict=self.get(dict_id) if dict_id else None)

    # Changing the table

    def add(self, dictionary: zstd.ZstdDict, activate: bool = True) -> int:
        """Store ``dictionary`` and, with ``activate``, make it the active one. Returns its id.

        A dictionary which is already stored is only activated. Raises ``ValueError`` if a
        different dictionary with the same id is stored (zstd derives ids from a hash of the
        content, so this is very unlikely), and ``RuntimeError`` inside a transaction: if that
        transaction were rolled back, values compressed with the new dictionary in the
        meantime could no longer be read."""
        if (
            self._database.in_transaction()
            or getattr(self._database, "transaction_token", lambda: None)() is not None
        ):
            raise RuntimeError("Can't add a zstd dictionary inside a transaction")
        dict_id = dictionary.dict_id
        if not dict_id:
            raise ValueError("Can't store a zstd dictionary without an id")
        content = dictionary.dict_content
        table = f'main."{ZSTD_DICTIONARY_TABLE}"'
        with self._database.atomic():
            rows = self._execute(f'SELECT "content" FROM {table} WHERE "id" = ?', (dict_id,))
            if not rows:
                self._execute(
                    f'INSERT INTO {table} ("id", "content", "created", "active") '
                    "VALUES (?, ?, ?, 0)",
                    (
                        dict_id,
                        content,
                        datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
                    ),
                )
            elif bytes(rows[0][0]) != content:
                raise ValueError(f"A different zstd dictionary with id {dict_id} is stored")
            if activate:
                self._execute(f'UPDATE {table} SET "active" = 0 WHERE "active" = 1')
                self._execute(f'UPDATE {table} SET "active" = 1 WHERE "id" = ?', (dict_id,))
        with self._lock:
            self._dictionaries[dict_id] = dictionary
        return dict_id

    def train(self, samples: list[bytes], activate: bool = True) -> int | None:
        """Train a dictionary on ``samples`` (values as they are stored, before compression),
        store it and, with ``activate``, make it the active one. Returns its id, or ``None``
        if zstd can't train a dictionary from these samples (none, too few, or too small)."""
        if not samples:
            return None
        try:
            dictionary = zstd.train_dict(samples, ZSTD_DICTIONARY_SIZE)
        except (zstd.ZstdError, ValueError) as err:
            # ``ValueError``: the samples hold no data at all
            stdout_feedback_logger.debug("Can't train a zstd dictionary", error=str(err))
            return None
        return self.add(dictionary, activate=activate)

    def remove_unused(self, tables: dict[str, str]) -> list[int]:
        """Delete the dictionaries which no value uses, except the active one. ``tables``
        maps the names of the tables to check to their compressed column. Returns the ids
        of the deleted dictionaries.

        Only the frame headers are read, not the values. Runs in one write transaction, so
        no other connection can activate a dictionary or store values in the meantime. A
        value which another connection has compressed but not stored yet can't use one of
        the deleted dictionaries either; see ``write_id``."""
        table = f'main."{ZSTD_DICTIONARY_TABLE}"'
        with self._database.atomic("IMMEDIATE"):
            used = {row[0] for row in self._execute(f'SELECT "id" FROM {table} WHERE "active" = 1')}
            for name, column in tables.items():
                # A frame header is at most 18 bytes long
                sql = f'SELECT substr("{column}", 1, 18) FROM main."{name}"'
                with closing(self._database.execute_sql(sql)) as cursor:
                    for (header,) in cursor:
                        if header is not None and bytes(header[:4]) == ZSTD_MAGIC:
                            used.add(zstd.get_frame_info(bytes(header)).dictionary_id)
            unused = [dict_id for dict_id in self.ids() if dict_id not in used]
            for dict_id in unused:
                self._execute(f'DELETE FROM {table} WHERE "id" = ?', (dict_id,))
        with self._lock:
            for dict_id in unused:
                self._dictionaries.pop(dict_id, None)
        return unused


def zstd_dictionaries(database: SqliteDatabase) -> ZstdDictionaries:
    """The ``ZstdDictionaries`` of ``database``, created on first use."""
    try:
        return database._bw_zstd_dictionaries
    except AttributeError:
        store = database._bw_zstd_dictionaries = ZstdDictionaries(database)
        return store


class CompressedJSONField(BlobField):
    """A value stored as tagged JSON (see ``bw2data.codec``), compressed with zstd.

    The column is a ``BLOB``, and its first bytes say how a value is stored:

    * ``28 b5 2f fd``: a zstd frame. Its header names the dictionary it was compressed with,
      if any, which must be in the ``zstddictionary`` table of the same file (see
      ``ZstdDictionaries``). New values are always written this way.
    * Anything else: a pickle written by an older bw2data version. It is read with
      ``codec.safe_pickle_loads``, which refuses anything except the types bw2data stores.

    So the same column can hold values in both formats, for example while a schema migration
    is postponed."""

    def db_value(self, value):
        if value is None:
            return None
        store = zstd_dictionaries(self.model._meta.database)
        return super().db_value(store.compress(codec.dumps(value)))

    def python_value(self, value):
        if value is None:
            return None
        value = bytes(value)
        if value[:4] == ZSTD_MAGIC:
            store = zstd_dictionaries(self.model._meta.database)
            return codec.loads(store.decompress(value))
        return codec.safe_pickle_loads(value)


#: A connection hook gets the raw DBAPI connection. ``sqlite3`` here is the driver peewee
#: uses (``peewee.sqlite3``), which is ``pysqlite3`` when that package is installed and the
#: standard library module otherwise. Its ``Connection`` and exception classes are not
#: interchangeable with those of the other driver.
ConnectionHook = Callable[[sqlite3.Connection], None]


def _path_as_uri(path: PurePath) -> str:
    """``file:`` URI for an absolute ``path``, in the form SQLite accepts.

    ``PurePath.as_uri()`` puts the server of a Windows UNC path (``\\\\server\\share\\x.db``) in
    the URI authority (``file://server/share/x.db``), but SQLite only accepts an empty or
    ``localhost`` authority. UNC paths therefore keep the server in the URI path:
    ``file:////server/share/x.db``."""
    if path.drive.startswith("\\\\"):
        return "file://" + quote(path.as_posix(), safe="/")
    return path.as_uri()


def _sqlite_uri(path, **params) -> str:
    """Build a ``file:`` URI for ``path``, suitable for ``sqlite3.connect(..., uri=True)`` or
    ``ATTACH``.

    Characters with a meaning in URIs (space, ``#``, ``?``, ``%``, ...) are percent-escaped.
    Windows drive-letter and UNC (network share) paths are both supported. ``params`` become
    the query string, e.g. ``_sqlite_uri(path, mode="ro")`` gives ``file:///.../x.db?mode=ro``."""
    uri = _path_as_uri(Path(path).resolve())
    if params:
        uri += "?" + urlencode(params)
    return uri


#: Primary SQLite result codes which mean that a file can't be written right now.
#: ``SQLITE_LOCKED`` (6, "database table is locked") is left out on purpose: it is a conflict
#: inside one connection, i.e. a bug, and waiting doesn't make it go away. ``SQLITE_CANTOPEN``
#: on a connection which is already open means that the journal can't be created, as in a
#: read-only directory.
_WRITE_OBSTACLES = {5: "locked", 8: "readonly", 14: "readonly"}  # BUSY, READONLY, CANTOPEN


def _write_obstacle(err: Exception) -> str | None:
    """Why the file can't be written right now, if this is what ``err`` means: ``"locked"``
    (another connection holds a lock; this passes) or ``"readonly"`` (it doesn't pass while
    the file is open). ``None`` for every other error.

    Uses the SQLite result code where the driver gives one, and the exact SQLite messages
    otherwise (e.g. for an older ``pysqlite3``)."""
    code = getattr(getattr(err, "orig", err), "sqlite_errorcode", None)
    if code is not None:
        return _WRITE_OBSTACLES.get(code & 0xFF)
    message = str(err).lower()
    if "database is locked" in message:
        return "locked"
    if "readonly database" in message or "unable to open database file" in message:
        return "readonly"
    return None


def _cannot_write(err: Exception) -> bool:
    """Whether ``err`` means that the file can't be written right now (locked or read-only)."""
    return _write_obstacle(err) is not None


_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__)) + os.sep


def _user_stacklevel() -> int:
    """``stacklevel`` for a ``warnings.warn`` call in the calling function, such that the
    warning is reported at the first line of code outside ``bw2data``.

    Files are opened deep inside ``bw2data`` (e.g. from ``projects.set_current``), so a fixed
    ``stacklevel`` always reports the same internal line."""
    level = 2
    frame = sys._getframe(2) if hasattr(sys, "_getframe") else None
    while frame is not None:
        filename = frame.f_code.co_filename
        if "importlib" in filename and "_bootstrap" in filename:
            # Not counted by ``warnings.warn`` either
            pass
        elif os.path.abspath(filename).startswith(_PACKAGE_DIR):
            level += 1
        else:
            break
        frame = frame.f_back
    return level


@contextmanager
def _no_busy_wait(db: SqliteDatabase):
    """Fail at once on a locked file, instead of waiting for the busy timeout."""
    (previous,) = db.execute_sql("PRAGMA busy_timeout").fetchone()
    db.execute_sql("PRAGMA busy_timeout = 0")
    try:
        yield
    finally:
        db.execute_sql(f"PRAGMA busy_timeout = {int(previous)}")


class BWSqliteDatabase(SqliteDatabase):
    """``SqliteDatabase`` that runs extra callables on every new connection.

    Peewee opens one connection per thread, and has no public hook to run SQL right after a
    connection is opened. Callables registered with ``add_connection_hook`` are called with the
    raw DBAPI connection (see ``ConnectionHook``) after peewee's own setup (pragmas,
    functions, ...), for every connection, in every thread. Hooks can also be given at
    construction time with ``connection_hooks``, so that they apply to the very first connection.

    A hook must work with the connection it is given and must not go back through this object
    (``db.connection()``, ``db.execute_sql(...)``): it runs while peewee holds its connection
    lock, so that would deadlock.

    Connections are always opened with ``uri=True``, so that ``file:`` URIs (and in particular
    ``?mode=ro``) work in ``ATTACH`` statements. A plain filesystem path is still a valid
    database name in URI mode.

    ``defer_setup`` registers work which could not be done when the file was opened because it
    was locked; see there."""

    def __init__(
        self, database, *args, connection_hooks: Iterable[ConnectionHook] = (), **kwargs
    ):
        self._connection_hooks: list[ConnectionHook] = list(connection_hooks)
        self._deferred_setup: Callable[["BWSqliteDatabase"], bool] | None = None
        self._deferred_setup_lock = threading.Lock()
        self._transaction_local = threading.local()
        super().__init__(database, *args, **kwargs)

    def init(self, database, *args, **kwargs):
        # Set here and not in ``__init__``: peewee replaces all connection parameters on every
        # ``init`` call, including the one that gives a deferred database its path.
        kwargs.setdefault("uri", True)
        super().init(database, *args, **kwargs)

    def defer_setup(self, setup: Callable[["BWSqliteDatabase"], bool] | None) -> None:
        """Call ``setup(self)`` before the next statement which is not inside a transaction,
        and again before each following one until it returns ``True``.

        ``setup`` returns ``False`` when the file is still locked; it should then have failed
        fast (see ``_no_busy_wait``), as it is tried before every statement. If it raises, the
        exception is passed on to the caller of the statement, and ``setup`` stays registered.
        Never called inside a transaction (including one begun with a bare ``begin()``; see
        ``transaction_token``), so that its changes can't be rolled back with it."""
        self._deferred_setup = setup

    def execute_sql(self, sql, params=None):
        if (
            self._deferred_setup is not None
            and not self.in_transaction()
            and self.transaction_token() is None
        ):
            self._run_deferred_setup()
        return super().execute_sql(sql, params)

    def _run_deferred_setup(self) -> None:
        # Another thread is already at it; its result is not waited for
        if not self._deferred_setup_lock.acquire(blocking=False):
            return
        try:
            setup = self._deferred_setup
            if setup is None:
                return
            # The statements of ``setup`` itself must not come back here
            self._deferred_setup = None
            done = False
            try:
                done = setup(self)
            finally:
                if not done:
                    self._deferred_setup = setup
        finally:
            self._deferred_setup_lock.release()
    def transaction_token(self) -> object | None:
        """An object which stands for the transaction the calling thread is in, or ``None``
        outside of a transaction. Two calls return the same object exactly when no transaction
        was begun, committed or rolled back in between, so it can be used to keep something
        for the length of one transaction.

        Unlike ``in_transaction()``, this follows ``begin()``, ``commit()`` and ``rollback()``
        themselves. It therefore also knows transactions begun with a bare ``db.begin()``, and
        gives a new token when a transaction is committed or rolled back and begun again
        inside one ``with db.atomic() as txn:`` block. Savepoints don't change it. A ``BEGIN``
        sent with ``execute_sql`` is not seen."""
        return getattr(self._transaction_local, "token", None)

    def begin(self, *args, **kwargs):
        super().begin(*args, **kwargs)
        self._transaction_local.token = object()

    def commit(self):
        try:
            return super().commit()
        finally:
            # Also if this fails: nothing may be kept for a transaction whose state is
            # unknown. Not before, as the ``COMMIT`` itself is still inside the transaction.
            self._transaction_local.token = None

    def rollback(self):
        try:
            return super().rollback()
        finally:
            self._transaction_local.token = None

    def close(self):
        try:
            return super().close()
        finally:
            # A transaction which was still open is rolled back with the connection
            if self.is_closed():
                self._transaction_local.token = None

    def add_connection_hook(self, hook: ConnectionHook) -> None:
        """Run ``hook(conn)`` on every connection opened from now on.

        If the calling thread already has an open connection, ``hook`` is run on it right away,
        so that this thread does not behave differently from threads which connect later.
        Connections already open in *other* threads cannot be reached and are left as they are.
        If ``hook`` raises here, it is not registered."""
        if not self.is_closed():
            hook(self.connection())
        self._connection_hooks.append(hook)

    def remove_connection_hook(self, hook: ConnectionHook) -> None:
        """Stop running ``hook`` on new connections. Does nothing if ``hook`` is not registered.

        Connections which are already open are not changed."""
        try:
            self._connection_hooks.remove(hook)
        except ValueError:
            pass

    @property
    def connection_hooks(self) -> tuple[ConnectionHook, ...]:
        return tuple(self._connection_hooks)

    def _add_conn_hooks(self, conn):
        super()._add_conn_hooks(conn)
        for hook in self._connection_hooks:
            hook(conn)


class SubstitutableDatabase:
    def __init__(
        self,
        filepath,
        tables,
        pragmas=None,
        connection_hooks=(),
        on_open=None,
        postpone_tables=False,
        before_vacuum=None,
    ):
        """``on_open``, if given, is called with the peewee database each time a file is opened
        (on creation and after every ``change_path``), after the tables have been created. It is
        used to run schema migrations. If it raises, the file is closed again and the exception
        is passed on.

        Tables in ``tables`` which the file doesn't have yet are created when it is opened, all
        of them or none. If the file can't be written, this fails, unless ``postpone_tables`` is
        true. Then such a file can still be opened and read:

        * If the file is read-only, there is a warning, and the tables are created the next
          time the file is opened and can be written. ``on_open`` is called as usual.
        * If the file is locked by another connection, there is a warning if tables are known
          to be missing. The tables are created, and ``on_open`` is called, before the first
          statement (outside of a transaction) for which the file is no longer locked. If
          ``on_open`` raises then, the exception is passed on to the caller of that statement.

        Until the tables are created, anything which needs a missing table fails with "no such
        table". Use ``postpone_tables`` only when the code using the file can handle that.

        ``before_vacuum``, if given, is called with the peewee database right before ``vacuum``
        runs ``VACUUM``, for example to delete rows which are no longer needed."""
        self._filepath = filepath
        self._tables = tables
        self._pragmas = pragmas or {}
        self._on_open = on_open
        self._postpone_tables = postpone_tables
        self._before_vacuum = before_vacuum
        self._database = self._create_database(connection_hooks)

    def _create_database(self, connection_hooks=()):
        # Hooks are given to the constructor so that they also run on the connection opened
        # below
        db = BWSqliteDatabase(
            self._filepath, pragmas=self._pragmas, connection_hooks=connection_hooks
        )
        self._bind(db)
        try:
            db.connect()
            if self._create_tables(db) == "locked":
                # ``on_open`` has to wait as well: it may need the tables, and by the time it
                # runs the lock may be gone
                db.defer_setup(self._finish_setup)
            elif self._on_open is not None:
                self._on_open(db)
        except BaseException:
            db.close()
            raise
        return db

    def _create_tables(self, db, warn=True) -> str | None:
        """Create the tables which ``db`` doesn't have yet.

        Returns ``None`` if ``db`` now has all tables. If it can't be written and
        ``postpone_tables`` is set, returns why (see ``_write_obstacle``), after a warning if
        ``warn`` is set and tables are known to be missing.

        Tables which already exist need no write, so an up to date file can be opened while
        another connection is writing to it, or when it is read-only."""
        try:
            # In one transaction: otherwise every table, index and trigger of a new file is
            # committed, and synced to disk, on its own, which is slow on Windows. It also
            # means that a lock taken by another process half-way leaves nothing half-created.
            with db.atomic():
                db.create_tables(self._tables)
            return None
        except OperationalError as err:
            obstacle = _write_obstacle(err)
            if obstacle is None or not self._postpone_tables:
                raise
            error = err

        try:
            existing = set(db.get_tables())
        except OperationalError as err:
            if _write_obstacle(err) is None:
                raise
            # Locked for reading as well. Nothing is known to be missing, and the next
            # statement says "database is locked" by itself, so there is no warning.
            return "locked"
        missing = sorted(
            model._meta.table_name
            for model in self._tables
            if model._meta.table_name not in existing
        )
        if not missing:
            # All tables are there (the lock is gone, or only an index is missing)
            return None
        if warn:
            when = (
                "as soon as the file is no longer locked"
                if obstacle == "locked"
                else "the next time the file is opened and can be written"
            )
            warnings.warn(
                f"Can't create the tables {', '.join(missing)} in {db.database} ({error}). "
                f"They are created {when}; until then, the data stored in them is not "
                "available.",
                stacklevel=_user_stacklevel(),
            )
        return obstacle

    def _finish_setup(self, db) -> bool:
        """What ``_create_database`` could not do because the file was locked. Returns whether
        it is done; see ``BWSqliteDatabase.defer_setup``."""
        with _no_busy_wait(db):
            if self._create_tables(db, warn=False) == "locked":
                return False
        if self._on_open is not None:
            self._on_open(db)
        return True

    def _bind(self, db):
        for model in self._tables:
            model.bind(db, bind_refs=False, bind_backrefs=False)

    @property
    def db(self):
        return self._database

    def change_path(self, filepath):
        """Close the current file and open ``filepath`` instead.

        If the new file can't be opened, the previous file is opened again and the exception is
        passed on, so that this object and the models never point at different files."""
        old_db = self._database
        old_filepath = self._filepath
        # The database object is replaced, but its connection hooks must survive the switch
        connection_hooks = old_db.connection_hooks
        # ``close`` releases the file handle, as every cursor is closed after use (#308). No
        # ``gc.collect()`` here: it took most of the time of a project switch.
        old_db.close()
        self._filepath = filepath
        try:
            self._database = self._create_database(connection_hooks)
        except BaseException:
            self._filepath = old_filepath
            self._bind(old_db)
            old_db.connect(reuse_if_open=True)
            raise

    def add_connection_hook(self, hook: ConnectionHook) -> None:
        """Run ``hook(conn)`` on every connection to this database, now and after
        ``change_path``. See ``BWSqliteDatabase.add_connection_hook``."""
        self.db.add_connection_hook(hook)

    def remove_connection_hook(self, hook: ConnectionHook) -> None:
        self.db.remove_connection_hook(hook)

    @property
    def connection_hooks(self) -> tuple[ConnectionHook, ...]:
        return self.db.connection_hooks

    def atomic(self):
        return self.db.atomic()

    def execute_sql(self, *args, **kwargs):
        return self.db.execute_sql(*args, **kwargs)

    def transaction(self):
        return self.db.transaction()

    #: Minimum share of the file that must be free pages before the default
    #: policy (``config.vacuum is None``) runs ``VACUUM``.
    VACUUM_FREE_FRACTION = 0.1

    def page_count(self) -> int:
        """Total number of pages in the database file."""
        return self.execute_sql("PRAGMA page_count;").fetchone()[0]

    def freelist_count(self) -> int:
        """Number of unused pages in the database file.

        These pages are already allocated but hold no data; ``VACUUM`` is the only way to
        return them to the filesystem."""
        return self.execute_sql("PRAGMA freelist_count;").fetchone()[0]

    def vacuum(self) -> bool:
        """Run ``VACUUM`` on the database file according to ``config.vacuum``.

        ``VACUUM`` rewrites the whole file, which for a large LCI database can take tens of
        seconds, so it should only run when there is space worth reclaiming. The policy is:

        * ``config.vacuum is None`` (default): run only when more than
          ``VACUUM_FREE_FRACTION`` of the file's pages are on the freelist, as reported by
          ``PRAGMA freelist_count``.
        * ``config.vacuum is True``: always run.
        * ``config.vacuum is False``: never run.

        Must not be called inside a transaction. Returns whether ``VACUUM`` ran."""
        if config.vacuum is None:
            pages = self.page_count()
            needed = pages > 0 and self.freelist_count() / pages > self.VACUUM_FREE_FRACTION
        else:
            needed = bool(config.vacuum)
        if needed:
            if self._before_vacuum is not None:
                self._before_vacuum(self.db)
            stdout_feedback_logger.info("Vacuuming database ")
            self.execute_sql("VACUUM;")
        return needed


class JSONField(TextField):
    """Simpler JSON field that doesn't support advanced querying and is human-readable"""

    def db_value(self, value):
        if value is None:
            return None
        return super().db_value(
            json.dumps(
                value,
                ensure_ascii=False,
                indent=2,
                default=lambda x: x.isoformat() if hasattr(x, "isoformat") else x,
            )
        )

    def python_value(self, value):
        if value is None:
            return None
        return json.loads(value)


class TupleNameField(TextField):
    """A tuple name, like ``("ReCiPe", "climate change")``, stored as a compact JSON list.

    Equal tuples are always stored as equal strings, so the column can be unique and compared
    with ``==``. A list is stored like the equal tuple; values are read back as tuples."""

    def db_value(self, value):
        if value is None:
            return None
        return super().db_value(json.dumps(list(value), ensure_ascii=False))

    def python_value(self, value):
        if value is None:
            return None
        return tuple(json.loads(value))


class TupleJSONField(JSONField):
    def python_value(self, value):
        if value is None:
            return None
        data = json.loads(value)
        if isinstance(data, list):
            data = tuple(data)
        return data
