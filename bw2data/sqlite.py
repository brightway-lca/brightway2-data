import json
import os
import pickle
import sys
import threading
import warnings
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from pathlib import Path, PurePath
from urllib.parse import quote, urlencode

from peewee import BlobField, OperationalError, SqliteDatabase, TextField, sqlite3

from bw2data.configuration import config
from bw2data.logs import stdout_feedback_logger


class PickleField(BlobField):
    def db_value(self, value):
        return super(PickleField, self).db_value(pickle.dumps(value, protocol=4))

    def python_value(self, value):
        return pickle.loads(bytes(value))


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
        Never called inside a transaction, so that its changes can't be rolled back with it."""
        self._deferred_setup = setup

    def execute_sql(self, sql, params=None):
        if self._deferred_setup is not None and not self.in_transaction():
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
        table". Use ``postpone_tables`` only when the code using the file can handle that."""
        self._filepath = filepath
        self._tables = tables
        self._pragmas = pragmas or {}
        self._on_open = on_open
        self._postpone_tables = postpone_tables
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
