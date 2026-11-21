import json
import pickle
from collections.abc import Callable, Iterable
from pathlib import Path, PurePath
from urllib.parse import quote, urlencode

from peewee import BlobField, SqliteDatabase, TextField, sqlite3

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
    database name in URI mode."""

    def __init__(
        self, database, *args, connection_hooks: Iterable[ConnectionHook] = (), **kwargs
    ):
        self._connection_hooks: list[ConnectionHook] = list(connection_hooks)
        super().__init__(database, *args, **kwargs)

    def init(self, database, *args, **kwargs):
        # Set here and not in ``__init__``: peewee replaces all connection parameters on every
        # ``init`` call, including the one that gives a deferred database its path.
        kwargs.setdefault("uri", True)
        super().init(database, *args, **kwargs)

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
    def __init__(self, filepath, tables, pragmas=None, connection_hooks=(), on_open=None):
        """``on_open``, if given, is called with the peewee database each time a file is opened
        (on creation and after every ``change_path``), after the tables have been created. It is
        used to run schema migrations. If it raises, the file is closed again and the exception
        is passed on."""
        self._filepath = filepath
        self._tables = tables
        self._pragmas = pragmas or {}
        self._on_open = on_open
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
            db.create_tables(self._tables)
            if self._on_open is not None:
                self._on_open(db)
        except BaseException:
            db.close()
            raise
        return db

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
        import gc
        old_db = self._database
        old_filepath = self._filepath
        # The database object is replaced, but its connection hooks must survive the switch
        connection_hooks = old_db.connection_hooks
        old_db.close()
        gc.collect()
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


class TupleJSONField(JSONField):
    def python_value(self, value):
        if value is None:
            return None
        data = json.loads(value)
        if isinstance(data, list):
            data = tuple(data)
        return data
