"""SQL for read-only LCI extension databases.

An extension is a read-only SQLite file with the same LCI tables as the project's
``lci/databases.db``, attached to every connection as ``ext_1``, ``ext_2``, ... The functions here
build the SQL that makes the attached files look like one inventory together with ``main``:

* a TEMP table ``lci_extension_databases``, listing the database names stored in each
  extension;
* one TEMP view per overlaid table (``lci_activitydataset``, ``lci_exchangedataset`` and
  ``lci_databaserecord``), each a ``UNION ALL`` of ``main`` and every extension;
* ``INSTEAD OF`` triggers on each view, which send writes to ``main`` and refuse writes to rows
  stored in an extension, or that would put rows under an extension's database name.

Everything is created in the ``temp`` schema, so it exists only on the connection it was
created on, and never ends up in a project file. The SQL must therefore be run on every new
connection, after the extensions are attached; that is the job of the code which attaches them.

Each database name belongs to exactly one file. A file owns a name when it has rows for it: a
database record, activities (``database``) or exchanges (``output_database``). An extension
must have a database record for every name it owns, and no name owned by an extension may be
owned by ``main`` or by another extension. The triggers rely on this; ``ownership_check_sql``
builds the query which checks it, to be run before the overlay is created. An exchange whose
*input* is in another file is fine: that is how files link to each other.

Some fields of a database record are local to the project: ``dirty``, ``searchable`` and
``modified`` change when the project uses a database, also one stored in an extension. A change
to one of these fields of an extension database is stored in the project-local table
``extensiondatabasestate``, and the ``lci_databaserecord`` view shows it in place of the value
stored in the extension. Only the fields which were changed are stored. ``NULL`` means "not set
locally": setting a local field of an extension database to ``NULL`` shows the extension's
value again. Everything else in an extension's record, including ``extra``, is read-only.

Refusals raise ``sqlite3.IntegrityError`` with a message starting with ``EXTENSION_ERROR_PREFIX``.
Writes through a view report ``rowcount == 0``, also when rows were changed: SQLite doesn't
count changes made by triggers.

The SQL builders are pure functions. ``check_extensions`` and ``attach_hook`` are used by
``SubstitutableDatabase`` to attach extension files (``ExtensionSpec``) to every connection; see
"Attaching" below.
"""

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path
from types import MappingProxyType

from peewee import Model, sqlite3

from bw2data.backends.schema import (
    ACTIVITY_TABLE,
    EXCHANGE_TABLE,
    ActivityDataset,
    DatabaseRecord,
    ExchangeDataset,
    ExtensionDatabaseState,
)
from bw2data.errors import (
    EXTENSION_ERROR_PREFIX,
    ExtensionDependencyError,
    ExtensionError,
    ReadOnlyExtensionError,
)
from bw2data.sqlite import ZSTD_DICTIONARY_TABLE, ConnectionHook, _sqlite_uri

#: TEMP table with one row per database name stored in an attached extension
REGISTRY_TABLE = "lci_extension_databases"

#: Columns of ``DatabaseRecord`` which are local to the project, also for a database stored in
#: an extension
LOCAL_STATE_COLUMNS = ("dirty", "searchable", "modified")

#: Key of ``DatabaseRecord.extra`` which is local to the project, also for a database stored in
#: an extension: when the project last processed the database. Stored in the ``processed``
#: column of ``ExtensionDatabaseState``.
LOCAL_STATE_EXTRA_KEY = "processed"

#: Values of the ``problem`` column of the query built by ``ownership_check_sql``
NO_RECORD = "no_record"
DUPLICATE_NAME = "duplicate_name"

#: Error messages of the triggers. SQLite only accepts a string literal in ``RAISE``.
READ_ONLY_DATABASE = (
    f"{EXTENSION_ERROR_PREFIX} this database is stored in a read-only extension and can't be "
    "changed"
)
EXTENSION_DATABASE_NAME = (
    f"{EXTENSION_ERROR_PREFIX} this database name is used by a read-only extension"
)
DUPLICATE_ID = f"{EXTENSION_ERROR_PREFIX} this id is already used in a read-only extension"
MISSING_ID = f"{EXTENSION_ERROR_PREFIX} a row needs an id when read-only extensions are attached"
NOT_LOCAL_STATE = (
    f"{EXTENSION_ERROR_PREFIX} only the project-local fields ("
    + ", ".join(LOCAL_STATE_COLUMNS + (LOCAL_STATE_EXTRA_KEY,))
    + ") of a database stored in a read-only extension can be changed"
)

_SCHEMA_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_RESERVED_SCHEMAS = {"main", "temp"}


#: The overlaid models, in the order their views are created, and the names of their tables.
#: While extensions are attached, the models point at the views instead (``use_views``), so
#: ``model._meta.table_name`` can't be used to find the table.
TABLE_NAMES: Mapping[type[Model], str] = MappingProxyType(
    {
        ActivityDataset: ACTIVITY_TABLE,
        ExchangeDataset: EXCHANGE_TABLE,
        DatabaseRecord: "databaserecord",
    }
)
OVERLAID_MODELS = tuple(TABLE_NAMES)


def _table(model: type[Model]) -> str:
    """Name of the table of ``model``, also while the model points at its view."""
    return TABLE_NAMES.get(model, model._meta.table_name)


def view_name(model: type[Model]) -> str:
    """Name of the TEMP view which overlays the table of ``model``."""
    return f"lci_{_table(model)}"


def use_views(on: bool) -> None:
    """Point the overlaid models at their views (``on``) or back at their tables.

    The models are shared by all threads; only ``SubstitutableDatabase`` should call this, when
    it opens a file."""
    for model in OVERLAID_MODELS:
        model._meta.set_table_name(view_name(model) if on else _table(model))


def _q(name: str) -> str:
    """Quote an SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _literal(value: str) -> str:
    """Quote an SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _columns(model: type[Model]) -> list[str]:
    return [field.column_name for field in model._meta.sorted_fields]


def _check_schemas(schemas: Sequence[str]) -> None:
    if isinstance(schemas, str):
        raise ValueError(f"Extension schemas must be a list of names, not a string: {schemas!r}")
    if not schemas:
        raise ValueError("At least one extension schema is needed")
    for schema in schemas:
        if not isinstance(schema, str) or not _SCHEMA_NAME.fullmatch(schema):
            raise ValueError(f"Invalid extension schema name: {schema!r}")
        if schema.lower() in _RESERVED_SCHEMAS:
            raise ValueError(f"{schema!r} can't be used as an extension schema name")
    # SQLite schema names are not case sensitive
    if len({schema.lower() for schema in schemas}) != len(schemas):
        raise ValueError(f"Extension schemas must be unique: {list(schemas)!r}")


def _in_registry(expression: str) -> str:
    return f"EXISTS (SELECT 1 FROM temp.{_q(REGISTRY_TABLE)} WHERE database = {expression})"


def _raise_if(condition: str, message: str) -> str:
    return f"SELECT RAISE(ABORT, {_literal(message)}) WHERE {condition};"


def _id_in_extensions(model: type[Model], schemas: Sequence[str], expression: str) -> str:
    table = _q(_table(model))
    return (
        "("
        + " OR ".join(
            f"EXISTS (SELECT 1 FROM {_q(s)}.{table} WHERE id = {expression})" for s in schemas
        )
        + ")"
    )


def drop_overlay_sql() -> list[str]:
    """SQL removing the overlay from a connection. Dropping a view also drops its triggers.

    Safe to run when there is no overlay."""
    statements = [f"DROP VIEW IF EXISTS temp.{_q(view_name(m))};" for m in OVERLAID_MODELS]
    statements.append(f"DROP TABLE IF EXISTS temp.{_q(REGISTRY_TABLE)};")
    return statements


def _registry_sql(schemas: Sequence[str]) -> list[str]:
    statements = [
        f"CREATE TEMP TABLE {_q(REGISTRY_TABLE)} "
        "(database TEXT NOT NULL PRIMARY KEY, schema TEXT NOT NULL);"
    ]
    record = _q(_table(DatabaseRecord))
    for schema in schemas:
        # An extension has a record for every database name it owns (see
        # ``ownership_check_sql``), so the records are enough. A name in two extensions fails
        # here on the primary key.
        statements.append(
            f"INSERT INTO temp.{_q(REGISTRY_TABLE)} (database, schema) "
            f"SELECT name, {_literal(schema)} FROM {_q(schema)}.{record};"
        )
    return statements


def ownership_check_sql(schemas: Sequence[str]) -> str:
    """One query which checks that each database name belongs to exactly one file.

    Run it after attaching the extensions as ``schemas`` and before creating the overlay; it
    doesn't need the overlay. It returns no rows when all is well, and otherwise rows of
    ``(problem, database, schema, other_schema)``:

    * ``NO_RECORD``: extension ``schema`` has activities in ``database``, or exchanges whose
      output is in ``database``, but no database record for it. ``other_schema`` is ``NULL``.
    * ``DUPLICATE_NAME``: ``database`` has a record in extension ``schema``, and ``other_schema``
      (``main`` or another extension) also has a record, activities or exchange outputs for it.

    The same problem can be reported more than once, e.g. for activities and for exchanges."""
    _check_schemas(schemas)
    record = _q(_table(DatabaseRecord))
    owned_columns = (
        (record, "name"),
        (_q(_table(ActivityDataset)), "database"),
        (_q(_table(ExchangeDataset)), "output_database"),
    )
    parts = []
    for schema in schemas:
        own_records = f"SELECT name FROM {_q(schema)}.{record}"
        for table, column in owned_columns[1:]:
            parts.append(
                f"SELECT {_literal(NO_RECORD)} AS problem, d AS database, "
                f"{_literal(schema)} AS schema, NULL AS other_schema "
                f"FROM (SELECT DISTINCT {_q(column)} AS d FROM {_q(schema)}.{table}) "
                f"WHERE d NOT IN ({own_records})"
            )
        for table, column in owned_columns:
            parts.append(
                f"SELECT DISTINCT {_literal(DUPLICATE_NAME)}, {_q(column)}, {_literal(schema)}, "
                f"'main' FROM main.{table} WHERE {_q(column)} IN ({own_records})"
            )
    for position, schema in enumerate(schemas):
        for other in schemas[position + 1 :]:
            # Each extension has a record for every name it owns, or is reported above
            parts.append(
                f"SELECT {_literal(DUPLICATE_NAME)}, name, {_literal(schema)}, {_literal(other)} "
                f"FROM {_q(schema)}.{record} WHERE name IN (SELECT name FROM {_q(other)}.{record})"
            )
    return " UNION ALL ".join(parts) + ";"


def _union_view_sql(model: type[Model], schemas: Sequence[str]) -> str:
    columns = ", ".join(_q(c) for c in _columns(model))
    table = _q(_table(model))
    selects = [f"SELECT {columns} FROM main.{table}"] + [
        f"SELECT {columns} FROM {_q(s)}.{table}" for s in schemas
    ]
    return f"CREATE TEMP VIEW {_q(view_name(model))} AS " + " UNION ALL ".join(selects) + ";"


def _record_view_sql(schemas: Sequence[str]) -> str:
    """The database record view. Rows from an extension show the project-local state from
    ``extensiondatabasestate`` where it is set (not ``NULL``)."""
    columns = _columns(DatabaseRecord)
    table = _q(_table(DatabaseRecord))
    state = _q(ExtensionDatabaseState._meta.table_name)

    def extension_column(column: str) -> str:
        if column in LOCAL_STATE_COLUMNS:
            return f"COALESCE(s.{_q(column)}, e.{_q(column)}) AS {_q(column)}"
        if column == "extra":
            key = _literal(f"$.{LOCAL_STATE_EXTRA_KEY}")
            processed = f"s.{_q(LOCAL_STATE_EXTRA_KEY)}"
            return (
                f"CASE WHEN {processed} IS NULL THEN e.extra "
                f"ELSE json_set(COALESCE(e.extra, '{{}}'), {key}, {processed}) END AS extra"
            )
        return f"e.{_q(column)}"

    main_select = f"SELECT {', '.join(_q(c) for c in columns)} FROM main.{table}"
    extension_selects = [
        f"SELECT {', '.join(extension_column(c) for c in columns)} "
        f"FROM {_q(s)}.{table} AS e LEFT JOIN main.{state} AS s ON s.name = e.name"
        for s in schemas
    ]
    return (
        f"CREATE TEMP VIEW {_q(view_name(DatabaseRecord))} AS "
        + " UNION ALL ".join([main_select] + extension_selects)
        + ";"
    )


def _node_edge_triggers_sql(
    model: type[Model], database_column: str, schemas: Sequence[str]
) -> list[str]:
    """Triggers for the activity and exchange views. A row belongs to the database in
    ``database_column`` (``database``, or ``output_database`` for exchanges: an exchange
    whose input is in an extension is allowed, as that is how editable nodes link to it)."""
    view = _q(view_name(model))
    table = _q(_table(model))
    cols = _columns(model)
    column_list = ", ".join(_q(c) for c in cols)
    new_values = ", ".join(f"NEW.{_q(c)}" for c in cols)
    assignments = ", ".join(f"{_q(c)} = NEW.{_q(c)}" for c in cols)
    db_col = _q(database_column)
    trigger = view_name(model)

    insert = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_insert')} INSTEAD OF INSERT ON {view} BEGIN "
        + _raise_if(_in_registry(f"NEW.{db_col}"), EXTENSION_DATABASE_NAME)
        # SQLite would pick an id which is free in main, but maybe not in the extensions
        + " "
        + _raise_if("NEW.id IS NULL", MISSING_ID)
        + " "
        + _raise_if(_id_in_extensions(model, schemas, "NEW.id"), DUPLICATE_ID)
        + f" INSERT INTO main.{table} ({column_list}) VALUES ({new_values}); END;"
    )
    update = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_update')} INSTEAD OF UPDATE ON {view} BEGIN "
        + _raise_if(_in_registry(f"OLD.{db_col}"), READ_ONLY_DATABASE)
        + " "
        + _raise_if(_in_registry(f"NEW.{db_col}"), EXTENSION_DATABASE_NAME)
        + " "
        + _raise_if("NEW.id IS NULL", MISSING_ID)
        + " "
        + _raise_if(
            f"NEW.id IS NOT OLD.id AND {_id_in_extensions(model, schemas, 'NEW.id')}",
            DUPLICATE_ID,
        )
        + f" UPDATE main.{table} SET {assignments} WHERE id = OLD.id; END;"
    )
    delete = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_delete')} INSTEAD OF DELETE ON {view} BEGIN "
        + _raise_if(_in_registry(f"OLD.{db_col}"), READ_ONLY_DATABASE)
        + f" DELETE FROM main.{table} WHERE id = OLD.id; END;"
    )
    return [insert, update, delete]


def _json_differs(column: str) -> str:
    """Whether the JSON in ``NEW.column`` and ``OLD.column`` differs, ignoring formatting.
    Text which is not valid JSON is compared as text."""
    new, old = f"NEW.{_q(column)}", f"OLD.{_q(column)}"
    return (
        f"CASE WHEN json_valid({new}) AND json_valid({old}) "
        f"THEN json({new}) IS NOT json({old}) ELSE {new} IS NOT {old} END"
    )


def _extra_processed(expression: str) -> str:
    """The ``LOCAL_STATE_EXTRA_KEY`` value in the ``extra`` JSON ``expression``, or ``NULL``."""
    key = _literal(f"$.{LOCAL_STATE_EXTRA_KEY}")
    return f"CASE WHEN json_valid({expression}) THEN json_extract({expression}, {key}) END"


def _extra_differs() -> str:
    """Whether ``NEW.extra`` and ``OLD.extra`` differ, ignoring formatting, the order of the
    top-level keys and the project-local key ``LOCAL_STATE_EXTRA_KEY``. ``NULL`` and ``{}`` are
    the same. ``type`` is compared too, as ``json_each`` gives ``true`` and ``1`` the same
    value."""

    def keys(expression: str) -> str:
        key = _literal(f"$.{LOCAL_STATE_EXTRA_KEY}")
        stripped = f"json_remove(COALESCE({expression}, '{{}}'), {key})"
        return f"SELECT key, type, value FROM json_each({stripped})"

    new, old = "NEW.extra", "OLD.extra"
    return (
        f"CASE WHEN json_valid(COALESCE({new}, '{{}}')) AND json_valid(COALESCE({old}, '{{}}')) "
        f"THEN EXISTS ({keys(new)} EXCEPT {keys(old)}) OR EXISTS ({keys(old)} EXCEPT {keys(new)}) "
        f"ELSE {new} IS NOT {old} END"
    )


def _record_triggers_sql() -> list[str]:
    view = _q(view_name(DatabaseRecord))
    table = _q(_table(DatabaseRecord))
    state = _q(ExtensionDatabaseState._meta.table_name)
    cols = _columns(DatabaseRecord)
    column_list = ", ".join(_q(c) for c in cols)
    new_values = ", ".join(f"NEW.{_q(c)}" for c in cols)
    assignments = ", ".join(f"{_q(c)} = NEW.{_q(c)}" for c in cols)
    trigger = view_name(DatabaseRecord)

    # A change to an extension's record may only touch the project-local columns
    json_columns = {"depends", "geocollections"}
    changes = []
    for column in cols:
        if column in LOCAL_STATE_COLUMNS:
            continue
        if column == "extra":
            changes.append(_extra_differs())
        elif column in json_columns:
            changes.append(_json_differs(column))
        else:
            changes.append(f"NEW.{_q(column)} IS NOT OLD.{_q(column)}")

    # Only the local columns which this statement changes are stored: the others have the
    # extension's value in ``NEW``, and must keep following the extension. Changing a column
    # to NULL stores NULL, which shows the extension's value again. ``processed`` comes from
    # the ``extra`` JSON.
    local = {c: (f"NEW.{_q(c)}", f"OLD.{_q(c)}") for c in LOCAL_STATE_COLUMNS}
    local[LOCAL_STATE_EXTRA_KEY] = (_extra_processed("NEW.extra"), _extra_processed("OLD.extra"))

    def changed(column: str) -> str:
        new, old = local[column]
        return f"({new}) IS NOT ({old})"

    state_columns = ", ".join(["name", *(_q(c) for c in local)])
    state_values = ", ".join(
        ["OLD.name"] + [f"CASE WHEN {changed(c)} THEN {local[c][0]} END" for c in local]
    )
    state_updates = ", ".join(
        f"{_q(c)} = CASE WHEN {changed(c)} THEN {local[c][0]} ELSE {_q(c)} END" for c in local
    )
    any_changed = " OR ".join(changed(c) for c in local)
    in_extension = _in_registry("OLD.name")

    insert = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_insert')} INSTEAD OF INSERT ON {view} BEGIN "
        + _raise_if(_in_registry("NEW.name"), EXTENSION_DATABASE_NAME)
        + f" INSERT INTO main.{table} ({column_list}) VALUES ({new_values}); END;"
    )
    update = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_update')} INSTEAD OF UPDATE ON {view} BEGIN "
        + _raise_if(
            f"{in_extension} AND ({' OR '.join(f'({c})' for c in changes)})", NOT_LOCAL_STATE
        )
        + " "
        + _raise_if(f"NOT {in_extension} AND {_in_registry('NEW.name')}", EXTENSION_DATABASE_NAME)
        # Extension record: store the changed local state in the project
        + f" INSERT INTO main.{state} ({state_columns}) SELECT {state_values} "
        + f"WHERE {in_extension} AND ({any_changed})"
        + f" ON CONFLICT (name) DO UPDATE SET {state_updates};"
        # Record in main: update it
        + f" UPDATE main.{table} SET {assignments} WHERE name = OLD.name AND NOT {in_extension};"
        + " END;"
    )
    delete = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_delete')} INSTEAD OF DELETE ON {view} BEGIN "
        + _raise_if(in_extension, READ_ONLY_DATABASE)
        + f" DELETE FROM main.{table} WHERE name = OLD.name; END;"
    )
    return [insert, update, delete]


def overlay_sql(schemas: Sequence[str]) -> list[str]:
    """SQL statements which create the overlay of the extensions attached as ``schemas``.

    Run them in order on a connection to the project's ``lci/databases.db``, after attaching
    each extension under its schema name and checking the files with ``ownership_check_sql``.
    Any overlay already on the connection is dropped first, so the statements can be run again
    after attaching or detaching an extension.

    Column lists come from the peewee models, so the views follow schema changes. The
    extensions must have the same columns as the models; checking that is up to the code which
    attaches them."""
    _check_schemas(schemas)
    statements = drop_overlay_sql()
    statements += _registry_sql(schemas)
    statements.append(_union_view_sql(ActivityDataset, schemas))
    statements.append(_union_view_sql(ExchangeDataset, schemas))
    statements.append(_record_view_sql(schemas))
    statements += _node_edge_triggers_sql(ActivityDataset, "database", schemas)
    statements += _node_edge_triggers_sql(ExchangeDataset, "output_database", schemas)
    statements += _record_triggers_sql()
    return statements


# Attaching
#
# An extension file is a copy of the LCI schema holding the rows of some databases, plus two
# tables of its own: ``extension_info`` (``key``/``value`` text rows: its id, version label and
# the fingerprint of each database) and, if it needs databases from other extensions,
# ``extension_requires`` (``database``, ``extension_id``, ``fingerprint``). ``check_extensions``
# reads them, and checks the files against each other and the project's own file, on a separate
# read-only connection. ``attach_hook`` then attaches the files and creates the overlay on every
# connection of the project's file.

#: Table of an extension file with its identity, as ``key``/``value`` text rows
INFO_TABLE = "extension_info"
#: Table of an extension file with the databases it needs from other extensions
REQUIRES_TABLE = "extension_requires"
#: Key of ``INFO_TABLE``: the extension's id (a UUID), the same for every copy of the file
INFO_ID = "id"
#: Key of ``INFO_TABLE``: a version label, for people
INFO_VERSION = "version"
#: Key of ``INFO_TABLE``: a JSON object with the fingerprint of each database in the file
INFO_FINGERPRINTS = "fingerprints"
#: Start of every fingerprint, naming the hash function
FINGERPRINT_PREFIX = "sha256:"

#: SQLite's default maximum number of attached files, if the driver can't tell
_DEFAULT_ATTACH_LIMIT = 10


@dataclass(frozen=True)
class ExtensionSpec:
    """A read-only extension file to attach to a project's ``lci/databases.db``.

    * ``name`` identifies the extension in the project, and must be unique there.
    * ``immutable`` tells SQLite that the file never changes, for files on network or
      read-only volumes. SQLite then takes no locks; if the file does change while it is
      attached, reads can give wrong results or fail.
    * ``allow_main_requirements`` lets a database in the project's own file meet a requirement
      of this extension, if its fingerprint is the required one. It is checked again every
      time the project is opened."""

    name: str
    path: str | PathLike
    immutable: bool = False
    allow_main_requirements: bool = False


@dataclass(frozen=True)
class AttachedExtension:
    """An extension file which passed ``check_extensions``, and what was read from it."""

    spec: ExtensionSpec
    #: Schema name the file is attached under (``ext_1``, ``ext_2``, ...)
    schema: str
    #: URI the file is attached with
    uri: str
    #: Value of ``INFO_ID``
    id: str
    #: Value of ``INFO_VERSION``, if any
    version: str | None
    #: Names of the databases stored in the file, sorted
    databases: tuple[str, ...]
    #: Fingerprint of each database in the file, as written when it was built
    fingerprints: Mapping[str, str] = field(compare=False)
    #: ``(database, extension id, fingerprint)`` of each database the file needs from another
    #: extension
    requirements: tuple[tuple[str, str, str], ...] = ()

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def path(self) -> Path:
        return Path(self.spec.path)

    def __str__(self) -> str:
        return f"extension {self.name!r} ({self.path})"


def database_fingerprint(connection, schema: str, database: str) -> str:
    """Fingerprint of ``database`` in the file attached as ``schema`` on ``connection``.

    A SHA-256 hash of the sorted ``(code, type)`` pairs of its activities. Edges and
    characterization factors refer to activities by ``(database, code)``, so two builds of a
    database with the same fingerprint can be used in place of each other. Names, amounts and
    other data are not part of it."""
    sql = f"SELECT code, type FROM {_q(schema)}.{_q(_table(ActivityDataset))} WHERE database = ?"
    with closing(connection.execute(sql, (database,))) as cursor:
        rows = sorted(cursor, key=lambda row: (row[0], row[1] or ""))
    digest = hashlib.sha256()
    for code, type_ in rows:
        digest.update(json.dumps([code, type_], ensure_ascii=False).encode("utf8") + b"\n")
    return FINGERPRINT_PREFIX + digest.hexdigest()


def _rows(connection, sql: str, params=()) -> list:
    with closing(connection.execute(sql, params)) as cursor:
        return cursor.fetchall()


def _attach_limit(connection) -> int:
    getlimit = getattr(connection, "getlimit", None)
    category = getattr(sqlite3, "SQLITE_LIMIT_ATTACHED", None)
    if getlimit is None or category is None:
        return _DEFAULT_ATTACH_LIMIT
    return getlimit(category)


def _tables(connection, schema: str) -> set[str]:
    sql = f"SELECT name FROM {_q(schema)}.sqlite_master WHERE type = 'table'"
    return {row[0] for row in _rows(connection, sql)}


def _check_specs(specs: Sequence[ExtensionSpec]) -> None:
    names, paths = set(), {}
    for spec in specs:
        if not isinstance(spec, ExtensionSpec):
            raise TypeError(f"Extensions must be given as ExtensionSpec, not {spec!r}")
        if not isinstance(spec.name, str) or not spec.name:
            raise ValueError(f"An extension needs a name: {spec!r}")
        if spec.name in names:
            raise ExtensionError(f"Two extensions are named {spec.name!r}")
        names.add(spec.name)
        path = Path(spec.path).resolve()
        if path in paths:
            raise ExtensionError(
                f"Extensions {paths[path]!r} and {spec.name!r} are the same file ({path})"
            )
        paths[path] = spec.name


def _check_main(connection, main_path) -> None:
    needed = [
        *TABLE_NAMES.values(),
        ExtensionDatabaseState._meta.table_name,
        ZSTD_DICTIONARY_TABLE,
    ]
    existing = _tables(connection, "main")
    missing = [table for table in needed if table not in existing]
    if missing:
        raise ExtensionError(
            f"Can't attach extensions to {main_path}: it lacks the tables {', '.join(missing)}, "
            "which are created the next time it is opened and can be written"
        )


def _attach(connection, number: int, spec: ExtensionSpec) -> AttachedExtension:
    try:
        return _read_extension(connection, number, spec)
    except sqlite3.Error as err:
        # E.g. an extension table with other columns
        raise ExtensionError(
            f"Can't attach extension {spec.name!r} ({Path(spec.path)}): {err}"
        ) from err


def _read_extension(connection, number: int, spec: ExtensionSpec) -> AttachedExtension:
    from bw2data.backends.migrations import MIN_EXTENSION_SCHEMA_VERSION, SCHEMA_VERSION

    path = Path(spec.path)
    label = f"extension {spec.name!r} ({path})"
    if not path.is_file():
        raise ExtensionError(f"Can't attach {label}: the file doesn't exist")
    params = {"mode": "ro"}
    if spec.immutable:
        params["immutable"] = "1"
    uri = _sqlite_uri(path, **params)
    schema = f"ext_{number}"
    try:
        connection.execute(f"ATTACH DATABASE ? AS {_q(schema)}", (uri,)).close()
        # Reading is what fails for a file which is not an SQLite database
        tables = _tables(connection, schema)
        ((version,),) = _rows(connection, f"PRAGMA {_q(schema)}.user_version")
    except sqlite3.DatabaseError as err:
        raise ExtensionError(f"Can't attach {label}: {err}") from err
    if INFO_TABLE not in tables:
        raise ExtensionError(
            f"Can't attach {label}: it is not a bw2data extension (no {INFO_TABLE} table)"
        )
    if version > SCHEMA_VERSION:
        raise ExtensionError(
            f"Can't attach {label}: it has schema version {version}, but this version of "
            f"bw2data only reads extensions up to schema version {SCHEMA_VERSION}. It was "
            "built by a newer version of bw2data."
        )
    if version < MIN_EXTENSION_SCHEMA_VERSION:
        raise ExtensionError(
            f"Can't attach {label}: it has schema version {version}, and this version of "
            f"bw2data needs at least {MIN_EXTENSION_SCHEMA_VERSION}. An extension can't be "
            "updated; build it again with a newer version of bw2data."
        )
    missing = [t for t in (*TABLE_NAMES.values(), ZSTD_DICTIONARY_TABLE) if t not in tables]
    if missing:
        raise ExtensionError(f"Can't attach {label}: it lacks the tables {', '.join(missing)}")
    # The views select the columns of the models from every file
    for model in OVERLAID_MODELS:
        table = _table(model)
        existing = {
            row[1] for row in _rows(connection, f"PRAGMA {_q(schema)}.table_info({_q(table)})")
        }
        missing = [column for column in _columns(model) if column not in existing]
        if missing:
            raise ExtensionError(
                f"Can't attach {label}: its table {table} lacks the columns {', '.join(missing)}"
            )

    info = dict(_rows(connection, f"SELECT key, value FROM {_q(schema)}.{_q(INFO_TABLE)}"))
    extension_id = info.get(INFO_ID)
    if not isinstance(extension_id, str) or not extension_id:
        raise ExtensionError(f"Can't attach {label}: {INFO_TABLE} has no {INFO_ID!r}")
    try:
        fingerprints = json.loads(info.get(INFO_FINGERPRINTS) or "{}")
    except (TypeError, ValueError):
        fingerprints = None
    if not isinstance(fingerprints, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in fingerprints.items()
    ):
        raise ExtensionError(
            f"Can't attach {label}: {INFO_FINGERPRINTS!r} in {INFO_TABLE} is not a JSON object "
            "of strings"
        )
    record = _q(_table(DatabaseRecord))
    sql = f"SELECT name FROM {_q(schema)}.{record} ORDER BY name"
    databases = tuple(row[0] for row in _rows(connection, sql))
    requirements = ()
    if REQUIRES_TABLE in tables:
        sql = (
            f"SELECT database, extension_id, fingerprint FROM {_q(schema)}.{_q(REQUIRES_TABLE)} "
            "ORDER BY database"
        )
        requirements = tuple(tuple(row) for row in _rows(connection, sql))
    return AttachedExtension(
        spec=spec,
        schema=schema,
        uri=uri,
        id=extension_id,
        version=info.get(INFO_VERSION),
        databases=databases,
        fingerprints=MappingProxyType(fingerprints),
        requirements=requirements,
    )


def _check_unique_ids(attached: Sequence[AttachedExtension]) -> None:
    seen = {}
    for extension in attached:
        if extension.id in seen:
            raise ExtensionError(
                f"{seen[extension.id]} and {extension} are the same extension (id "
                f"{extension.id}); attach only one of them"
            )
        seen[extension.id] = extension


def _check_requirements(connection, attached: Sequence[AttachedExtension]) -> None:
    problems = _unmet_requirements(connection, attached)
    if problems:
        raise ExtensionDependencyError("\n".join(problem for _, problem in problems))


def _unmet_requirements(
    connection, attached: Sequence[AttachedExtension]
) -> list[tuple[AttachedExtension, str]]:
    """The requirements which ``attached`` and the project's own file don't meet, as
    ``(extension with the requirement, message)``."""
    by_id = {extension.id: extension for extension in attached}
    record = _q(_table(DatabaseRecord))
    main_databases = {row[0] for row in _rows(connection, f"SELECT name FROM main.{record}")}
    main_fingerprints = {}
    problems = []
    for extension in attached:
        for database, required_id, fingerprint in extension.requirements:
            provider = by_id.get(required_id)
            if (
                provider is not None
                and database in provider.databases
                and provider.fingerprints.get(database) == fingerprint
            ):
                continue
            in_main = extension.spec.allow_main_requirements and database in main_databases
            if in_main:
                if database not in main_fingerprints:
                    main_fingerprints[database] = database_fingerprint(connection, "main", database)
                if main_fingerprints[database] == fingerprint:
                    continue
            if provider is None:
                problem = f"from the extension with id {required_id}, which is not attached"
            elif database not in provider.databases:
                problem = f"from {provider}, which doesn't have it"
            else:
                problem = (
                    f"from {provider}, which has a different version of it (fingerprint "
                    f"{provider.fingerprints.get(database)})"
                )
            if in_main:
                problem += "; the database in the project has a different fingerprint too"
            problems.append(
                (
                    extension,
                    f"{extension} needs database {database!r} with fingerprint {fingerprint} "
                    + problem,
                )
            )
    return problems


def _labels(attached: Sequence[AttachedExtension]) -> dict[str, str]:
    labels = {extension.schema: str(extension) for extension in attached}
    labels["main"] = "the project"
    return labels


def _check_ownership(connection, attached: Sequence[AttachedExtension]) -> None:
    labels = _labels(attached)
    rows = _rows(connection, ownership_check_sql([extension.schema for extension in attached]))
    problems = []
    for problem, database, schema, other in sorted(set(rows), key=lambda r: tuple(map(str, r))):
        if problem == NO_RECORD:
            problems.append(
                f"{labels[schema]} has activities or exchanges in database {database!r}, but no "
                "database record for it"
            )
        else:
            problems.append(
                f"Database {database!r} is in {labels[schema]} and also in {labels[other]}; "
                "each database can only be stored in one file"
            )
    if problems:
        raise ExtensionError("\n".join(problems))


def _check_ids(connection, attached: Sequence[AttachedExtension]) -> None:
    """No activity or exchange id may be in two files: the views would show both rows."""
    labels = _labels(attached)
    schemas = ["main"] + [extension.schema for extension in attached]
    problems = []
    for model, what in ((ActivityDataset, "activity"), (ExchangeDataset, "exchange")):
        table = _q(_table(model))
        for position, first in enumerate(schemas):
            for second in schemas[position + 1 :]:
                ((count, example),) = _rows(
                    connection,
                    f"SELECT count(*), min(a.id) FROM {_q(first)}.{table} AS a "
                    f"JOIN {_q(second)}.{table} AS b ON a.id = b.id",
                )
                if count:
                    problems.append(
                        f"{count} {what} ids are used in both {labels[first]} and "
                        f"{labels[second]} (for example {example})"
                    )
    if problems:
        raise ExtensionError("\n".join(problems))


def _check_dictionaries(connection, attached: Sequence[AttachedExtension]) -> None:
    """A zstd frame names its dictionary by id, which is looked up in every file. Two files may
    have the same dictionary, but not two different ones with the same id."""
    labels = _labels(attached)
    seen = {}
    problems = []
    for schema in ["main"] + [extension.schema for extension in attached]:
        sql = f"SELECT id, content FROM {_q(schema)}.{_q(ZSTD_DICTIONARY_TABLE)}"
        for dict_id, content in _rows(connection, sql):
            content = bytes(content)
            if dict_id not in seen:
                seen[dict_id] = (content, schema)
            elif seen[dict_id][0] != content:
                problems.append(
                    f"{labels[schema]} and {labels[seen[dict_id][1]]} have different zstd "
                    f"dictionaries with the same id {dict_id}"
                )
    if problems:
        raise ExtensionError("\n".join(problems))


def check_extensions(main_path, specs: Sequence[ExtensionSpec]) -> tuple[AttachedExtension, ...]:
    """Check that the extension files ``specs`` can be attached to the project file
    ``main_path``, and read what is needed to attach them. Raises ``ExtensionError`` (or
    ``ExtensionDependencyError`` for requirements) naming every problem found by the first
    check which fails:

    * the project file has the tables the overlay needs;
    * names and files are unique, and there are no more files than SQLite can attach;
    * each file exists, is an extension (has ``INFO_TABLE`` with an id), and has a schema
      version between ``MIN_EXTENSION_SCHEMA_VERSION`` and ``SCHEMA_VERSION``;
    * no extension is given twice under different paths (same id);
    * every requirement is met: by an extension in ``specs`` with the required id which has the
      database with exactly the required fingerprint or, if the requiring extension allows it,
      by a database in the project's file with that fingerprint;
    * each database name belongs to one file (``ownership_check_sql``);
    * no activity or exchange id is in two files;
    * no two files have different zstd dictionaries with the same id.

    Uses its own read-only connection, which is closed again. An SQLite error while checking,
    e.g. because the project file is locked, is raised as ``ExtensionError`` too."""
    specs = tuple(specs)
    _check_specs(specs)
    try:
        return _check_extensions(main_path, specs)
    except sqlite3.Error as err:
        raise ExtensionError(f"Can't check the extensions of {main_path}: {err}") from err


def _check_extensions(main_path, specs: tuple[ExtensionSpec, ...]) -> tuple[AttachedExtension, ...]:
    connection = sqlite3.connect(_sqlite_uri(main_path, mode="ro"), uri=True, isolation_level=None)
    with closing(connection):
        _check_main(connection, main_path)
        limit = _attach_limit(connection)
        if len(specs) > limit:
            raise ExtensionError(
                f"Can't attach {len(specs)} extensions: SQLite attaches at most {limit} files"
            )
        attached = [_attach(connection, number, spec) for number, spec in enumerate(specs, 1)]
        _check_unique_ids(attached)
        _check_requirements(connection, attached)
        _check_ownership(connection, attached)
        _check_ids(connection, attached)
        _check_dictionaries(connection, attached)
    return tuple(attached)


def attach_hook(attached: Sequence[AttachedExtension]) -> ConnectionHook:
    """A connection hook which attaches the checked extensions ``attached`` and creates the
    overlay on a new connection to the project's file."""
    attached = tuple(attached)
    statements = overlay_sql([extension.schema for extension in attached])

    def attach_extensions(connection) -> None:
        for extension in attached:
            sql = f"ATTACH DATABASE ? AS {_q(extension.schema)}"
            connection.execute(sql, (extension.uri,)).close()
        for statement in statements:
            connection.execute(statement).close()

    return attach_extensions


# Extensions of a project
#
# A project stores the extensions attached to it (``projects.attach_extension``). When it is
# opened, an extension which can't be attached must not keep the project from opening: it is left
# out, with the extensions which need it. ``usable_extensions`` decides which ones are left.

#: Values of ``ProjectExtension.status``
AVAILABLE = "available"
MISSING_FILE = "missing file"
MISSING_REQUIREMENT = "missing requirement"
#: The file can't be attached for another reason; see ``ProjectExtension.problem``
INVALID = "invalid"


@dataclass(frozen=True)
class ProjectExtension:
    """An extension stored with a project, as listed by ``projects.extensions``."""

    name: str
    #: Absolute path of the file
    path: Path
    #: Id of the extension when it was attached to the project
    id: str
    immutable: bool
    allow_main_requirements: bool
    #: ``AVAILABLE`` if the extension is attached; otherwise why it isn't: ``MISSING_FILE``,
    #: ``MISSING_REQUIREMENT`` or ``INVALID``
    status: str
    #: What is wrong, if the extension is not available
    problem: str | None = None
    #: Names of the databases stored in the file; empty if the extension is not available
    databases: tuple[str, ...] = ()

    @property
    def available(self) -> bool:
        return self.status == AVAILABLE


def usable_extensions(
    main_path, specs: Sequence[ExtensionSpec], ids: Mapping[str, str] | None = None
) -> tuple[list[ExtensionSpec], dict[str, tuple[str, str]]]:
    """Which of the extensions ``specs`` of the project file ``main_path`` can be attached.

    Returns the specs to attach, in the given order, and ``{name: (status, problem)}`` for the
    others. An extension is left out if its file is missing or can't be read as an extension,
    if the file is not the extension with the id in ``ids`` (``{name: id}``: the file was
    replaced), or if one of its requirements is met neither by the extensions which are left
    nor, where allowed, by the project's own file. Leaving one out can leave out others which
    need it.

    The checks between files (``check_extensions``) are not made here."""
    specs = tuple(specs)
    ids = ids or {}
    _check_specs(specs)
    problems: dict[str, tuple[str, str]] = {}
    try:
        connection = sqlite3.connect(
            _sqlite_uri(main_path, mode="ro"), uri=True, isolation_level=None
        )
        with closing(connection):
            _check_main(connection, main_path)
            attached = []
            for number, spec in enumerate(specs, 1):
                path = Path(spec.path)
                if not path.is_file():
                    problems[spec.name] = (MISSING_FILE, f"the file {path} doesn't exist")
                    continue
                problem = None
                try:
                    extension = _attach(connection, number, spec)
                except ExtensionError as err:
                    problem = str(err)
                else:
                    expected = ids.get(spec.name)
                    if expected is not None and extension.id != expected:
                        problem = (
                            f"the file {path} is not the extension which was attached: it has "
                            f"the id {extension.id} instead of {expected}. If this is intended, "
                            "detach the extension and attach the file again."
                        )
                if problem is None:
                    attached.append(extension)
                    continue
                problems[spec.name] = (INVALID, problem)
                try:
                    connection.execute(f"DETACH DATABASE {_q(f'ext_{number}')}").close()
                except sqlite3.Error:
                    # It was not attached
                    pass
            while True:
                unmet = _unmet_requirements(connection, attached)
                if not unmet:
                    break
                for extension, problem in unmet:
                    problems.setdefault(extension.name, (MISSING_REQUIREMENT, problem))
                attached = [extension for extension in attached if extension.name not in problems]
    except sqlite3.Error as err:
        raise ExtensionError(f"Can't check the extensions of {main_path}: {err}") from err
    return [extension.spec for extension in attached], problems


def requirement_order(attached: Sequence[AttachedExtension]) -> list[AttachedExtension]:
    """``attached`` ordered such that each extension comes after the ones it requires, and
    otherwise as given. Raises ``ExtensionDependencyError`` if extensions require each other
    in a cycle."""
    by_id = {extension.id: extension for extension in attached}
    ordered: list[AttachedExtension] = []
    done: set[str] = set()

    def visit(extension: AttachedExtension, chain: tuple[AttachedExtension, ...]) -> None:
        if extension.id in done:
            return
        if extension in chain:
            cycle = chain[chain.index(extension) :] + (extension,)
            raise ExtensionDependencyError(
                "These extensions require each other: "
                + ", which requires ".join(repr(e.name) for e in cycle)
            )
        for _, required_id, _ in extension.requirements:
            if required_id in by_id and required_id != extension.id:
                visit(by_id[required_id], chain + (extension,))
        done.add(extension.id)
        ordered.append(extension)

    for extension in attached:
        visit(extension, ())
    return ordered


# Python-level guards: refuse changes to the data of an extension before any SQL runs. The
# triggers refuse them as well, but only once some changes (to calculation setups, search
# indexes, parameters, the local state of the database record, ...) may already be made.

#: Keys of a database's metadata which can be changed for a database stored in an extension
LOCAL_METADATA_KEYS = frozenset(LOCAL_STATE_COLUMNS + (LOCAL_STATE_EXTRA_KEY,))


def extension_of(database: str) -> AttachedExtension | None:
    """The attached extension which stores ``database``, or ``None``."""
    from bw2data.backends import sqlite3_lci_db

    if not sqlite3_lci_db.extensions:
        return None
    return sqlite3_lci_db.extension_databases().get(database)


def check_writable(database: str, what: str = "can't be changed") -> None:
    """Raise ``ReadOnlyExtensionError`` if ``database`` is stored in an attached extension.
    ``what`` ends the message, e.g. ``"can't be deleted"``."""
    extension = extension_of(database)
    if extension is not None:
        raise ReadOnlyExtensionError(
            f"Database {database!r} is stored in the read-only {extension} and {what}"
        )


def check_name_free(database: str) -> None:
    """Raise ``ReadOnlyExtensionError`` if the database name ``database`` is used by an
    attached extension, so that it can't be given to a database of the project."""
    check_writable(database, "its name can't be used for a database of the project")
