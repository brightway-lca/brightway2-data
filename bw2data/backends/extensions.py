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
import io
import json
import os
import re
import uuid
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from os import PathLike
from pathlib import Path
from types import MappingProxyType

from peewee import Model, sqlite3

from bw2data.backends.schema import (
    ACTIVITY_TABLE,
    EXCHANGE_TABLE,
    ActivityDataset,
    CharacterizationFactor,
    DatabaseRecord,
    ExchangeDataset,
    ExtensionDatabaseState,
    MethodRecord,
    NormalizationFactor,
    NormalizationRecord,
)
from bw2data.errors import (
    EXTENSION_ERROR_PREFIX,
    ExtensionDependencyError,
    ExtensionError,
    ReadOnlyExtensionError,
)
from bw2data.sqlite import (
    ZSTD_DICTIONARY_SIZE,
    ZSTD_DICTIONARY_TABLE,
    ZSTD_LEVEL,
    ZSTD_MAGIC,
    ConnectionHook,
    _sqlite_uri,
    zstd,
)

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
    # The tables written to in the trigger bodies are not qualified with ``main.``: SQLite
    # before 3.51 refuses qualified names there. Unqualified names are looked up in ``temp``,
    # then ``main``, then the attached files, and ``temp`` has no tables with these names.

    insert = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_insert')} INSTEAD OF INSERT ON {view} BEGIN "
        + _raise_if(_in_registry(f"NEW.{db_col}"), EXTENSION_DATABASE_NAME)
        # SQLite would pick an id which is free in main, but maybe not in the extensions
        + " "
        + _raise_if("NEW.id IS NULL", MISSING_ID)
        + " "
        + _raise_if(_id_in_extensions(model, schemas, "NEW.id"), DUPLICATE_ID)
        + f" INSERT INTO {table} ({column_list}) VALUES ({new_values}); END;"
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
        + f" UPDATE {table} SET {assignments} WHERE id = OLD.id; END;"
    )
    delete = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_delete')} INSTEAD OF DELETE ON {view} BEGIN "
        + _raise_if(_in_registry(f"OLD.{db_col}"), READ_ONLY_DATABASE)
        + f" DELETE FROM {table} WHERE id = OLD.id; END;"
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
        + f" INSERT INTO {table} ({column_list}) VALUES ({new_values}); END;"
    )
    update = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_update')} INSTEAD OF UPDATE ON {view} BEGIN "
        + _raise_if(
            f"{in_extension} AND ({' OR '.join(f'({c})' for c in changes)})", NOT_LOCAL_STATE
        )
        + " "
        + _raise_if(f"NOT {in_extension} AND {_in_registry('NEW.name')}", EXTENSION_DATABASE_NAME)
        # Extension record: store the changed local state in the project
        + f" INSERT INTO {state} ({state_columns}) SELECT {state_values} "
        + f"WHERE {in_extension} AND ({any_changed})"
        + f" ON CONFLICT (name) DO UPDATE SET {state_updates};"
        # Record in main: update it
        + f" UPDATE {table} SET {assignments} WHERE name = OLD.name AND NOT {in_extension};"
        + " END;"
    )
    delete = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_delete')} INSTEAD OF DELETE ON {view} BEGIN "
        + _raise_if(in_extension, READ_ONLY_DATABASE)
        + f" DELETE FROM {table} WHERE name = OLD.name; END;"
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
#: Optional table of an extension file with the processed datapackage of each database, as
#: ``database``/``content`` rows; ``content`` is the ``bw_processing`` zip file
DATAPACKAGE_TABLE = "extension_datapackage"
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
    #: Names of the databases whose processed datapackage is stored in the file
    datapackages: tuple[str, ...] = ()

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
    datapackages = ()
    if DATAPACKAGE_TABLE in tables:
        sql = f"SELECT database FROM {_q(schema)}.{_q(DATAPACKAGE_TABLE)} ORDER BY database"
        datapackages = tuple(row[0] for row in _rows(connection, sql))
    return AttachedExtension(
        spec=spec,
        schema=schema,
        uri=uri,
        id=extension_id,
        version=info.get(INFO_VERSION),
        databases=databases,
        fingerprints=MappingProxyType(fingerprints),
        requirements=requirements,
        datapackages=datapackages,
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


def extension_id(path) -> str:
    """The id of the extension file ``path``. Raises ``ExtensionError`` if it is missing or
    is not an extension."""
    path = Path(path)
    if not path.is_file():
        raise ExtensionError(f"The extension file {path} doesn't exist")
    try:
        connection = sqlite3.connect(_sqlite_uri(path, mode="ro"), uri=True, isolation_level=None)
        with closing(connection):
            if INFO_TABLE not in _tables(connection, "main"):
                rows = []
            else:
                sql = f"SELECT value FROM {_q(INFO_TABLE)} WHERE key = ?"
                rows = _rows(connection, sql, (INFO_ID,))
    except sqlite3.DatabaseError as err:
        raise ExtensionError(f"Can't read the extension file {path}: {err}") from err
    if not rows or not isinstance(rows[0][0], str) or not rows[0][0]:
        raise ExtensionError(f"{path} is not a bw2data extension (no {INFO_ID!r} in {INFO_TABLE})")
    return rows[0][0]


def missing_link_targets(
    main_path,
    specs: Sequence[ExtensionSpec],
    databases: Sequence[str],
    flows: Iterable[int] | None = None,
    also: Sequence[ExtensionSpec] = (),
) -> tuple[list[str], list[tuple], list[tuple]]:
    """What in the project file ``main_path`` would point at nothing with the extension files
    ``specs`` attached, as sorted lists of names:

    * the databases of the project with exchanges whose input is in one of ``databases`` but
      is not an activity in any of the files;
    * the methods and the normalizations of the project with factors for one of the activity
      ids ``flows`` which is in none of the files. Without ``flows``, the flows of all their
      factors are checked. Flows in the files ``also`` are not missing either: files which
      are not attached with ``specs`` but may be where such flows are, e.g. the files of
      extensions which are not available. The ones which can't be read are skipped.

    Uses its own read-only connection. ``specs`` should have passed ``check_extensions``."""
    specs = tuple(specs)
    databases = list(databases)
    try:
        connection = sqlite3.connect(
            _sqlite_uri(main_path, mode="ro"), uri=True, isolation_level=None
        )
        with closing(connection):
            schemas = ["main"] + [
                _attach(connection, number, spec).schema for number, spec in enumerate(specs, 1)
            ]
            activities = _q(_table(ActivityDataset))
            exchanges = _q(_table(ExchangeDataset))
            users = []
            if databases:
                exists = " OR ".join(
                    f"EXISTS (SELECT 1 FROM {_q(schema)}.{activities} AS a "
                    'WHERE a."database" = e."input_database" AND a."code" = e."input_code")'
                    for schema in schemas
                )
                marks = ", ".join("?" * len(databases))
                sql = (
                    f'SELECT DISTINCT e."output_database" FROM main.{exchanges} AS e '
                    f'WHERE e."input_database" IN ({marks}) AND NOT ({exists}) ORDER BY 1'
                )
                users = [row[0] for row in _rows(connection, sql, databases)]
            connection.execute("CREATE TEMP TABLE bw_flows (id INTEGER PRIMARY KEY)").close()
            if flows is None:
                factor_tables = [
                    _table(model)
                    for model in (CharacterizationFactor, NormalizationFactor)
                    if _table(model) in _tables(connection, "main")
                ]
                for table in factor_tables:
                    sql = f'INSERT OR IGNORE INTO temp.bw_flows SELECT "flow" FROM main.{_q(table)}'
                    connection.execute(sql).close()
            else:
                connection.executemany(
                    "INSERT OR IGNORE INTO temp.bw_flows VALUES (?)", ((int(f),) for f in flows)
                ).close()
            flow_schemas = list(schemas)
            for number, spec in enumerate(also, len(specs) + 1):
                try:
                    flow_schemas.append(_attach(connection, number, spec).schema)
                except (ExtensionError, sqlite3.Error):
                    try:
                        connection.execute(f"DETACH DATABASE {_q(f'ext_{number}')}").close()
                    except sqlite3.Error:
                        # It was not attached
                        pass
            known = " OR ".join(
                f'EXISTS (SELECT 1 FROM {_q(schema)}.{activities} AS a WHERE a."id" = f."id")'
                for schema in flow_schemas
            )
            sql = (
                "CREATE TEMP TABLE bw_missing AS "
                f"SELECT f.id FROM temp.bw_flows AS f WHERE NOT ({known})"
            )
            connection.execute(sql).close()
            names = []
            for record, factor, column in (
                (MethodRecord, CharacterizationFactor, "method_id"),
                (NormalizationRecord, NormalizationFactor, "normalization_id"),
            ):
                sql = (
                    f'SELECT r."name" FROM main.{_q(_table(record))} AS r WHERE r."id" IN '
                    f"(SELECT x.{_q(column)} FROM main.{_q(_table(factor))} AS x "
                    'WHERE x."flow" IN (SELECT id FROM temp.bw_missing))'
                )
                if _table(factor) in _tables(connection, "main"):
                    rows = _rows(connection, sql)
                else:
                    rows = []
                names.append(sorted(record.name.python_value(row[0]) for row in rows))
    except sqlite3.Error as err:
        raise ExtensionError(f"Can't check the links of {main_path}: {err}") from err
    return users, names[0], names[1]


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
                            f"the id {extension.id} instead of {expected}. If this is a new version "
                            f"of it, `projects.update_extension({spec.name!r})` takes it up."
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


# Stored datapackages
#
# An extension file can hold the processed datapackage of each of its databases
# (``DATAPACKAGE_TABLE``), written by ``build_extension``. The technosphere and biosphere
# arrays use activity ids, which the file keeps, so the datapackage is the same in every
# project, and ``Database.datapackage()`` reads it from the file instead of processing the
# database. The ids in its ``inv_geomapping_matrix`` are derived from the location names
# (``bw2data.meta.location_id``), so they are the same in every project too.


def _requirements_met_by_project(extension: AttachedExtension) -> bool:
    """Whether a requirement of the attached ``extension`` is met by a database of the
    project's own file (``allow_main_requirements``) rather than by another extension."""
    from bw2data.backends import sqlite3_lci_db

    attached_ids = {other.id for other in sqlite3_lci_db.extensions}
    return any(required_id not in attached_ids for _, required_id, _ in extension.requirements)


def has_stored_datapackage(database: str) -> bool:
    """Whether ``database`` is stored in an attached extension whose file holds its processed
    datapackage, and that datapackage can be used in this project. It can't if a requirement
    of the extension is met by a database of the project's own file: the arrays hold the
    activity ids of the database it was built with, which the project's copy doesn't have."""
    extension = extension_of(database)
    return (
        extension is not None
        and database in extension.datapackages
        and not _requirements_met_by_project(extension)
    )


def stored_datapackage(database: str, usable_only: bool = True) -> bytes | None:
    """The processed datapackage of ``database`` (a ``bw_processing`` zip file) as stored in
    its extension's file, if ``has_stored_datapackage(database)``; otherwise ``None``. With
    ``usable_only=False``, also one which can't be used in this project."""
    from bw2data.backends import sqlite3_lci_db

    extension = extension_of(database)
    if extension is None or database not in extension.datapackages:
        return None
    if usable_only and _requirements_met_by_project(extension):
        return None
    sql = f"SELECT content FROM {_q(extension.schema)}.{_q(DATAPACKAGE_TABLE)} WHERE database = ?"
    with closing(sqlite3_lci_db.execute_sql(sql, (database,))) as cursor:
        row = cursor.fetchone()
    return bytes(row[0]) if row is not None else None


# Building extensions
#
# ``build_extension`` writes databases of a project's own file into a new extension file. The
# file is a copy of the project file's schema (tables and indexes) holding only the rows of the
# exported databases; the project-local tables are left empty. It carries the search index
# tables of the exported databases with their rows, so that nothing has to be built when it is
# attached. The zstd dictionaries of the project stay behind: they hold pieces of all its data.

#: Key of ``INFO_TABLE``: the version of bw2data which built the file
INFO_BW2DATA_VERSION = "bw2data_version"
#: Key of ``INFO_TABLE``: when the file was built, as an ISO timestamp (UTC)
INFO_CREATED = "created"

#: Rows written in one ``executemany``
_COPY_CHUNK = 10_000
#: Most values a new zstd dictionary is trained on, and fewest values to train one for
_DICTIONARY_SAMPLES = 5_000
_DICTIONARY_MIN_VALUES = 500


def _schema_sql(
    connection, schema: str, skip_indexes: Iterable[str] = ()
) -> tuple[list[str], list[str]]:
    """The ``CREATE`` statements of the tables and of the indexes of ``schema``, without
    SQLite's own tables, without virtual tables (search indexes) and their shadow tables, and
    without the indexes named in ``skip_indexes``."""
    skip_indexes = set(skip_indexes)
    rows = _rows(
        connection,
        f"SELECT type, name, tbl_name, sql FROM {_q(schema)}.sqlite_master "
        "WHERE sql IS NOT NULL ORDER BY rowid",
    )
    virtual = {
        name
        for type_, name, _, sql in rows
        if type_ == "table" and sql.upper().startswith("CREATE VIRTUAL TABLE")
    }

    def skipped(table: str) -> bool:
        return (
            table.startswith("sqlite_")
            or table in virtual
            or any(table.startswith(f"{name}_") for name in virtual)
        )

    tables = [sql for type_, name, _, sql in rows if type_ == "table" and not skipped(name)]
    indexes = [
        sql
        for type_, name, table, sql in rows
        if type_ == "index" and not skipped(table) and name not in skip_indexes
    ]
    return tables, indexes


def _json_value(value):
    if value is None or isinstance(value, (dict, list)):
        return value
    return json.loads(value)


def _check_export_arguments(main_path, path: Path, databases, version, attached, overwrite):
    """The checks of ``build_extension`` which need no data; returns the database names."""
    names = [databases] if isinstance(databases, str) else list(databases)
    if not names:
        raise ValueError("No database to export")
    if not all(isinstance(name, str) for name in names) or len(set(names)) != len(names):
        raise ValueError(f"Databases must be given as distinct names: {names!r}")
    if version is not None and not isinstance(version, str):
        raise TypeError(f"The version label must be a string, not {version!r}")
    # Never to be replaced, also with ``overwrite``
    resolved = path.resolve()
    if resolved == Path(main_path).resolve():
        raise ExtensionError(f"Can't export to {path}: it is the project's own file")
    for extension in attached:
        if resolved == extension.path.resolve():
            raise ExtensionError(
                f"Can't export to {path}: it is the file of the attached {extension}; detach "
                "it first"
            )
    if path.exists() and not overwrite:
        raise FileExistsError(f"{path} exists; pass `overwrite=True` to replace it")
    return names


def check_export(
    main_path,
    path,
    databases: Sequence[str],
    version: str | None = None,
    attached: Sequence[AttachedExtension] = (),
    overwrite: bool = False,
) -> None:
    """Raise what ``build_extension`` would raise for these arguments before it writes
    anything, and do nothing else: for callers which have work to do before the export, such
    as processing the databases. Uses its own read-only connection."""
    from bw2data.backends.migrations import SCHEMA_VERSION

    names = _check_export_arguments(main_path, Path(path), databases, version, attached, overwrite)
    try:
        connection = sqlite3.connect(
            _sqlite_uri(main_path, mode="ro"), uri=True, isolation_level=None
        )
        with closing(connection):
            _export_plan(connection, "main", main_path, names, tuple(attached), SCHEMA_VERSION)
    except sqlite3.Error as err:
        raise ExtensionError(f"Can't export to {path}: {err}") from err


def _export_plan(
    connection,
    schema: str,
    main_path,
    names: list[str],
    attached: tuple[AttachedExtension, ...],
    schema_version: int,
) -> tuple[dict, dict, dict]:
    """Check that the databases ``names`` of the project file, which ``connection`` has as
    ``schema``, can be exported (``ExtensionError`` if not), and return their records, the
    databases each one links to, and the requirements of the new file."""
    marks = ", ".join("?" * len(names))
    exchanges = _q(_table(ExchangeDataset))
    records = _q(_table(DatabaseRecord))
    source = _q(schema)

    ((source_version,),) = _rows(connection, f"PRAGMA {source}.user_version")
    if source_version != schema_version:
        raise ExtensionError(
            f"Can't export from {main_path}: it has schema version {source_version}, not "
            f"{schema_version}; open the project to finish its migrations first"
        )

    # Which databases can be exported
    record_columns = [r[1] for r in _rows(connection, f"PRAGMA {source}.table_info({records})")]
    sql = f"SELECT * FROM {source}.{records} WHERE name IN ({marks})"
    found = {
        row["name"]: row
        for row in (dict(zip(record_columns, r)) for r in _rows(connection, sql, names))
    }
    stored_in = {database: e for e in attached for database in e.databases}
    problems = []
    for name in names:
        if name in stored_in:
            problems.append(
                f"database {name!r} is stored in the read-only {stored_in[name]}; copy it "
                "into the project to export it"
            )
        elif name not in found:
            problems.append(f"the project has no database {name!r}")
        elif found[name]["backend"] == "iotable":
            problems.append(
                f"database {name!r} is an IO table database, whose exchanges are not "
                "stored in the project's file"
            )
    if problems:
        raise ExtensionError("Can't export: " + "; ".join(problems))

    # An extension file can never be fixed, so it must not have two activities with one key
    duplicates = _rows(
        connection,
        f'SELECT "database", "code", count(*) FROM {source}.{_q(ACTIVITY_TABLE)} '
        f'WHERE "database" IN ({marks}) GROUP BY "database", "code" HAVING count(*) > 1 '
        "ORDER BY 1, 2 LIMIT 5",
        names,
    )
    if duplicates:
        raise ExtensionError(
            "Can't export: activity keys are used more than once, e.g. "
            + ", ".join(f"({d!r}, {c!r}) {n} times" for d, c, n in duplicates)
            + ". Delete all but one of each, or give them another code, and export again."
        )

    # What the exported databases link to
    sql = (
        f"SELECT DISTINCT output_database, input_database FROM {source}.{exchanges} "
        f"WHERE output_database IN ({marks}) AND input_database != output_database "
        "ORDER BY 1, 2"
    )
    depends = {name: set() for name in names}
    requirements = {}
    for output, input_ in _rows(connection, sql, names):
        depends[output].add(input_)
        if input_ in depends:
            continue
        provider = stored_in.get(input_)
        if provider is None:
            problems.append(
                f"database {output!r} links to database {input_!r}, which is neither "
                "exported nor in an attached extension"
            )
        elif input_ not in provider.fingerprints:
            problems.append(
                f"database {output!r} links to database {input_!r} in {provider}, which "
                "has no fingerprint for it"
            )
        else:
            requirements[input_] = (input_, provider.id, provider.fingerprints[input_])
    if problems:
        raise ExtensionError(
            "Can't export: "
            + "; ".join(problems)
            + ". An extension can only link to databases in other extensions: export the "
            "databases it links to as well, or attach them as an extension."
        )
    return found, depends, requirements


def build_extension(
    main_path,
    path,
    databases: Sequence[str],
    version: str | None = None,
    attached: Sequence[AttachedExtension] = (),
    overwrite: bool = False,
    datapackages: Mapping[str, str | PathLike] | None = None,
) -> str:
    """Write the databases ``databases`` of the project file ``main_path`` into a new extension
    file ``path``, and return the id of the new extension. Use ``projects.export_extension``,
    which calls this for the current project.

    ``attached`` are the extensions attached to the project. An exported database may link to
    databases in them: the new file then requires each of those databases, with the extension's
    id and the database's fingerprint. Raises ``ExtensionError`` if an exported database links
    to a database which is neither exported nor in an attached extension, as a read-only file
    must not depend on data which can still change; or if a database is not in the project's
    own file, or is an IO table database, whose exchanges are not stored in SQLite.

    The file gets:

    * the schema of the project file and ``user_version = SCHEMA_VERSION``;
    * the activities of the databases, with their ids, and their exchanges, with new ids;
    * their database records, with ``depends`` worked out from the exchanges, ``processed``
      removed from ``extra``, ``dirty`` cleared and ``searchable`` set to whether the file has
      an index;
    * the search index tables of the searchable databases, copied with their rows, or built if
      the project has no index for one;
    * no zstd dictionary of the project: if the values use one, they are compressed again
      with a new dictionary trained on them alone (``_recompress``);
    * ``INFO_TABLE`` (a new id, ``version``, the fingerprints, the bw2data version and the time
      it was built) and ``REQUIRES_TABLE``;
    * with ``datapackages`` (``{database: path of its processed datapackage}``, for some or
      all of the databases), ``DATAPACKAGE_TABLE`` with the content of these files. They must
      be up to date: they are stored as they are.

    Only committed data is read. The file is built next to ``path``, and only moved there once
    it is complete and passes the checks made when an extension is attached. Raises
    ``FileExistsError`` if ``path`` exists, unless ``overwrite``."""
    from bw2data.backends.migrations import SCHEMA_VERSION

    path = Path(path)
    names = _check_export_arguments(main_path, path, databases, version, attached, overwrite)
    datapackages = dict(datapackages or {})
    unknown = sorted(set(datapackages).difference(names))
    if unknown:
        raise ValueError(f"Datapackages given for databases which are not exported: {unknown}")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        try:
            extension_id = _build_extension(
                main_path,
                temporary,
                names,
                version,
                tuple(attached),
                SCHEMA_VERSION,
                datapackages,
            )
        except sqlite3.Error as err:
            raise ExtensionError(f"Can't export to {path}: {err}") from err
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return extension_id


def _build_extension(
    main_path,
    target: Path,
    names: list[str],
    version: str | None,
    attached: tuple[AttachedExtension, ...],
    schema_version: int,
    datapackages: Mapping[str, str | PathLike],
) -> str:
    from bw2data import __version__
    from bw2data.backends import LCI_PAGE_SIZE
    from bw2data.backends.migrations import LCI_INDEXES
    from bw2data.search.indices import _table_definition, create_table_sql, index_table
    from bw2data.snowflake_ids import new_snowflake_id

    marks = ", ".join("?" * len(names))
    activities = _q(_table(ActivityDataset))
    exchanges = _q(_table(ExchangeDataset))
    records = _q(_table(DatabaseRecord))

    # ``uri=True``: the project's file is attached by URI, which SQLite otherwise only accepts
    # if it was compiled to
    connection = sqlite3.connect(_sqlite_uri(target), uri=True, isolation_level=None)
    with closing(connection):
        connection.execute(f"PRAGMA main.page_size = {int(LCI_PAGE_SIZE)}")
        connection.execute("ATTACH DATABASE ? AS source", (_sqlite_uri(main_path, mode="ro"),))
        # All reads see one state of the project file
        connection.execute("BEGIN")
        found, depends, requirements = _export_plan(
            connection, "source", main_path, names, attached, schema_version
        )

        # The LCI indexes are created as schema migration 8 creates them, not copied: the
        # project's key index isn't unique if migration 8 found duplicate keys in it
        tables, indexes = _schema_sql(
            connection, "source", skip_indexes=[name for name, _ in LCI_INDEXES]
        )
        for statement in tables:
            connection.execute(statement)

        # Activities keep their ids: search indexes and characterization factors use them
        connection.execute(
            f"INSERT INTO main.{activities} SELECT * FROM source.{activities} "
            f"WHERE database IN ({marks})",
            names,
        )
        # Exchanges get new ids, as ids which aren't snowflake ids (written by older versions)
        # are the same in many files. Nothing refers to the id of an exchange in an extension.
        columns = [r[1] for r in _rows(connection, f"PRAGMA source.table_info({exchanges})")]
        position = columns.index("id")
        insert = (
            f"INSERT INTO main.{exchanges} ({', '.join(map(_q, columns))}) "
            f"VALUES ({', '.join('?' * len(columns))})"
        )
        cursor = connection.execute(
            f"SELECT {', '.join(map(_q, columns))} FROM source.{exchanges} "
            f"WHERE output_database IN ({marks})",
            names,
        )
        with closing(cursor):
            while rows := cursor.fetchmany(_COPY_CHUNK):
                connection.executemany(
                    insert,
                    [
                        row[:position] + (new_snowflake_id(),) + row[position + 1 :]
                        for row in rows
                    ],
                )
        dictionaries = _recompress(connection, (activities, exchanges))
        for statement in indexes:
            connection.execute(statement)
        for _, statement in LCI_INDEXES:
            connection.execute(statement)

        # Search indexes
        searchable = {}
        for name in names:
            flag = found[name]["searchable"]
            if flag is not None and not flag:
                searchable[name] = False
                continue
            table = index_table(name)
            rows = _rows(
                connection,
                "SELECT sql FROM source.sqlite_master WHERE type = 'table' AND name = ?",
                (table,),
            )
            connection.execute(create_table_sql(table))
            definition = (rows[0][0] or "") if rows else ""
            if definition.startswith("CREATE VIRTUAL TABLE") and definition.endswith(
                _table_definition()
            ):
                _copy_index(connection, table)
            else:
                _build_index(connection, name, table, dictionaries)
            searchable[name] = True

        # Database records
        for name in names:
            record = dict(found[name])
            extra = _json_value(record.get("extra"))
            if isinstance(extra, dict):
                extra.pop(LOCAL_STATE_EXTRA_KEY, None)
            record.update(
                depends=DatabaseRecord.depends.db_value(sorted(depends[name])),
                dirty=False,
                searchable=searchable[name],
                extra=DatabaseRecord.extra.db_value(extra or None),
            )
            connection.execute(
                f"INSERT INTO main.{records} ({', '.join(map(_q, record))}) "
                f"VALUES ({', '.join('?' * len(record))})",
                list(record.values()),
            )

        # Processed datapackages, as they are
        if datapackages:
            connection.execute(
                f"CREATE TABLE main.{_q(DATAPACKAGE_TABLE)} "
                "(database TEXT PRIMARY KEY, content BLOB NOT NULL)"
            )
            for name in names:
                if name not in datapackages:
                    continue
                content = Path(datapackages[name]).read_bytes()
                if not zipfile.is_zipfile(io.BytesIO(content)):
                    raise ExtensionError(
                        f"Can't export: the processed datapackage of database {name!r} "
                        f"({datapackages[name]}) is not a zip file"
                    )
                connection.execute(
                    f"INSERT INTO main.{_q(DATAPACKAGE_TABLE)} VALUES (?, ?)", (name, content)
                )

        # Identity and requirements
        fingerprints = {name: database_fingerprint(connection, "main", name) for name in names}
        extension_id = str(uuid.uuid4())
        info = {
            INFO_ID: extension_id,
            INFO_VERSION: version,
            INFO_FINGERPRINTS: json.dumps(fingerprints, ensure_ascii=False, sort_keys=True),
            INFO_BW2DATA_VERSION: __version__,
            INFO_CREATED: datetime.now(timezone.utc).isoformat(),
        }
        connection.execute(f"CREATE TABLE main.{_q(INFO_TABLE)} (key TEXT PRIMARY KEY, value TEXT)")
        connection.executemany(
            f"INSERT INTO main.{_q(INFO_TABLE)} VALUES (?, ?)",
            [(key, value) for key, value in info.items() if value is not None],
        )
        connection.execute(
            f"CREATE TABLE main.{_q(REQUIRES_TABLE)} "
            "(database TEXT PRIMARY KEY, extension_id TEXT NOT NULL, fingerprint TEXT NOT NULL)"
        )
        connection.executemany(
            f"INSERT INTO main.{_q(REQUIRES_TABLE)} VALUES (?, ?, ?)",
            [requirements[key] for key in sorted(requirements)],
        )
        connection.execute(f"PRAGMA main.user_version = {int(schema_version)}")
        connection.execute("COMMIT")
        connection.execute("DETACH DATABASE source")

    # The file must attach on its own: the checks of ``check_extensions`` which look at one file,
    # with an empty project file
    check = sqlite3.connect(":memory:", uri=True, isolation_level=None)
    with closing(check):
        for statement in tables:
            check.execute(statement)
        extension = _attach(check, 1, ExtensionSpec("export", target))
        problems = [
            f"database {problem[1]!r}: {problem[0]}"
            for problem in _rows(check, ownership_check_sql([extension.schema]))
        ]
        if sorted(extension.databases) != sorted(names):
            problems.append(f"it has the databases {list(extension.databases)}")
        # As they are in the finished file, not as they were written into ``INFO_TABLE``
        for name in names:
            found_fingerprint = database_fingerprint(check, extension.schema, name)
            if extension.fingerprints.get(name) != found_fingerprint:
                problems.append(
                    f"database {name!r} has the fingerprint {found_fingerprint}, but "
                    f"{extension.fingerprints.get(name)} is stored"
                )
    if problems:
        raise ExtensionError(
            "The exported file doesn't pass the checks for extensions: " + "; ".join(problems)
        )
    return extension_id


def _dictionary_ids(connection, table: str) -> set[int]:
    """Ids of the zstd dictionaries which the ``data`` values of the quoted ``table`` in
    ``main`` were compressed with, read from the frame headers."""
    # A frame header is at most 18 bytes
    sql = f"SELECT substr(data, 1, 18) FROM main.{table}"
    ids = set()
    with closing(connection.execute(sql)) as cursor:
        for (header,) in cursor:
            header = bytes(header)
            if header[:4] == ZSTD_MAGIC:
                ids.add(zstd.get_frame_info(header).dictionary_id)
    ids.discard(0)
    return ids


def _recompress(connection, tables: Sequence[str]) -> dict[int, bytes]:
    """Free the copied ``data`` values of the quoted ``tables`` in ``main`` from the zstd
    dictionaries of the project, and return the content of the dictionaries of the new file
    by id: at most one.

    A dictionary holds pieces of the data it was trained on, also of databases which are not
    exported, and values written later are compressed with it too. So no dictionary of the
    project goes into the file. If any copied value uses one, a new dictionary is trained on
    the copied values alone, and all of them are compressed again with it; or without a
    dictionary, if zstd can't train one from them. Does nothing if no value uses a
    dictionary."""
    used = set().union(*(_dictionary_ids(connection, table) for table in tables))
    if not used:
        return {}
    old = {}
    for dict_id in used:
        rows = _rows(
            connection,
            f"SELECT content FROM source.{_q(ZSTD_DICTIONARY_TABLE)} WHERE id = ?",
            (dict_id,),
        )
        if not rows:
            raise ExtensionError(
                f"Can't export: values of the databases were compressed with zstd dictionary "
                f"{dict_id}, which is not in the project's file"
            )
        old[dict_id] = zstd.ZstdDict(bytes(rows[0][0]))

    def content(frame: bytes) -> bytes:
        dict_id = zstd.get_frame_info(frame).dictionary_id
        return zstd.decompress(frame, zstd_dict=old[dict_id] if dict_id else None)

    counts = {
        table: _rows(connection, f"SELECT count(*) FROM main.{table}")[0][0] for table in tables
    }
    total = sum(counts.values())
    samples = []
    for table in tables:
        limit = max(1, round(_DICTIONARY_SAMPLES * counts[table] / total))
        sql = f"SELECT data FROM main.{table} ORDER BY random() LIMIT ?"
        for (value,) in _rows(connection, sql, (limit,)):
            value = bytes(value)
            if value[:4] == ZSTD_MAGIC:
                samples.append(content(value))
    new = None
    if total >= _DICTIONARY_MIN_VALUES:
        try:
            new = zstd.train_dict(samples, ZSTD_DICTIONARY_SIZE)
        except (zstd.ZstdError, ValueError):
            # Too few or too small samples
            new = None
    compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL, zstd_dict=new)
    for table in tables:
        last = None
        while True:
            where, params = ("WHERE id > ?", (last,)) if last is not None else ("", ())
            rows = _rows(
                connection,
                f"SELECT id, data FROM main.{table} {where} ORDER BY id LIMIT {_COPY_CHUNK}",
                params,
            )
            if not rows:
                break
            last = rows[-1][0]
            connection.executemany(
                f"UPDATE main.{table} SET data = ? WHERE id = ?",
                [
                    (
                        compressor.compress(content(value), mode=zstd.ZstdCompressor.FLUSH_FRAME),
                        id_,
                    )
                    for id_, value in ((id_, bytes(value)) for id_, value in rows)
                    if value[:4] == ZSTD_MAGIC
                ],
            )
    if new is None:
        return {}
    connection.execute(
        f"INSERT INTO main.{_q(ZSTD_DICTIONARY_TABLE)} (id, content, created, active) "
        "VALUES (?, ?, ?, 0)",
        (new.dict_id, new.dict_content, datetime.now(timezone.utc).isoformat()),
    )
    return {new.dict_id: new.dict_content}


def _decode(value, dictionaries: Mapping[int, bytes], cache: dict):
    """A stored ``CompressedJSONField`` value of the exported file, as Python data.
    ``dictionaries`` has the content of the file's zstd dictionaries."""
    from bw2data import codec

    value = bytes(value)
    if value[:4] != ZSTD_MAGIC:
        return codec.safe_pickle_loads(value)
    dict_id = zstd.get_frame_info(value).dictionary_id
    if dict_id and dict_id not in cache:
        cache[dict_id] = zstd.ZstdDict(dictionaries[dict_id])
    return codec.loads(zstd.decompress(value, zstd_dict=cache[dict_id] if dict_id else None))


def _copy_index(connection, table: str) -> None:
    """Copy the rows of the search index ``table`` from ``source`` into the same, empty table
    in ``main``. The rows of an FTS5 table are stored in its shadow tables."""
    sql = "SELECT name FROM source.sqlite_master WHERE type = 'table' AND name LIKE ? ESCAPE '\\'"
    pattern = table.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%") + "\\_%"
    for (shadow,) in _rows(connection, sql, (pattern,)):
        connection.execute(f"DELETE FROM main.{_q(shadow)}")
        connection.execute(f"INSERT INTO main.{_q(shadow)} SELECT * FROM source.{_q(shadow)}")


def _build_index(
    connection, database: str, table: str, dictionaries: Mapping[int, bytes]
) -> None:
    """Index the activities of ``database`` in the new table ``table`` in ``main``, from the
    rows which were copied into the file."""
    from bw2data.search.indices import FIELDS, format_dataset

    columns = ", ".join(_q(field) for field in FIELDS)
    sql = (
        f"INSERT INTO main.{_q(table)} (rowid, {columns}) "
        f"VALUES (?, {', '.join('?' * len(FIELDS))})"
    )
    stored = _rows(
        connection,
        f"SELECT id, code, data FROM main.{_q(_table(ActivityDataset))} WHERE database = ?",
        (database,),
    )
    rows, cache = [], {}
    for id_, code, data in stored:
        data = _decode(data, dictionaries, cache) if data is not None else {}
        values = format_dataset({**(data or {}), "code": code})
        rows.append((id_, *(values[field] for field in FIELDS)))
    connection.executemany(sql, rows)
    # One segment: smaller, and faster to search
    connection.execute(f"INSERT INTO main.{_q(table)} ({_q(table)}) VALUES ('optimize')")
