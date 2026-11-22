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

These are pure functions; nothing here opens a connection.
"""

import re
from collections.abc import Sequence

from peewee import Model

from bw2data.backends.schema import (
    ActivityDataset,
    DatabaseRecord,
    ExchangeDataset,
    ExtensionDatabaseState,
)

#: Start of the message of every error raised by the extension triggers
EXTENSION_ERROR_PREFIX = "bw2data-extension:"

#: TEMP table with one row per database name stored in an attached extension
REGISTRY_TABLE = "lci_extension_databases"

#: Columns of ``DatabaseRecord`` which are local to the project, also for a database stored in
#: an extension
LOCAL_STATE_COLUMNS = ("dirty", "searchable", "modified")

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
MISSING_ID = (
    f"{EXTENSION_ERROR_PREFIX} a row needs an id when read-only extensions are attached"
)
NOT_LOCAL_STATE = (
    f"{EXTENSION_ERROR_PREFIX} only the project-local fields ("
    + ", ".join(LOCAL_STATE_COLUMNS)
    + ") of a database stored in a read-only extension can be changed"
)

_SCHEMA_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_RESERVED_SCHEMAS = {"main", "temp"}


def view_name(model: type[Model]) -> str:
    """Name of the TEMP view which overlays the table of ``model``."""
    return f"lci_{model._meta.table_name}"


#: The overlaid models, in the order their views are created
OVERLAID_MODELS = (ActivityDataset, ExchangeDataset, DatabaseRecord)


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
    table = _q(model._meta.table_name)
    return "(" + " OR ".join(
        f"EXISTS (SELECT 1 FROM {_q(s)}.{table} WHERE id = {expression})" for s in schemas
    ) + ")"


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
    record = _q(DatabaseRecord._meta.table_name)
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
    record = _q(DatabaseRecord._meta.table_name)
    owned_columns = (
        (record, "name"),
        (_q(ActivityDataset._meta.table_name), "database"),
        (_q(ExchangeDataset._meta.table_name), "output_database"),
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
    table = _q(model._meta.table_name)
    selects = [f"SELECT {columns} FROM main.{table}"] + [
        f"SELECT {columns} FROM {_q(s)}.{table}" for s in schemas
    ]
    return f"CREATE TEMP VIEW {_q(view_name(model))} AS " + " UNION ALL ".join(selects) + ";"


def _record_view_sql(schemas: Sequence[str]) -> str:
    """The database record view. Rows from an extension show the project-local state from
    ``extensiondatabasestate`` where it is set (not ``NULL``)."""
    columns = _columns(DatabaseRecord)
    table = _q(DatabaseRecord._meta.table_name)
    state = _q(ExtensionDatabaseState._meta.table_name)

    def extension_column(column: str) -> str:
        if column in LOCAL_STATE_COLUMNS:
            return f"COALESCE(s.{_q(column)}, e.{_q(column)}) AS {_q(column)}"
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
    table = _q(model._meta.table_name)
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


def _extra_differs() -> str:
    """Whether ``NEW.extra`` and ``OLD.extra`` differ, ignoring formatting and the order of the
    top-level keys. ``NULL`` and ``{}`` are the same. ``type`` is compared too, as ``json_each``
    gives ``true`` and ``1`` the same value."""

    def keys(expression: str) -> str:
        return f"SELECT key, type, value FROM json_each(COALESCE({expression}, '{{}}'))"

    new, old = "NEW.extra", "OLD.extra"
    return (
        f"CASE WHEN json_valid(COALESCE({new}, '{{}}')) AND json_valid(COALESCE({old}, '{{}}')) "
        f"THEN EXISTS ({keys(new)} EXCEPT {keys(old)}) OR EXISTS ({keys(old)} EXCEPT {keys(new)}) "
        f"ELSE {new} IS NOT {old} END"
    )


def _record_triggers_sql() -> list[str]:
    view = _q(view_name(DatabaseRecord))
    table = _q(DatabaseRecord._meta.table_name)
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
    # to NULL stores NULL, which shows the extension's value again.
    def changed(column: str) -> str:
        return f"NEW.{_q(column)} IS NOT OLD.{_q(column)}"

    state_columns = ", ".join(["name", *(_q(c) for c in LOCAL_STATE_COLUMNS)])
    state_values = ", ".join(
        ["OLD.name"]
        + [f"CASE WHEN {changed(c)} THEN NEW.{_q(c)} END" for c in LOCAL_STATE_COLUMNS]
    )
    state_updates = ", ".join(
        f"{_q(c)} = CASE WHEN {changed(c)} THEN NEW.{_q(c)} ELSE {_q(c)} END"
        for c in LOCAL_STATE_COLUMNS
    )
    any_changed = " OR ".join(changed(c) for c in LOCAL_STATE_COLUMNS)
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
