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

Some fields of a database record are local to the project: ``dirty``, ``searchable``,
``modified`` and ``extra["processed"]`` change when the project uses a database, also one stored
in an extension. Changes to these fields of an extension database are stored in the
project-local table ``extensiondatabasestate``, and the ``lci_databaserecord`` view shows them
in place of the values stored in the extension.

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

#: Fields of a database record which are local to the project, also for a database stored in
#: an extension. Columns of ``DatabaseRecord``, and keys of ``DatabaseRecord.extra``.
LOCAL_STATE_COLUMNS = ("dirty", "searchable", "modified")
LOCAL_STATE_EXTRA_KEYS = ("processed",)

#: Error messages of the triggers. SQLite only accepts a string literal in ``RAISE``.
READ_ONLY_DATABASE = (
    f"{EXTENSION_ERROR_PREFIX} this database is stored in a read-only extension and can't be "
    "changed"
)
EXTENSION_DATABASE_NAME = (
    f"{EXTENSION_ERROR_PREFIX} this database name is used by a read-only extension"
)
DUPLICATE_ID = f"{EXTENSION_ERROR_PREFIX} this id is already used in a read-only extension"
NOT_LOCAL_STATE = (
    f"{EXTENSION_ERROR_PREFIX} only the project-local fields ("
    + ", ".join(LOCAL_STATE_COLUMNS + LOCAL_STATE_EXTRA_KEYS)
    + ") of a database stored in a read-only extension can be changed"
)

_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
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
    if not schemas:
        raise ValueError("At least one extension schema is needed")
    if len(set(schemas)) != len(schemas):
        raise ValueError(f"Extension schemas must be unique: {list(schemas)!r}")
    for schema in schemas:
        if not isinstance(schema, str) or not _SCHEMA_NAME.match(schema):
            raise ValueError(f"Invalid extension schema name: {schema!r}")
        if schema.lower() in _RESERVED_SCHEMAS:
            raise ValueError(f"{schema!r} can't be used as an extension schema name")


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
    activity = _q(ActivityDataset._meta.table_name)
    for schema in schemas:
        # Database names come from the records, and also from the activities, so that a
        # database without a record is protected too. A name in two files fails on the primary
        # key; attaching checks for that first, with a better message.
        statements.append(
            f"INSERT INTO temp.{_q(REGISTRY_TABLE)} (database, schema) "
            f"SELECT name, {_literal(schema)} FROM {_q(schema)}.{record} "
            f"UNION SELECT DISTINCT database, {_literal(schema)} FROM {_q(schema)}.{activity};"
        )
    return statements


def _union_view_sql(model: type[Model], schemas: Sequence[str]) -> str:
    columns = ", ".join(_q(c) for c in _columns(model))
    table = _q(model._meta.table_name)
    selects = [f"SELECT {columns} FROM main.{table}"] + [
        f"SELECT {columns} FROM {_q(s)}.{table}" for s in schemas
    ]
    return f"CREATE TEMP VIEW {_q(view_name(model))} AS " + " UNION ALL ".join(selects) + ";"


def _record_view_sql(schemas: Sequence[str]) -> str:
    """The database record view. Rows from an extension show the project-local state from
    ``extensiondatabasestate`` where it is set."""
    columns = _columns(DatabaseRecord)
    table = _q(DatabaseRecord._meta.table_name)
    state = _q(ExtensionDatabaseState._meta.table_name)

    def extension_column(column: str) -> str:
        if column in LOCAL_STATE_COLUMNS:
            return f"COALESCE(s.{_q(column)}, e.{_q(column)}) AS {_q(column)}"
        if column == "extra":
            # ``json_patch`` replaces the keys set locally, and removes those set to null
            return (
                "CASE WHEN s.extra IS NULL THEN e.extra "
                "ELSE json_patch(COALESCE(e.extra, '{}'), s.extra) END AS extra"
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
        + " "
        + _raise_if(
            f"NEW.id IS NOT NULL AND {_id_in_extensions(model, schemas, 'NEW.id')}", DUPLICATE_ID
        )
        + f" INSERT INTO main.{table} ({column_list}) VALUES ({new_values}); END;"
    )
    update = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_update')} INSTEAD OF UPDATE ON {view} BEGIN "
        + _raise_if(_in_registry(f"OLD.{db_col}"), READ_ONLY_DATABASE)
        + " "
        + _raise_if(_in_registry(f"NEW.{db_col}"), EXTENSION_DATABASE_NAME)
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


def _json_keys(expression: str, keep: str) -> str:
    """``key, value`` rows of the JSON object ``expression``, for keys matching ``keep``."""
    return f"SELECT key, value FROM json_each(COALESCE({expression}, '{{}}')) WHERE {keep}"


def _record_triggers_sql() -> list[str]:
    view = _q(view_name(DatabaseRecord))
    table = _q(DatabaseRecord._meta.table_name)
    state = _q(ExtensionDatabaseState._meta.table_name)
    cols = _columns(DatabaseRecord)
    column_list = ", ".join(_q(c) for c in cols)
    new_values = ", ".join(f"NEW.{_q(c)}" for c in cols)
    assignments = ", ".join(f"{_q(c)} = NEW.{_q(c)}" for c in cols)
    trigger = view_name(DatabaseRecord)

    # A change to an extension's record may only touch the project-local fields. JSON columns
    # are compared after ``json()``, so that formatting doesn't count as a change; ``extra`` is
    # compared key by key, so that key order doesn't either.
    json_columns = {"depends", "geocollections"}
    fixed = [c for c in cols if c not in LOCAL_STATE_COLUMNS and c not in {"name", "extra"}]
    changes = [f"NEW.name IS NOT OLD.name"] + [
        f"json(NEW.{_q(c)}) IS NOT json(OLD.{_q(c)})"
        if c in json_columns
        else f"NEW.{_q(c)} IS NOT OLD.{_q(c)}"
        for c in fixed
    ]
    local_keys = ", ".join(_literal(k) for k in LOCAL_STATE_EXTRA_KEYS)
    not_local = f"key NOT IN ({local_keys})"
    changes.append(
        f"EXISTS ({_json_keys('NEW.extra', not_local)} EXCEPT {_json_keys('OLD.extra', not_local)})"
    )
    changes.append(
        f"EXISTS ({_json_keys('OLD.extra', not_local)} EXCEPT {_json_keys('NEW.extra', not_local)})"
    )
    # The local keys of the new ``extra``, as a JSON object (``{}`` when there are none)
    local_extra = (
        "(SELECT json_group_object(key, CASE WHEN type IN ('object', 'array') "
        "THEN json(value) ELSE value END) "
        f"FROM json_each(COALESCE(NEW.extra, '{{}}')) WHERE key IN ({local_keys}))"
    )
    state_columns = ", ".join(["name", *(_q(c) for c in LOCAL_STATE_COLUMNS), "extra"])
    state_values = ", ".join(["OLD.name", *(f"NEW.{_q(c)}" for c in LOCAL_STATE_COLUMNS), local_extra])
    state_updates = ", ".join(
        f"{_q(c)} = excluded.{_q(c)}" for c in (*LOCAL_STATE_COLUMNS, "extra")
    )
    in_extension = _in_registry("OLD.name")

    insert = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_insert')} INSTEAD OF INSERT ON {view} BEGIN "
        + _raise_if(_in_registry("NEW.name"), EXTENSION_DATABASE_NAME)
        + f" INSERT INTO main.{table} ({column_list}) VALUES ({new_values}); END;"
    )
    update = (
        f"CREATE TEMP TRIGGER {_q(trigger + '_update')} INSTEAD OF UPDATE ON {view} BEGIN "
        + _raise_if(f"{in_extension} AND ({' OR '.join(changes)})", NOT_LOCAL_STATE)
        + " "
        + _raise_if(f"NOT {in_extension} AND {_in_registry('NEW.name')}", EXTENSION_DATABASE_NAME)
        # Extension record: store the local state in the project
        + f" INSERT INTO main.{state} ({state_columns}) SELECT {state_values} WHERE {in_extension}"
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
    each extension under its schema name. Any overlay already on the connection is dropped
    first, so the statements can be run again after attaching or detaching an extension.

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
