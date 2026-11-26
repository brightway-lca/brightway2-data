"""Versioned schema migrations for the per-project ``lci/databases.db``.

The schema version is stored in the file itself, in ``PRAGMA user_version``. Version ``0``
means the file has never been migrated (every file written before schema versioning).
Version ``N`` means that migration steps ``1`` to ``N`` have been applied.

Peewee's ``create_tables`` creates tables which don't exist yet, so new tables need no
migration step. Migration steps are for everything ``create_tables`` can't do: moving data
in from files in the project directory, or changing tables which already exist.

``migrate`` runs every time the file is opened, i.e. on import and on every project switch.
When the file is up to date, or newer than this bw2data, this costs one ``PRAGMA`` read and
takes no write lock. Otherwise all pending steps run in one ``IMMEDIATE`` transaction,
together with the version update: a step which fails leaves the file unchanged, and two
processes opening the same project don't both migrate.

If the write lock can't be taken (another process is writing, or the file is read-only), the
migration is postponed with a warning and tried again the next time the file is opened, so
that such a project can still be opened and read.

Rules for migration steps:

* Steps are only ever appended. Never change or remove a released step: its number is
  stored in users' files.
* A step gets the peewee database and the project directory. It must be written in raw SQL
  and not with the peewee models, which describe the *current* schema and change between
  releases. It must use the ``main`` schema explicitly, as other files can be attached to the
  connection.
* A step must not open or close transactions; it already runs inside one, and in a
  savepoint of its own. It may use nested savepoints (``db.atomic()``).
* A step which moves data in from a legacy file keeps that file, so older versions of
  bw2data can still open the project.
* A step which can't be applied yet (not enough free disk space, data it refuses to read)
  raises ``MigrationPostponed``. Its changes are rolled back, the steps before it are
  committed, and it is tried again the next time the file is opened.
* A step can return a function, which is called with the database after the transaction
  is committed, for work which can't run inside a transaction, like ``VACUUM``. It must not
  raise: the migration is already committed.
"""

import datetime
import json
import numbers
import pickle
import shutil
import warnings
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from peewee import Database, InterfaceError, OperationalError, ProgrammingError

from bw2data import codec
from bw2data.configuration import config
from bw2data.logs import stdout_feedback_logger
from bw2data.sqlite import (
    ZSTD_DICTIONARY_SIZE,
    ZSTD_LEVEL,
    _cannot_write,
    _user_stacklevel,
    zstd,
    zstd_dictionaries,
)

#: What a step can return: called with the database after the migration is committed
AfterCommit = Callable[[Database], None]


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    apply: Callable[[Database, Path], Optional[AfterCommit]]


class MigrationPostponed(Exception):
    """Raised by a migration step which can't be applied yet. The message says why, and what
    the user can do about it; it is shown as a warning."""


def _preferences_to_sqlite(db: Database, project_dir: Path) -> None:
    """Move ``preferences.pickle`` into the ``preference`` table.

    Preferences which are already in the table are kept as they are; this happens when the
    migration was postponed and preferences were set in the meantime. Entries with keys which
    are not strings can't be stored in the table; they are skipped with a warning, and remain
    in ``preferences.pickle``. An unreadable file is skipped with a warning, and the project
    starts with default preferences."""
    path = project_dir / "preferences.pickle"
    if not path.is_file():
        return

    try:
        with open(path, "rb") as f:
            legacy = dict(pickle.load(f))
    except Exception as err:
        warnings.warn(
            f"Can't read {path} ({err}); starting with default preferences.",
            stacklevel=2,
        )
        return

    # Same storage format as ``bw2data.sqlite.PickleField``. One statement per row, so there
    # is no limit on the number of preferences.
    rows = [(k, pickle.dumps(v, protocol=4)) for k, v in legacy.items() if isinstance(k, str)]
    skipped = [k for k in legacy if not isinstance(k, str)]
    for row in rows:
        db.execute_sql("INSERT OR IGNORE INTO main.preference (key, value) VALUES (?, ?)", row)
    if skipped:
        warnings.warn(
            f"Preferences with keys which are not strings can't be migrated, and are only kept "
            f"in {path}: {skipped!r}",
            stacklevel=2,
        )
    stdout_feedback_logger.info(
        "Migrated %s preference(s) from %s to SQLite; the file is kept as a backup",
        len(rows),
        path,
    )


def _geomapping_without_autoincrement(db: Database, project_dir: Path) -> None:
    """Rebuild ``geocollection`` and ``geomappingrecord`` without ``AUTOINCREMENT``, keeping all
    rows and ids.

    New ids in both tables are snowflake ids, assigned by bw2data. Existing ids are kept, as
    processed datapackages refer to geomapping ids. Tables created after the switch to
    snowflake ids have no ``AUTOINCREMENT`` and are left alone.

    Both tables are rebuilt together: renaming ``geocollection`` makes SQLite change the
    foreign key of ``geomappingrecord`` to point at the renamed table, so the old
    ``geomappingrecord`` can't be kept either. The new tables and indices are the ones peewee
    creates for a new project; ``tests/unit/test_geomapping_sqlite.py`` checks that both stay
    identical."""
    definitions = dict(
        db.execute_sql(
            "SELECT name, sql FROM main.sqlite_master WHERE type = 'table' "
            "AND name IN ('geocollection', 'geomappingrecord')"
        ).fetchall()
    )
    if not any("AUTOINCREMENT" in sql.upper() for sql in definitions.values()):
        return

    # Indices keep their names when their table is renamed; drop them so that the names are
    # free for the new tables
    for index in (
        "geomappingrecord_collection_location",
        "geomappingrecord_geocollection_id",
        "geocollection_name",
    ):
        db.execute_sql(f'DROP INDEX IF EXISTS main."{index}"')
    # Child first: it then keeps pointing at the old parent until both old tables are gone
    db.execute_sql('ALTER TABLE main."geomappingrecord" RENAME TO "geomappingrecord_old"')
    db.execute_sql('ALTER TABLE main."geocollection" RENAME TO "geocollection_old"')

    db.execute_sql(
        'CREATE TABLE main."geocollection" ("id" INTEGER NOT NULL PRIMARY KEY, '
        '"name" TEXT NOT NULL, "extra" TEXT)'
    )
    db.execute_sql(
        'INSERT INTO main."geocollection" ("id", "name", "extra") '
        'SELECT "id", "name", "extra" FROM main."geocollection_old"'
    )
    db.execute_sql(
        'CREATE TABLE main."geomappingrecord" ("id" INTEGER NOT NULL PRIMARY KEY, '
        '"geocollection_id" INTEGER, "location" TEXT NOT NULL, FOREIGN KEY ("geocollection_id") '
        'REFERENCES "geocollection" ("id") ON DELETE RESTRICT)'
    )
    db.execute_sql(
        'INSERT INTO main."geomappingrecord" ("id", "geocollection_id", "location") '
        'SELECT "id", "geocollection_id", "location" FROM main."geomappingrecord_old"'
    )
    # Child first, so that no row refers to the old parent when it is dropped. Dropping also
    # removes each table's row in ``sqlite_sequence``.
    db.execute_sql('DROP TABLE main."geomappingrecord_old"')
    db.execute_sql('DROP TABLE main."geocollection_old"')

    db.execute_sql(
        'CREATE UNIQUE INDEX main."geocollection_name" ON "geocollection" ("name")'
    )
    db.execute_sql(
        'CREATE INDEX main."geomappingrecord_geocollection_id" '
        'ON "geomappingrecord" ("geocollection_id")'
    )
    db.execute_sql(
        'CREATE UNIQUE INDEX main."geomappingrecord_collection_location" '
        'ON "geomappingrecord" (COALESCE("geocollection_id", 0), "location")'
    )


#: Columns of ``databaserecord`` with their own column at schema version 3; other keys go
#: into ``extra``. Fixed here, and not read from the model, as the step must not change when
#: the model does.
_DATABASERECORD_COLUMNS = (
    "backend",
    "depends",
    "dirty",
    "geocollections",
    "modified",
    "number",
    "searchable",
    "version",
)
#: ``JSONField``, ``IntegerField`` and ``BooleanField`` columns of ``databaserecord``; the
#: others are ``TextField`` columns
_DATABASERECORD_JSON = frozenset({"depends", "geocollections"})
_DATABASERECORD_INTEGER = frozenset({"number", "version"})
_DATABASERECORD_BOOLEAN = frozenset({"dirty", "searchable"})


def _json_value(value):
    """``value`` as stored by ``bw2data.sqlite.JSONField``."""
    if value is None:
        return None
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        default=lambda x: x.isoformat() if hasattr(x, "isoformat") else x,
    )


def _integer_value(value):
    """``value`` as stored by a peewee ``IntegerField``: an integer if it can be converted to
    one, otherwise unchanged. Values which SQLite can't store (lists, dicts) are stored as
    JSON text."""
    try:
        return int(value)
    except ValueError:
        return value
    except TypeError:
        return None if value is None else _json_value(value)


def _databaserecord_value(column: str, value):
    """``value`` as stored by the model field of ``column``. Legacy values of the wrong type
    are converted as the field would do it, so that every value can be stored."""
    if value is None:
        return None
    if column in _DATABASERECORD_JSON:
        return _json_value(value)
    if column in _DATABASERECORD_INTEGER:
        return _integer_value(value)
    if column in _DATABASERECORD_BOOLEAN:
        return bool(value)
    return value if isinstance(value, str) else str(value)


def _databases_json_to_sqlite(db: Database, project_dir: Path) -> None:
    """Move ``databases.json`` into the ``databaserecord`` table.

    Databases which are already in the table are kept as they are; this happens when the
    migration was postponed and databases were registered in the meantime, and for projects
    opened by a development version of bw2data which moved the file on first access. Values
    of the wrong type (e.g. a list as ``version``) are converted to something which can be
    stored. An unreadable file, entries which aren't dictionaries and entries which still
    can't be stored are skipped with a warning; the file is kept as a backup either way."""
    path = project_dir / "databases.json"
    if not path.is_file():
        return

    try:
        with open(path, encoding="utf-8") as f:
            legacy = json.load(f)
        if not isinstance(legacy, dict):
            raise ValueError("not a JSON object")
    except Exception as err:
        warnings.warn(
            f"Can't read {path} ({err}); starting with no registered databases.",
            stacklevel=2,
        )
        return

    migrated, skipped = 0, []
    columns = ("name", *_DATABASERECORD_COLUMNS, "extra")
    sql = 'INSERT OR IGNORE INTO main."databaserecord" ({}) VALUES ({})'.format(
        ", ".join(f'"{c}"' for c in columns), ", ".join("?" * len(columns))
    )
    for name, metadata in legacy.items():
        if not isinstance(metadata, dict):
            skipped.append(name)
            continue
        metadata = dict(metadata)
        values = [name]
        for column in _DATABASERECORD_COLUMNS:
            values.append(_databaserecord_value(column, metadata.pop(column, None)))
        values.append(_json_value(metadata or None))
        try:
            migrated += db.execute_sql(sql, values).rowcount
        except (InterfaceError, ProgrammingError, OverflowError, ValueError, TypeError):
            # A value which SQLite can't store, e.g. an integer with more than 64 bits
            skipped.append(name)
    if skipped:
        warnings.warn(
            f"Entries in {path} which are not dictionaries, or have values which can't be "
            f"stored, can't be migrated, and are only kept in the file: {skipped!r}",
            stacklevel=2,
        )
    stdout_feedback_logger.info(
        "Migrated %s database(s) from %s to SQLite; the file is kept as a backup",
        migrated,
        path,
    )


#: Legacy LCIA metadata files, their tables, and the metadata keys with their own column in
#: each table at schema version 4 (``abbreviation`` is a text column in all tables)
_LCIA_METADATA = (
    ("methods.json", "method", ("num_cfs", "geocollections")),
    ("weightings.json", "weighting", ()),
    ("normalizations.json", "normalization", ()),
)


def _tuple_name_value(name) -> str:
    """``name`` as stored by ``bw2data.sqlite.TupleNameField``."""
    return json.dumps(list(name), ensure_ascii=False)


def _lcia_metadata_to_sqlite(db: Database, project_dir: Path) -> None:
    """Move ``methods.json``, ``weightings.json`` and ``normalizations.json`` into the
    ``method``, ``weighting`` and ``normalization`` tables.

    Objects which are already in a table are kept as they are; this happens when the
    migration was postponed and objects were registered in the meantime. Rows get new
    snowflake ids; the names are the natural key. Entries which can't be migrated (names
    which aren't non-empty lists or can't be dictionary keys, metadata which isn't a
    dictionary, values which can't be stored) are skipped with a warning, as is an
    unreadable file. The files are kept as backups.

    ``has_data`` is ``0``: the data is still in ``intermediate/*.pickle``."""
    # Imported here: only this step needs it, and it has no dependency on the models
    from bw2data.snowflake_ids import snowflake_id_generator

    for filename, table, columns in _LCIA_METADATA:
        path = project_dir / filename
        if not path.is_file():
            continue

        try:
            with open(path, encoding="utf-8") as f:
                legacy = json.load(f)
            if not isinstance(legacy, list):
                raise ValueError("not a JSON list")
        except Exception as err:
            warnings.warn(
                f"Can't read {path} ({err}); starting with no registered {table}s.",
                stacklevel=2,
            )
            continue

        # Same as ``CompoundJSONDict.unpack``: a later entry for the same name wins
        entries, skipped = {}, []
        for entry in legacy:
            if (
                isinstance(entry, list)
                and len(entry) == 2
                and isinstance(entry[0], list)
                and entry[0]
                and isinstance(entry[1], dict)
            ):
                try:
                    entries[tuple(entry[0])] = dict(entry[1])
                    continue
                except TypeError:
                    # A name with a list or dictionary in it can't be a dictionary key, and
                    # so can't be a name in the registry either
                    pass
            skipped.append(entry[0] if isinstance(entry, list) and entry else entry)

        migrated = 0
        all_columns = ("id", "name", "abbreviation", *columns, "extra", "has_data")
        sql = 'INSERT OR IGNORE INTO main."{}" ({}) VALUES ({})'.format(
            table, ", ".join(f'"{c}"' for c in all_columns), ", ".join("?" * len(all_columns))
        )
        for name, metadata in entries.items():
            values = [next(snowflake_id_generator), _tuple_name_value(name)]
            abbreviation = metadata.pop("abbreviation", None)
            values.append(None if abbreviation is None else str(abbreviation))
            for column in columns:
                if column == "num_cfs":
                    values.append(_integer_value(metadata.pop(column, None)))
                else:
                    values.append(_json_value(metadata.pop(column, None)))
            values.append(_json_value(metadata or None))
            values.append(0)
            try:
                migrated += db.execute_sql(sql, values).rowcount
            except (InterfaceError, ProgrammingError, OverflowError, ValueError, TypeError):
                # A value which SQLite can't store, e.g. an integer with more than 64 bits
                skipped.append(list(name))

        if skipped:
            warnings.warn(
                f"Entries in {path} which aren't a name (list of strings) and metadata "
                f"(dictionary), or have values which can't be stored, can't be migrated, and "
                f"are only kept in the file: {skipped[:20]!r}",
                stacklevel=2,
            )
        stdout_feedback_logger.info(
            "Migrated %s %s(s) from %s to SQLite; the file is kept as a backup",
            migrated,
            table,
            path,
        )


#: Registry table, factor table, foreign key column and kind of row, for migration 5
_LCIA_FACTORS = (
    ("method", "characterizationfactor", "method_id", "method"),
    ("weighting", "weightingfactor", "weighting_id", "weighting"),
    ("normalization", "normalizationfactor", "normalization_id", "normalization"),
)


class _CantMigrate(ValueError):
    pass


def _m5_plain(value):
    return value.item() if hasattr(value, "item") and not isinstance(value, (int, float)) else value


def _m5_amount(amount) -> float:
    """``amount`` as stored in the ``amount`` column. SQLite stores NaN as ``NULL``, which
    would be read back as a missing amount, so NaN can't be migrated."""
    if not isinstance(amount, numbers.Number) or isinstance(amount, bool):
        raise _CantMigrate(f"amount {amount!r} is not a number")
    try:
        result = float(amount)
    except TypeError:
        raise _CantMigrate(f"amount {amount!r} is not a real number") from None
    if result != result:
        raise _CantMigrate("an amount is NaN (not a number)")
    return result


def _m5_value(value) -> tuple:
    """``(amount, uncertainty JSON)`` columns of a factor value, as written by
    ``bw2data.ia_data_store.value_as_columns`` and ``JSONField``."""
    if isinstance(value, dict):
        amount = value.get("amount")
        rest = {k: _m5_plain(v) for k, v in value.items() if k != "amount"}
        return (None if amount is None else _m5_amount(amount), _json_value(rest))
    if isinstance(value, numbers.Number) and not isinstance(value, bool):
        return (_m5_amount(value), None)
    raise _CantMigrate(f"value {value!r} is not a number or an uncertainty dictionary")


def _m5_location(row) -> tuple:
    """``(geocollection, location)`` columns of a CF row."""
    if len(row) < 3 or row[2] is None or row[2] == "":
        return (None, None)
    location = row[2]
    if isinstance(location, str):
        return (None, location)
    if (
        isinstance(location, (tuple, list))
        and len(location) == 2
        and isinstance(location[0], str)
        and location[0]
    ):
        geocollection, feature = location
        if isinstance(feature, numbers.Integral) and not isinstance(feature, bool):
            # Integer feature ids become strings, as in the migration of ``geomapping.pickle``
            feature = str(int(feature))
        if isinstance(feature, str) and feature:
            return (geocollection, feature)
    raise _CantMigrate(f"location {location!r} is not a string or a pair of strings")


def _m5_flow(db: Database, flow, cache: dict) -> int:
    """Activity id of ``flow`` (an id or a ``(database, code)`` key)."""
    if isinstance(flow, numbers.Integral) and not isinstance(flow, bool):
        # Also NumPy integers
        return int(flow)
    if isinstance(flow, (tuple, list)) and len(flow) == 2:
        key = tuple(flow)
        if key not in cache:
            found = db.execute_sql(
                'SELECT "id" FROM main."activitydataset" WHERE "database" = ? AND "code" = ?',
                key,
            ).fetchone()
            cache[key] = None if found is None else found[0]
        if cache[key] is None:
            raise _CantMigrate(f"flow {key!r} doesn't exist")
        return cache[key]
    raise _CantMigrate(f"flow {flow!r} is not an activity id or a (database, code) key")


def _m5_rows(db: Database, kind: str, data, cache: dict) -> list[tuple]:
    """Factor columns, without the owner and position, for the legacy ``data`` of a
    ``kind``."""
    if not isinstance(data, (list, tuple)):
        raise _CantMigrate("the data is not a list")
    rows = []
    for line in data:
        if kind == "weighting":
            rows.append(_m5_value(line))
            continue
        lengths = (2, 3) if kind == "method" else (2,)
        if not isinstance(line, (tuple, list)) or len(line) not in lengths:
            raise _CantMigrate(f"row {line!r} has the wrong form")
        columns = (_m5_flow(db, line[0], cache), *_m5_value(line[1]))
        if kind == "method":
            columns += _m5_location(line)
        rows.append(columns)
    return rows


def _lcia_factors_to_sqlite(db: Database, project_dir: Path) -> None:
    """Move the data of LCIA methods, weightings and normalizations from
    ``intermediate/<abbreviation>.pickle`` into the ``characterizationfactor``,
    ``weightingfactor`` and ``normalizationfactor`` tables, and set ``has_data``.

    Only objects with ``has_data = 0`` and a pickle are migrated. Flows given as
    ``(database, code)`` keys (normalizations, and methods written before bw2data 4) are
    stored as activity ids. An object whose pickle can't be read, or has rows which can't be
    stored (a flow which doesn't exist, a location which isn't a string or a pair of
    strings, an amount which is NaN, ...), is skipped with a warning: it keeps ``has_data = 0``, so its data can't be
    loaded until it is written again, e.g. from the pickle, which is kept as a backup like
    all others."""
    from contextlib import closing

    cache = {}
    for table, factor_table, owner, kind in _LCIA_FACTORS:
        records = db.execute_sql(
            f'SELECT "id", "name", "abbreviation" FROM main."{table}" '
            'WHERE NOT "has_data" AND "abbreviation" IS NOT NULL ORDER BY "id"'
        ).fetchall()
        migrated, skipped = 0, []
        for id_, name, abbreviation in records:
            path = project_dir / "intermediate" / f"{abbreviation}.pickle"
            if not path.is_file():
                continue
            try:
                with open(path, "rb") as f:
                    rows = _m5_rows(db, kind, pickle.load(f), cache)
            except Exception as err:
                skipped.append(f"{name} ({path.name}: {err})")
                continue
            columns = ["position", "amount", "uncertainty", owner]
            if kind != "weighting":
                columns.insert(1, "flow")
            if kind == "method":
                columns += ["geocollection", "location"]
            sql = 'INSERT INTO main."{}" ({}) VALUES ({})'.format(
                factor_table, ", ".join(f'"{c}"' for c in columns), ", ".join("?" * len(columns))
            )

            def values(rows=rows):
                for position, row in enumerate(rows):
                    if kind == "weighting":
                        yield (position, *row, id_)
                    elif kind == "method":
                        flow, amount, uncertainty, geocollection, location = row
                        yield (position, flow, amount, uncertainty, id_, geocollection, location)
                    else:
                        yield (position, *row, id_)

            try:
                # A savepoint, so that an object which can't be stored changes nothing
                with db.atomic():
                    with closing(db.cursor()) as cursor:
                        cursor.executemany(sql, values())
                    db.execute_sql(
                        f'UPDATE main."{table}" SET "has_data" = 1 WHERE "id" = ?', (id_,)
                    )
            except (OverflowError, ValueError, TypeError) as err:
                skipped.append(f"{name} ({path.name}: {err})")
                continue
            migrated += 1

        if skipped:
            warnings.warn(
                f"The data of these {table}s can't be moved into SQLite; their pickles are "
                f"kept, but the data can't be loaded until it is written again: {skipped[:20]}",
                stacklevel=2,
            )
        if migrated:
            stdout_feedback_logger.info(
                "Migrated the data of %s %s(s) from intermediate/*.pickle to SQLite; the files "
                "are kept as backups",
                migrated,
                table,
            )


#: Tables converted by migration 6, and whether their rows are exchanges. Fixed here, and
#: not read from the models, as the step must not change when the models do.
_M6_TABLES = (("activitydataset", False), ("exchangedataset", True))
#: Rows read and written at a time
_M6_BATCH_SIZE = 10_000
#: Most rows a dictionary is trained on, and the fewest activity rows worth one: a
#: dictionary takes 110 KB, more than the data of a small project
_M6_SAMPLES = 5_000
_M6_MIN_ACTIVITIES = 500
#: Free disk space needed, as a multiple of the size of the pages which hold pickled rows.
#: The rollback journal holds a copy of every page which is changed, until the commit.
_M6_SPACE_FACTOR = 1.1
#: Exchange keys which are stored in the key columns, and not in ``data``
_M6_EXCHANGE_KEYS = ("input", "output")
#: Values which aren't zstd frames (``28 b5 2f fd``), i.e. pickles
_M6_PICKLED = "substr(\"data\", 1, 4) != X'28B52FFD'"
#: Most different problems listed in the warning, and most rows named for each of them
_M6_MAX_PROBLEMS = 20
_M6_MAX_EXAMPLES = 3
#: Key columns which name a row in the warning
_M6_KEY_COLUMNS = {
    "activitydataset": ("database", "code"),
    "exchangedataset": ("input_database", "input_code", "output_database", "output_code"),
}


def _free_space(path: Path) -> int:
    return shutil.disk_usage(path).free


def _m6_temporal_distribution(td: str):
    """A temporal distribution stored as the JSON string written by ``to_json()`` (bw2data
    4), as the tagged value which ``bw_temporalis`` registers with ``bw2data.codec``: the tag
    is the ``__loader__``, and the value the parsed JSON. ``bw_temporalis`` isn't imported.
    Returns ``None`` for a string which isn't a serialized temporal distribution."""
    if "__loader__" not in td:
        return None
    try:
        payload = json.loads(td)
    except ValueError:
        return None
    loader = payload.get("__loader__") if isinstance(payload, dict) else None
    if not isinstance(loader, str) or not loader.startswith("bw_temporalis."):
        return None
    return codec.UnknownTaggedValue({codec.TAG: loader, "v": payload})


def _m6_encode(blob: bytes, exchange: bool) -> bytes:
    """A pickled ``data`` value as tagged JSON, before compression. Raises for a pickle which
    can't be read, or a value which can't be stored."""
    value = codec.safe_pickle_loads(blob)
    if exchange and isinstance(value, dict):
        value = {k: v for k, v in value.items() if k not in _M6_EXCHANGE_KEYS}
        td = value.get("temporal_distribution")
        if isinstance(td, str):
            tagged = _m6_temporal_distribution(td)
            if tagged is not None:
                value["temporal_distribution"] = tagged
    return codec.dumps(value)


def _m6_store_dictionary(db: Database, dictionary) -> bool:
    """Store ``dictionary`` as the active one. Returns ``False`` if a different dictionary
    with the same id is stored (very unlikely, as zstd derives ids from the content)."""
    stored = db.execute_sql(
        'SELECT "content" FROM main."zstddictionary" WHERE "id" = ?', (dictionary.dict_id,)
    ).fetchone()
    if stored is not None and bytes(stored[0]) != dictionary.dict_content:
        return False
    db.execute_sql('UPDATE main."zstddictionary" SET "active" = 0 WHERE "active" = 1')
    if stored is None:
        db.execute_sql(
            'INSERT INTO main."zstddictionary" ("id", "content", "created", "active") '
            "VALUES (?, ?, ?, 1)",
            (
                dictionary.dict_id,
                dictionary.dict_content,
                datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
            ),
        )
    else:
        db.execute_sql(
            'UPDATE main."zstddictionary" SET "active" = 1 WHERE "id" = ?',
            (dictionary.dict_id,),
        )
    return True


def _m6_dictionary(db: Database, counts: dict[str, int]):
    """The dictionary to compress the converted rows with (``None`` for no dictionary).

    This is the active one, if there is one: it was trained on rows written since, e.g. while
    this step was postponed, and stays the one to write new rows with. Otherwise, with enough
    rows, a new one is trained on a sample of them and made the active one."""
    active = db.execute_sql(
        'SELECT "content" FROM main."zstddictionary" WHERE "active" = 1'
    ).fetchone()
    if active:
        return zstd.ZstdDict(bytes(active[0]))
    if counts["activitydataset"] < _M6_MIN_ACTIVITIES:
        return None
    total = sum(counts.values())
    samples = []
    for table, exchange in _M6_TABLES:
        if not counts[table]:
            continue
        limit = max(1, round(_M6_SAMPLES * counts[table] / total))
        rows = db.execute_sql(
            f'SELECT "data" FROM main."{table}" WHERE {_M6_PICKLED} ORDER BY random() LIMIT ?',
            (limit,),
        ).fetchall()
        samples.extend(_m6_encode(bytes(blob), exchange) for (blob,) in rows)
    try:
        dictionary = zstd.train_dict(samples, ZSTD_DICTIONARY_SIZE) if samples else None
    except (zstd.ZstdError, ValueError):
        return None
    if dictionary is not None and dictionary.dict_id and _m6_store_dictionary(db, dictionary):
        return dictionary
    return None


def _m6_batches(db: Database, table: str, columns: tuple = ()):
    """The pickled rows of ``table`` in batches, as ``(id, data, *columns)``. By id, so that
    each batch starts where the previous one ended, also when the rows before were changed."""
    select = ", ".join(f'"{column}"' for column in ("id", "data", *columns))
    last_id = None
    while True:
        if last_id is None:
            where, params = _M6_PICKLED, (_M6_BATCH_SIZE,)
        else:
            where, params = f'{_M6_PICKLED} AND "id" > ?', (last_id, _M6_BATCH_SIZE)
        rows = db.execute_sql(
            f'SELECT {select} FROM main."{table}" WHERE {where} ORDER BY "id" LIMIT ?', params
        ).fetchall()
        if not rows:
            return
        last_id = rows[-1][0]
        yield rows


def _m6_row_name(exchange: bool, id_: int, keys: tuple) -> str:
    if exchange:
        return f"exchange {id_} from {keys[:2]!r} to {keys[2:]!r}"
    return f"activity {keys!r}"


def _m6_problems(db: Database, counts: dict[str, int]) -> list[str]:
    """What stops the pickled rows from being converted: one line for each different error,
    with the number of rows and the first of them. Nothing is changed. Stops reading at
    ``_M6_MAX_PROBLEMS`` different errors."""
    found: dict[str, list] = {}
    for table, exchange in _M6_TABLES:
        if not counts[table]:
            continue
        for rows in _m6_batches(db, table, _M6_KEY_COLUMNS[table]):
            for id_, blob, *keys in rows:
                try:
                    _m6_encode(bytes(blob), exchange)
                except Exception as err:
                    count_and_examples = found.setdefault(f"{type(err).__name__}: {err}", [0, []])
                    count_and_examples[0] += 1
                    if len(count_and_examples[1]) < _M6_MAX_EXAMPLES:
                        count_and_examples[1].append(_m6_row_name(exchange, id_, tuple(keys)))
            if len(found) >= _M6_MAX_PROBLEMS:
                break
        if len(found) >= _M6_MAX_PROBLEMS:
            break
    return [
        f"{error} ({count} row(s), e.g. {'; '.join(examples)})"
        for error, (count, examples) in found.items()
    ]


def _m6_vacuum(db: Database) -> None:
    """``VACUUM`` after migration 6, which frees the space of the old rows and switches the
    file to the current page size. Skipped if ``config.vacuum`` is ``False``. A failure only
    warns: the data is converted, and the file is compacted at a later ``VACUUM``."""
    if config.vacuum is False:
        stdout_feedback_logger.info("Not compacting %s, as `config.vacuum` is False", db.database)
        return
    if db.in_transaction():
        return
    stdout_feedback_logger.info("Compacting %s", db.database)
    try:
        db.execute_sql("VACUUM")
    except OperationalError as err:
        warnings.warn(
            f"Can't compact the project database {db.database} after converting its data "
            f"({err}). The data is converted, but the file only gets smaller once it is "
            "vacuumed, e.g. after a database is deleted.",
            stacklevel=_user_stacklevel(),
        )


def _compress_lci_data(db: Database, project_dir: Path) -> Optional[AfterCommit]:
    """Convert the pickled ``data`` of activities and exchanges to tagged JSON (see
    ``bw2data.codec``) compressed with zstd, as ``bw2data.sqlite.CompressedJSONField`` stores
    it. Exchanges no longer store ``input`` and ``output``, which are in the key columns, and
    temporal distributions stored as JSON strings become tagged values.

    Pickles are read with ``codec.safe_pickle_loads``. The rows are compressed with the
    active dictionary; if there is none, and at least 500 activities to convert, one is
    trained on a sample of the rows and made the active one. The file is vacuumed after the
    commit.

    The step is postponed if there is not enough free disk space for the rollback journal
    (1.1 times the size of the pages with pickled rows), or if a row can't be converted: a
    pickle with a class which isn't allowed (see ``config.trusted_pickle_classes``), a pickle
    which can't be read, or a value which can't be stored. All rows are checked before the
    first one is changed, so a project with such a row is only read, on every attempt.
    Pickled rows can still be read until then.

    Older versions of bw2data can't read the converted file. No backup is kept: it would be
    as large as the file which this step makes smaller."""
    counts, size = {}, 0
    for table, _ in _M6_TABLES:
        counts[table], pickled = db.execute_sql(
            f'SELECT count(*), coalesce(sum(length("data")), 0) FROM main."{table}" '
            f"WHERE {_M6_PICKLED}"
        ).fetchone()
        size += pickled
    if not any(counts.values()):
        return None

    path = Path(db.database) if db.database not in ("", ":memory:") else None
    if path is not None and path.is_file():
        # Every page with a pickled row is copied to the journal, whole
        page_size = db.execute_sql("PRAGMA main.page_size").fetchone()[0]
        pages = min(path.stat().st_size, size + sum(counts.values()) * page_size)
        needed = int(pages * _M6_SPACE_FACTOR)
        free = _free_space(path.parent)
        if free < needed:
            raise MigrationPostponed(
                f"Converting the activities and exchanges in {path} needs about "
                f"{needed / 1e6:,.0f} MB of free disk space, but only {free / 1e6:,.0f} MB are "
                "free. The project can be used as it is until there is enough space."
            )

    stdout_feedback_logger.info("Checking %s rows", sum(counts.values()))
    problems = _m6_problems(db, counts)
    if problems:
        raise MigrationPostponed(
            "Some activities or exchanges can't be converted from pickles, so none are; they "
            "can be used as they are until then. If you trust where the data came from, allow "
            "the classes named below with `config.trusted_pickle_classes`; otherwise change or "
            "delete these activities or exchanges.\n"
            + "\n".join(f"* {problem}" for problem in problems)
        )

    dictionary = _m6_dictionary(db, counts)
    compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL, zstd_dict=dictionary)
    for table, exchange in _M6_TABLES:
        if not counts[table]:
            continue
        done = 0
        for rows in _m6_batches(db, table):
            updates = [
                (
                    compressor.compress(
                        _m6_encode(bytes(blob), exchange), mode=zstd.ZstdCompressor.FLUSH_FRAME
                    ),
                    id_,
                )
                for id_, blob in rows
            ]
            with closing(db.cursor()) as cursor:
                cursor.executemany(f'UPDATE main."{table}" SET "data" = ? WHERE "id" = ?', updates)
            done += len(rows)
            stdout_feedback_logger.info("Converted %s of %s rows of %s", done, counts[table], table)
    return _m6_vacuum


def _m7_remove_search_files(project_dir: Path) -> AfterCommit:
    def remove(db: Database) -> None:
        directory = project_dir / "search"
        if not directory.is_dir():
            return
        for path in directory.iterdir():
            try:
                if path.is_file():
                    path.unlink()
            except OSError as err:
                warnings.warn(f"Can't delete the old search index {path}: {err}")

    return remove


def _m7_had_index(project_dir: Path, name: str) -> bool:
    """If the old index file of database ``name`` has rows. Opening a database's index in
    earlier versions created an empty file, so the file existing is not enough."""
    import sqlite3

    from bw_processing import safe_filename

    path = project_dir / "search" / safe_filename(name)
    if not path.is_file():
        return False
    try:
        with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
            return conn.execute("SELECT 1 FROM bw2schema LIMIT 1").fetchone() is not None
    except sqlite3.Error:
        return False


def _search_indexes_to_sqlite(db: Database, project_dir: Path) -> Optional[AfterCommit]:
    """Build the search index of every searchable database as a contentless FTS5 table
    ``searchindex_<md5 of the database name>`` in this file, with the node id as ``rowid``
    (see ``bw2data.search.indices``).

    The indexes were one SQLite file per database in the ``search`` directory before. They
    are not read: they have no node ids, and can have rows of nodes which no longer exist.
    The files are deleted after the commit; they can be built again from the nodes.

    A database without a ``searchable`` flag had its nodes indexed when they were saved, but
    not when the database was written. Such a database with nodes is indexed if its old index
    file has rows, and gets the flag which says so; one without nodes keeps no flag, so that
    the nodes saved into it later are indexed.

    A database whose index can't be built is not indexed, with a warning; it is marked as not
    searchable.

    The indexed text is the one bw2data indexes (``format_dataset``), and not a copy, because
    a row can only be removed from a contentless table with the text which was added. A
    change of ``format_dataset`` needs a new step which builds all indexes again."""
    from bw2data.search.indices import FIELDS, create_table_sql, format_dataset, index_table

    project_dir = Path(project_dir)
    store = zstd_dictionaries(db)
    columns = ", ".join(f'"{field}"' for field in FIELDS)
    marks = ", ".join("?" * len(FIELDS))
    records = db.execute_sql(
        'SELECT r."name", r."searchable", EXISTS (SELECT 1 FROM main."activitydataset" a '
        'WHERE a."database" = r."name") FROM main."databaserecord" r ORDER BY r."name"'
    ).fetchall()
    names = []
    for name, searchable, has_nodes in records:
        if searchable is None and has_nodes:
            searchable = _m7_had_index(project_dir, name)
            db.execute_sql(
                'UPDATE main."databaserecord" SET "searchable" = ? WHERE "name" = ?',
                (searchable, name),
            )
        if searchable:
            names.append(name)

    for name in names:
        table = index_table(name)
        try:
            with db.atomic():
                db.execute_sql(f'DROP TABLE IF EXISTS main."{table}"')
                db.execute_sql(create_table_sql(table))
                cursor = db.execute_sql(
                    'SELECT "id", "code", "data" FROM main."activitydataset" '
                    'WHERE "database" = ? ORDER BY "id"',
                    (name,),
                )
                count = 0
                with closing(cursor), closing(db.connection().cursor()) as insert:
                    while rows := cursor.fetchmany(1000):
                        values = []
                        for id_, code, blob in rows:
                            data = {}
                            if blob is not None:
                                data = codec.loads(store.decompress(bytes(blob)))
                            text = format_dataset({**data, "code": code})
                            values.append((id_, *(text[field] for field in FIELDS)))
                        insert.executemany(
                            f'INSERT INTO main."{table}" (rowid, {columns}) VALUES (?, {marks})',
                            values,
                        )
                        count += len(rows)
                # One segment: smaller, and faster to search
                db.execute_sql(f'INSERT INTO main."{table}" ("{table}") VALUES (\'optimize\')')
            stdout_feedback_logger.info("Built the search index of %s (%s nodes)", name, count)
        except Exception as err:
            # Any error: one database which can't be indexed (unreadable node, SQLite without
            # FTS5, ...) must not stop the project from opening
            warnings.warn(
                f"Can't build the search index of database {name!r} "
                f"({type(err).__name__}: {err}); it is no longer searchable. "
                "`Database(name).make_searchable(reset=True)` builds the index.",
                stacklevel=_user_stacklevel(),
            )
            db.execute_sql(
                'UPDATE main."databaserecord" SET "searchable" = 0 WHERE "name" = ?', (name,)
            )
    return _m7_remove_search_files(project_dir)


#: The indexes of the LCI tables, as ``(name, SQL)``. ``activitydataset_key`` is unique: it
#: also keeps two activities from having the same key.
LCI_INDEXES = (
    (
        "activitydataset_key",
        'CREATE UNIQUE INDEX IF NOT EXISTS main."activitydataset_key" '
        'ON "activitydataset" ("database", "code")',
    ),
    (
        "exchangedataset_input",
        'CREATE INDEX IF NOT EXISTS main."exchangedataset_input" '
        'ON "exchangedataset" ("input_database", "input_code")',
    ),
    (
        "exchangedataset_output",
        'CREATE INDEX IF NOT EXISTS main."exchangedataset_output" '
        'ON "exchangedataset" ("output_database", "output_code")',
    ),
)
_M8_MAX_EXAMPLES = 5


def _create_lci_indexes(db: Database, project_dir: Path) -> None:
    """Create the indexes of the activity and exchange tables (``LCI_INDEXES``) where they
    are missing. Before, only a write of at least 100 datasets created them (after dropping
    them for the write), so a file which only had smaller writes had none: every lookup of a
    node by its key scanned the table, and nothing kept keys unique. From now on they always
    exist, and writes keep them.

    Postponed if activities share a key, as the unique index can't be created then; the
    message names them."""
    existing = {
        row[0]
        for row in db.execute_sql(
            "SELECT name FROM main.sqlite_master WHERE type = 'index'"
        ).fetchall()
    }
    if "activitydataset_key" not in existing:
        duplicates = db.execute_sql(
            'SELECT "database", "code", count(*) FROM main."activitydataset" '
            'GROUP BY "database", "code" HAVING count(*) > 1 ORDER BY 1, 2'
        ).fetchall()
        if duplicates:
            examples = ", ".join(
                f"({database!r}, {code!r}) {count} times"
                for database, code, count in duplicates[:_M8_MAX_EXAMPLES]
            )
            raise MigrationPostponed(
                f"{len(duplicates)} activity key(s) are used more than once, e.g. {examples}. "
                "Each activity needs a unique `(database, code)`: delete or change the "
                "duplicates, and the index which keeps keys unique is created the next time "
                "the project is opened. Until then, looking up nodes is slower."
            )
    for name, sql in LCI_INDEXES:
        if name not in existing:
            stdout_feedback_logger.info("Creating index %s", name)
            db.execute_sql(sql)


#: Ordered migration steps. ``MIGRATIONS[i].version`` must be ``i + 1``.
MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "Move preferences.pickle into SQLite", _preferences_to_sqlite),
    Migration(
        2,
        "Use snowflake ids for new geomapping and geocollection ids (remove AUTOINCREMENT)",
        _geomapping_without_autoincrement,
    ),
    Migration(3, "Move databases.json into SQLite", _databases_json_to_sqlite),
    Migration(
        4,
        "Move methods.json, weightings.json and normalizations.json into SQLite",
        _lcia_metadata_to_sqlite,
    ),
    Migration(
        5,
        "Move the data of LCIA methods, weightings and normalizations into SQLite",
        _lcia_factors_to_sqlite,
    ),
    Migration(
        6,
        "Store activity and exchange data as compressed JSON instead of pickles",
        _compress_lci_data,
    ),
    Migration(
        7,
        "Move the search indexes into SQLite",
        _search_indexes_to_sqlite,
    ),
    Migration(8, "Create the indexes of the activity and exchange tables", _create_lci_indexes),
)

#: Schema version of a fully migrated ``lci/databases.db``
SCHEMA_VERSION = len(MIGRATIONS)


def schema_version(db: Database) -> int:
    """Schema version stored in the ``main`` file of ``db``."""
    return db.execute_sql("PRAGMA main.user_version").fetchone()[0]


def _warn_newer(db: Database, current: int) -> None:
    warnings.warn(
        f"The project database {db.database} has schema version {current}, but this "
        f"version of bw2data only knows schema versions up to {SCHEMA_VERSION}. It was "
        "probably written by a newer version of bw2data; some data may be missing or "
        "not be read correctly.",
        stacklevel=3,
    )


def migrate(db: Database, project_dir: Path) -> list[int]:
    """Apply all pending migration steps to ``db``. Returns the versions applied.

    A file with a newer version than this bw2data knows about was written by a newer
    bw2data; it is left alone, with a warning.

    If the file is locked by another process (even for reading) or read-only, nothing is
    applied: the migration is postponed with a warning, and tried again the next time the file
    is opened. A step which raises ``MigrationPostponed`` is postponed the same way, but the
    steps before it are committed.

    Functions returned by the applied steps run after the commit, in order."""
    # Checked before taking the write lock: opening an up to date or newer file must work
    # while another process is writing, and for a read-only file.
    try:
        current = schema_version(db)
    except OperationalError as err:
        # Another process holds an exclusive lock: the file can't even be read
        if not _cannot_write(err):
            raise
        warnings.warn(
            f"Can't read the schema version of the project database {db.database} ({err}). "
            "Pending schema updates, if any, are postponed until the project is next opened; "
            "until then, data which is not yet migrated is not available.",
            stacklevel=_user_stacklevel(),
        )
        return []
    if current == SCHEMA_VERSION:
        return []
    if current > SCHEMA_VERSION:
        _warn_newer(db, current)
        return []

    applied, after_commit, postponed = [], [], None
    try:
        with db.atomic("IMMEDIATE"):
            # Read again under the write lock: another process may have migrated in between
            current = schema_version(db)
            if current > SCHEMA_VERSION:
                _warn_newer(db, current)
                return []
            for step in MIGRATIONS[current:]:
                stdout_feedback_logger.info(
                    "Applying schema migration %s: %s", step.version, step.description
                )
                try:
                    # A savepoint, so that a postponed step changes nothing
                    with db.atomic():
                        action = step.apply(db, Path(project_dir))
                except MigrationPostponed as err:
                    postponed = (step, err)
                    break
                applied.append(step.version)
                if action is not None:
                    after_commit.append(action)
            if applied:
                # ``user_version`` is part of the transaction, and can't take a bound parameter
                db.execute_sql(f"PRAGMA main.user_version = {int(applied[-1])}")
    except OperationalError as err:
        if not _cannot_write(err):
            raise
        warnings.warn(
            f"Can't update the project database {db.database} from schema version {current} "
            f"to {SCHEMA_VERSION} ({err}). The update is postponed until the project is next "
            "opened; until then, data which is not yet migrated (e.g. preferences from "
            "preferences.pickle) is not available.",
            stacklevel=_user_stacklevel(),
        )
        return []
    if postponed is not None:
        step, err = postponed
        warnings.warn(
            f"Schema migration {step.version} of the project database {db.database} "
            f"({step.description}) is postponed until the project is next opened: {err}",
            stacklevel=_user_stacklevel(),
        )
    for action in after_commit:
        action(db)
    return applied
