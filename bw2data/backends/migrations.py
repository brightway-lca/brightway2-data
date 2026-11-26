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
import hashlib
import json
import numbers
import pickle
import shutil
import warnings
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from peewee import (
    Database,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

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


#: Legacy calculation setup files and the ``kind`` of their setups
_SETUP_FILES = (("setups.pickle", "static"), ("dynamicsetups.pickle", "dynamic"))


def _m8_demand_rows(db: Database, inv) -> tuple[list[tuple], list]:
    """``(unit, node id, amount, key database, key code)`` rows for the functional units
    ``inv``, and the keys of nodes which don't exist (left out). Raises ``ValueError`` for
    ``inv`` which isn't a list of dicts of keys or ids."""
    if not isinstance(inv, (list, tuple)):
        raise ValueError("`inv` is not a list")
    rows, missing = [], []
    for position, unit in enumerate(inv):
        if not isinstance(unit, dict):
            raise ValueError("a functional unit is not a dict")
        seen = set()
        for node, amount in unit.items():
            if isinstance(node, (tuple, list)) and len(node) == 2:
                found = db.execute_sql(
                    'SELECT "id" FROM main."activitydataset" WHERE "database" = ? AND "code" = ?',
                    (node[0], node[1]),
                ).fetchone()
                if found is None:
                    missing.append(tuple(node))
                    continue
                node_id, key = found[0], (node[0], node[1])
            elif isinstance(node, numbers.Integral) and not isinstance(node, bool):
                # Also numpy integers
                node_id, key = int(node), (None, None)
            else:
                raise ValueError(f"{node!r} is not a node key or id")
            if node_id in seen:
                continue
            seen.add(node_id)
            if isinstance(amount, numbers.Number) and hasattr(amount, "item"):
                # E.g. a numpy scalar, which SQLite can't store
                amount = amount.item()
            rows.append((position, node_id, amount, *key))
    return rows, missing


def _calculation_setups_to_sqlite(db: Database, project_dir: Path) -> None:
    """Move ``setups.pickle`` and ``dynamicsetups.pickle`` into the ``calculationsetup`` and
    ``calculationsetupdemand`` tables (see ``bw2data.backends.schema.CalculationSetupRecord``).

    The pickles are read with ``codec.safe_pickle_loads``. Nodes given by key are stored by
    id, with the key; keys of nodes which don't exist are left out, with a warning. Only the
    project's own file is searched (extensions are attached after the migrations), so this
    includes the nodes of read-only extensions. Setups
    which are already in the table are kept as they are. An unreadable file, and setups which
    can't be stored, are skipped with a warning; the files are kept as backups either way."""
    from bw2data.snowflake_ids import new_snowflake_id

    for filename, kind in _SETUP_FILES:
        path = project_dir / filename
        if not path.is_file():
            continue
        try:
            legacy = codec.safe_pickle_loads(path.read_bytes())
            if not isinstance(legacy, dict):
                raise ValueError("not a dictionary")
        except Exception as err:
            warnings.warn(
                f"Can't read {path} ({err}); its calculation setups are only kept in the file.",
                stacklevel=2,
            )
            continue
        migrated, skipped, missing = 0, [], {}
        for name, setup in legacy.items():
            # A name which is not a string can't be bound; it is skipped below
            exists = (
                isinstance(name, str)
                and db.execute_sql(
                    'SELECT 1 FROM main."calculationsetup" WHERE "kind" = ? AND "name" = ?',
                    (kind, name),
                ).fetchone()
            )
            if exists:
                continue
            try:
                if not isinstance(name, str) or not isinstance(setup, dict):
                    raise ValueError("not a named dictionary")
                setup = dict(setup)
                inv, ia = setup.pop("inv", None), setup.pop("ia", None)
                rows, gone = ([], []) if inv is None else _m8_demand_rows(db, inv)
                values = (
                    new_snowflake_id(),
                    kind,
                    name,
                    None if inv is None else len(inv),
                    None if ia is None else codec.dumps(ia).decode("utf-8"),
                    codec.dumps(setup).decode("utf-8") if setup else None,
                )
                with db.atomic():
                    db.execute_sql(
                        'INSERT INTO main."calculationsetup" '
                        '("id", "kind", "name", "units", "ia", "extra") VALUES (?, ?, ?, ?, ?, ?)',
                        values,
                    )
                    for row in rows:
                        db.execute_sql(
                            'INSERT INTO main."calculationsetupdemand" ("setup_id", "unit", '
                            '"node", "amount", "key_database", "key_code") '
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            (values[0], *row),
                        )
            except (
                TypeError,
                ValueError,
                OverflowError,
                # E.g. an amount which is ``None`` or NaN (``NULL`` in SQLite)
                IntegrityError,
                InterfaceError,
                ProgrammingError,
            ):
                skipped.append(name)
                continue
            if gone:
                missing[name] = gone
            migrated += 1
        if skipped:
            warnings.warn(
                f"Calculation setups in {path} which can't be stored are only kept in the "
                f"file: {skipped[:20]!r}",
                stacklevel=2,
            )
        if missing:
            warnings.warn(
                f"Calculation setups in {path} refer to nodes which don't exist; these are left "
                "out: "
                + "; ".join(f"{name!r}: {keys[:5]!r}" for name, keys in list(missing.items())[:5]),
                stacklevel=2,
            )
        stdout_feedback_logger.info(
            "Migrated %s calculation setup(s) from %s to SQLite; the file is kept as a backup",
            migrated,
            path,
        )


#: The parameter tables at schema version 9, as ``parameters.db`` had them, in the order their
#: rows are copied: parameters before the groups, whose ``updated`` the triggers would change
_M9_TABLES = (
    (
        "projectparameter",
        'CREATE TABLE IF NOT EXISTS main."projectparameter" ("id" INTEGER NOT NULL PRIMARY KEY, '
        '"name" TEXT NOT NULL, "formula" TEXT, "amount" REAL, "data" BLOB NOT NULL)',
    ),
    (
        "databaseparameter",
        'CREATE TABLE IF NOT EXISTS main."databaseparameter" ("id" INTEGER NOT NULL PRIMARY KEY, '
        '"database" TEXT NOT NULL, "name" TEXT NOT NULL, "formula" TEXT, "amount" REAL, '
        "\"data\" BLOB NOT NULL, CHECK (database != 'project'))",
    ),
    (
        "activityparameter",
        'CREATE TABLE IF NOT EXISTS main."activityparameter" ("id" INTEGER NOT NULL PRIMARY KEY, '
        '"group" TEXT NOT NULL, "database" TEXT NOT NULL, "code" TEXT NOT NULL, '
        '"name" TEXT NOT NULL, "formula" TEXT, "amount" REAL, "data" BLOB NOT NULL, '
        'CHECK (("group" != \'project\') AND ("group" != database)))',
    ),
    (
        "parameterizedexchange",
        'CREATE TABLE IF NOT EXISTS main."parameterizedexchange" ("id" INTEGER NOT NULL '
        'PRIMARY KEY, "group" TEXT NOT NULL, "exchange" INTEGER NOT NULL, "formula" TEXT NOT NULL)',
    ),
    (
        "groupdependency",
        'CREATE TABLE IF NOT EXISTS main."groupdependency" ("id" INTEGER NOT NULL PRIMARY KEY, '
        '"group" TEXT NOT NULL, "depends" TEXT NOT NULL, CHECK ("group" != depends))',
    ),
    (
        "group_table",
        'CREATE TABLE IF NOT EXISTS main."group_table" ("id" INTEGER NOT NULL PRIMARY KEY, '
        '"name" TEXT NOT NULL, "fresh" INTEGER NOT NULL, "updated" DATETIME NOT NULL, '
        '"order" BLOB NOT NULL)',
    ),
)
_M9_INDEXES = (
    'CREATE UNIQUE INDEX IF NOT EXISTS main."projectparameter_name" ON "projectparameter" ("name")',
    'CREATE INDEX IF NOT EXISTS main."databaseparameter_database" '
    'ON "databaseparameter" ("database")',
    'CREATE INDEX IF NOT EXISTS main."databaseparameter_name" ON "databaseparameter" ("name")',
    'CREATE UNIQUE INDEX IF NOT EXISTS main."databaseparameter_database_name" '
    'ON "databaseparameter" ("database", "name")',
    'CREATE UNIQUE INDEX IF NOT EXISTS main."activityparameter_group_name" '
    'ON "activityparameter" ("group", "name")',
    'CREATE UNIQUE INDEX IF NOT EXISTS main."parameterizedexchange_exchange" '
    'ON "parameterizedexchange" ("exchange")',
    'CREATE UNIQUE INDEX IF NOT EXISTS main."groupdependency_group_depends" '
    'ON "groupdependency" ("group", "depends")',
    'CREATE UNIQUE INDEX IF NOT EXISTS main."group_name" ON "group_table" ("name")',
)


def _m9_triggers() -> list[str]:
    """The triggers of the parameter tables at schema version 9, as ``parameters.db`` had
    them: ``group_table.updated`` follows changes to parameters, and some combinations are
    refused."""
    statements = []
    for table, old, new in (
        ("projectparameter", "'project'", "'project'"),
        ("databaseparameter", "OLD.database", "NEW.database"),
        ("activityparameter", 'OLD."group"', 'NEW."group"'),
    ):
        for action in ("INSERT", "UPDATE", "DELETE"):
            name = new if action == "INSERT" else old
            statements.append(
                f"CREATE TRIGGER IF NOT EXISTS main.{table}_{action}_trigger AFTER {action} "
                f"ON {table} BEGIN\n"
                f"    UPDATE group_table SET updated = datetime('now') WHERE name = {name};\nEND;"
            )
    for action in ("UPDATE", "INSERT"):
        statements += [
            f"CREATE TRIGGER IF NOT EXISTS main.ap_crossdatabase_{action} BEFORE {action} ON "
            "activityparameter BEGIN\n    SELECT CASE WHEN\n"
            '        ((SELECT COUNT(*) FROM activityparameter WHERE "group" = NEW."group") > 0)\n'
            '    AND (NEW.database NOT IN (SELECT DISTINCT "database" FROM activityparameter '
            'where "group" = NEW."group"))\n'
            "    THEN RAISE(ABORT,'Cross database group')\n    END;\nEND;",
            f"CREATE TRIGGER IF NOT EXISTS main.ap_crossgroup_{action} BEFORE {action} ON "
            "activityparameter BEGIN\n"
            "    SELECT CASE WHEN EXISTS (SELECT * FROM activityparameter AS a WHERE\n"
            "            a.database = NEW.database AND\n"
            "            a.code = NEW.code AND\n"
            '            a."group" != NEW."group")\n'
            "    THEN RAISE(ABORT,'Cross group activity')\n    END;\nEND;",
            f"CREATE TRIGGER IF NOT EXISTS main.gd_circular_{action} BEFORE {action} ON "
            "groupdependency BEGIN\n"
            '    SELECT CASE WHEN EXISTS (SELECT * FROM groupdependency AS g WHERE g."group" = '
            'NEW.depends AND g.depends = NEW."group")\n'
            "    THEN RAISE(ABORT,'Circular dependency')\n    END;\nEND;",
            f"CREATE TRIGGER IF NOT EXISTS main.pe_group_{action} BEFORE {action} ON "
            "parameterizedexchange BEGIN\n    SELECT CASE WHEN\n"
            '        ((SELECT COUNT(*) FROM activityparameter WHERE "group" = NEW."group") < 1)\n'
            "    THEN RAISE(ABORT,'Missing activity parameter group')\n    END;\nEND;",
        ]
    return statements


def _parameters_db_to_sqlite(db: Database, project_dir: Path) -> None:
    """Move the parameters from the project's ``parameters.db`` into ``lci/databases.db``.

    Creates the parameter tables, their indexes and their triggers (as ``parameters.db`` had
    them; ``bw2data.parameters`` uses them from now on), and copies the rows of each table,
    keeping their ids. The rows are copied without the triggers, so the groups keep their
    ``updated`` times, and rows which the triggers would refuse now (e.g. a parameterized
    exchange whose group has no activity parameter left) are copied as they are. Triggers
    which exist already (the tables are created with them when a project is opened after
    ``bw2data.parameters`` is imported) are dropped first, and created again afterwards.
    Rows which are already in the tables are kept as they are. The file is kept as a backup.

    Postponed if ``parameters.db`` is locked by another process (e.g. an older bw2data
    which has the project open). If the rows can't be copied otherwise (e.g. the file can't
    be read), none of them are, with a warning: the parameters are only kept in the file."""
    import sqlite3

    from bw2data.sqlite import _sqlite_uri

    for _, sql in _M9_TABLES:
        db.execute_sql(sql)
    for sql in _M9_INDEXES:
        db.execute_sql(sql)
    tables = [table for table, _ in _M9_TABLES]
    triggers = db.execute_sql(
        "SELECT name FROM main.sqlite_master WHERE type = 'trigger' AND tbl_name IN "
        f"({', '.join('?' * len(tables))})",
        tables,
    ).fetchall()
    for (trigger,) in triggers:
        db.execute_sql(f'DROP TRIGGER main."{trigger}"')

    path = project_dir / "parameters.db"
    copied = {}
    if path.is_file():
        try:
            source = sqlite3.connect(_sqlite_uri(path, mode="ro"), uri=True, timeout=1)
            # A savepoint: all rows are copied, or none
            with closing(source), db.atomic():
                existing = {
                    row[0]
                    for row in source.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }
                for table in tables:
                    if table not in existing:
                        continue
                    theirs = [row[1] for row in source.execute(f'PRAGMA table_info("{table}")')]
                    ours = {
                        row[1]
                        for row in db.execute_sql(f'PRAGMA main.table_info("{table}")').fetchall()
                    }
                    columns = [column for column in theirs if column in ours]
                    names = ", ".join(f'"{column}"' for column in columns)
                    rows = source.execute(f'SELECT {names} FROM "{table}"').fetchall()
                    if rows:
                        insert = (
                            f'INSERT OR IGNORE INTO main."{table}" ({names}) '
                            f"VALUES ({', '.join('?' * len(columns))})"
                        )
                        with closing(db.cursor()) as cursor:
                            cursor.executemany(insert, rows)
                    copied[table] = len(rows)
        except sqlite3.DatabaseError as err:
            if isinstance(err, sqlite3.OperationalError) and "locked" in str(err):
                raise MigrationPostponed(
                    f"{path} is locked by another process ({err}); its parameters are moved "
                    "into the project's LCI database the next time the project is opened."
                ) from err
            # The savepoint is rolled back: no rows were copied
            copied = {}
            warnings.warn(
                f"Can't copy the parameters of {path} ({err}); they are only kept in the file.",
                stacklevel=2,
            )
    for sql in _m9_triggers():
        db.execute_sql(sql)
    if copied:
        stdout_feedback_logger.info(
            "Moved the parameters from %s into the LCI database (%s); the file is kept as a "
            "backup",
            path,
            ", ".join(f"{count} {table}" for table, count in copied.items()),
        )


#: As ``bw2data.meta`` at schema version 10, kept here so that the step doesn't change with it
_M10_DEFAULT_GEOCOLLECTION = "world"
_M10_GLOBAL_LOCATIONS = frozenset({"", "GLO", "Global"})


def _m10_key(collection, location):
    """``(geocollection or None, location)`` as stored from schema version 10 on: locations in
    the default geocollection are stored without one, and the global location as ``"GLO"``."""
    if collection == _M10_DEFAULT_GEOCOLLECTION:
        collection = None
    if collection is None and (location is None or location in _M10_GLOBAL_LOCATIONS):
        location = "GLO"
    return collection, location


def _m10_id(collection, location) -> int:
    """``bw2data.meta.location_id`` at schema version 10."""
    text = f"{collection or _M10_DEFAULT_GEOCOLLECTION}\0{location}"
    value = int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big") >> 1
    return value or 1


def _m10_remove_processed(project_dir: Path, keep: set[str]) -> AfterCommit:
    """After the commit: forget the location ids which ``geomapping`` has cached, and remove
    the processed datapackages, except the files named in ``keep``."""

    def remove(db: Database) -> None:
        from bw2data.meta import geomapping

        # ``geomapping`` may have been read before this step (a deferred migration): its
        # cache would still give the old ids
        geomapping.__init__()
        directory = project_dir / "processed"
        if not directory.is_dir():
            return
        failed = []
        for path in directory.glob("*.zip"):
            if path.name in keep:
                continue
            try:
                path.unlink()
            except OSError:
                failed.append(path.name)
        if failed:
            warnings.warn(
                f"Can't remove the processed datapackages {failed[:10]!r} in {directory}; "
                "they still use the location ids from before schema migration 10. Delete them, "
                "and they are processed again when they are needed.",
                stacklevel=_user_stacklevel(),
            )

    return remove


def _reproducible_location_ids(db: Database, project_dir: Path) -> Optional[AfterCommit]:
    """Give every location in ``geomappingrecord`` the id derived from its name (see
    ``bw2data.meta.location_id``), the same in every project, so that processed datapackages
    from other projects and extension files can be used without translating location ids.

    Locations in the ``"world"`` geocollection are stored without a geocollection, and the
    names of the global location (``""``, ``"Global"``) become ``"GLO"``; rows which become
    the same location are merged. The ``"world"`` geocollection itself is removed, as in a
    new project, unless it has metadata (``extra``). Postponed if two different locations would get the same
    id (practically impossible).

    The processed datapackages which use the old ids are removed after the commit (all
    ``*.zip`` files in ``processed``); databases, methods, weightings and normalizations are
    processed again when their datapackage is needed. Not those of IO table databases, whose exchanges are only
    stored there; their inventory geomapping keeps the old ids, which only regionalized
    calculations read. Other packages (e.g. ``bw2regional``) must process their data again."""
    from bw_processing import clean_datapackage_name, safe_filename

    rows = db.execute_sql(
        'SELECT r."id", c."name", r."location" FROM main."geomappingrecord" AS r '
        'LEFT JOIN main."geocollection" AS c ON r."geocollection_id" = c."id"'
    ).fetchall()
    collection_ids = dict(
        db.execute_sql('SELECT "name", "id" FROM main."geocollection"').fetchall()
    )
    locations = {}
    for _, collection, location in rows:
        key = _m10_key(collection, location)
        locations.setdefault(_m10_id(*key), set()).add(key)
    clashes = [keys for keys in locations.values() if len(keys) > 1]
    if clashes:
        raise MigrationPostponed(
            f"Some locations would get the same id: {clashes[:5]!r}. Rename one of each, and the "
            "location ids are changed the next time the project is opened."
        )
    db.execute_sql('DELETE FROM main."geomappingrecord"')
    for id_, keys in locations.items():
        ((collection, location),) = keys
        db.execute_sql(
            'INSERT INTO main."geomappingrecord" ("id", "geocollection_id", "location") '
            "VALUES (?, ?, ?)",
            (id_, collection_ids.get(collection) if collection else None, location),
        )
    db.execute_sql(
        'DELETE FROM main."geocollection" WHERE "name" = ? AND "extra" IS NULL', ("world",)
    )
    io_tables = [
        name
        for (name,) in db.execute_sql(
            'SELECT "name" FROM main."databaserecord" WHERE "backend" = \'iotable\''
        ).fetchall()
    ]
    if io_tables:
        warnings.warn(
            "The processed data of IO table databases keeps the location ids from before "
            "schema migration 10, as their exchanges are only stored there; regionalized "
            f"calculations with them need the IO table to be written again: {io_tables[:10]!r}",
            stacklevel=_user_stacklevel(),
        )
    return _m10_remove_processed(
        project_dir,
        {clean_datapackage_name(safe_filename(name) + ".zip") for name in io_tables},
    )


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
    Migration(
        8,
        "Move setups.pickle and dynamicsetups.pickle into SQLite",
        _calculation_setups_to_sqlite,
    ),
    Migration(9, "Move parameters.db into the LCI database", _parameters_db_to_sqlite),
    Migration(10, "Derive location ids from location names", _reproducible_location_ids),
)

#: Schema version of a fully migrated ``lci/databases.db``
SCHEMA_VERSION = len(MIGRATIONS)

#: Oldest schema version of a read-only extension file which can be attached. An extension
#: can never be migrated, so it is read as it is: its overlaid tables (activities, exchanges,
#: database records, zstd dictionaries) must be stored as this version of bw2data stores them.
#: Raise this to the new version whenever a step changes how one of these tables is stored.
#: 6: activity and exchange data as compressed tagged JSON, without pickles.
#: 10: location ids derived from location names, as in the stored processed datapackages.
MIN_EXTENSION_SCHEMA_VERSION = 10


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
