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
* A step must not open or close transactions; it already runs inside one.
* A step which moves data in from a legacy file keeps that file, so older versions of
  bw2data can still open the project.
"""

import pickle
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from peewee import Database, OperationalError

from bw2data.logs import stdout_feedback_logger


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    apply: Callable[[Database, Path], None]


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


#: Ordered migration steps. ``MIGRATIONS[i].version`` must be ``i + 1``.
MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "Move preferences.pickle into SQLite", _preferences_to_sqlite),
    Migration(
        2,
        "Use snowflake ids for new geomapping and geocollection ids (remove AUTOINCREMENT)",
        _geomapping_without_autoincrement,
    ),
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


def _cannot_write(err: OperationalError) -> bool:
    """Whether ``err`` means that the file can't be written right now (locked or read-only)."""
    message = str(err).lower()
    return "locked" in message or "readonly" in message or "read-only" in message


def migrate(db: Database, project_dir: Path) -> list[int]:
    """Apply all pending migration steps to ``db``. Returns the versions applied.

    A file with a newer version than this bw2data knows about was written by a newer
    bw2data; it is left alone, with a warning.

    If the file is locked by another process or read-only, nothing is applied: the migration
    is postponed with a warning, and tried again the next time the file is opened."""
    # Checked before taking the write lock: opening an up to date or newer file must work
    # while another process is writing, and for a read-only file.
    current = schema_version(db)
    if current == SCHEMA_VERSION:
        return []
    if current > SCHEMA_VERSION:
        _warn_newer(db, current)
        return []

    applied = []
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
                step.apply(db, Path(project_dir))
                applied.append(step.version)
            if applied:
                # ``user_version`` is part of the transaction, and can't take a bound parameter
                db.execute_sql(f"PRAGMA main.user_version = {int(SCHEMA_VERSION)}")
    except OperationalError as err:
        if not _cannot_write(err):
            raise
        warnings.warn(
            f"Can't update the project database {db.database} from schema version {current} "
            f"to {SCHEMA_VERSION} ({err}). The update is postponed until the project is next "
            "opened; until then, data which is not yet migrated (e.g. preferences from "
            "preferences.pickle) is not available.",
            stacklevel=2,
        )
        return []
    return applied
