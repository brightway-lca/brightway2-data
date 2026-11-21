"""Versioned schema migrations for the per-project ``lci/databases.db``.

The schema version is stored in the file itself, in ``PRAGMA user_version``. Version ``0``
means the file has never been migrated (every file written before schema versioning).
Version ``N`` means that migration steps ``1`` to ``N`` have been applied.

Peewee's ``create_tables`` creates tables which don't exist yet, so new tables need no
migration step. Migration steps are for everything ``create_tables`` can't do: moving data
in from files in the project directory, or changing tables which already exist.

``migrate`` runs every time the file is opened, i.e. on import and on every project switch.
When the file is up to date this costs one ``PRAGMA`` read. Otherwise all pending steps run
in one ``IMMEDIATE`` transaction, together with the version update: a step which fails
leaves the file unchanged, and two processes opening the same project don't both migrate.

Rules for migration steps:

* Steps are only ever appended. Never change or remove a released step: its number is
  stored in users' files.
* A step gets the peewee database and the project directory. It must use the ``main``
  schema explicitly in raw SQL, as other files can be attached to the connection.
* A step must not open or close transactions; it already runs inside one.
* A step which moves data in from a legacy file keeps that file, so older versions of
  bw2data can still open the project.
"""

import pickle
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from peewee import Database

from bw2data.logs import stdout_feedback_logger


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    apply: Callable[[Database, Path], None]


def _preferences_to_sqlite(db: Database, project_dir: Path) -> None:
    """Move ``preferences.pickle`` into the ``preference`` table.

    Entries with keys which are not strings can't be stored in the table; they are skipped
    with a warning, and remain in ``preferences.pickle``. An unreadable file is skipped with a
    warning, and the project starts with default preferences."""
    from bw2data.backends.schema import Preference

    path = project_dir / "preferences.pickle"
    if not path.is_file() or Preference.select().exists():
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

    rows = [{"key": k, "value": v} for k, v in legacy.items() if isinstance(k, str)]
    skipped = [k for k in legacy if not isinstance(k, str)]
    if rows:
        Preference.insert_many(rows).execute()
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


#: Ordered migration steps. ``MIGRATIONS[i].version`` must be ``i + 1``.
MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "Move preferences.pickle into SQLite", _preferences_to_sqlite),
)

#: Schema version of a fully migrated ``lci/databases.db``
SCHEMA_VERSION = len(MIGRATIONS)


def schema_version(db: Database) -> int:
    """Schema version stored in the ``main`` file of ``db``."""
    return db.execute_sql("PRAGMA main.user_version").fetchone()[0]


def migrate(db: Database, project_dir: Path) -> list[int]:
    """Apply all pending migration steps to ``db``. Returns the versions applied.

    A file with a newer version than this bw2data knows about was written by a newer
    bw2data; it is left alone, with a warning."""
    if schema_version(db) == SCHEMA_VERSION:
        return []

    with db.atomic("IMMEDIATE"):
        # Read again under the write lock: another process may have migrated in between
        current = schema_version(db)
        if current > SCHEMA_VERSION:
            warnings.warn(
                f"The project database {db.database} has schema version {current}, but this "
                f"version of bw2data only knows schema versions up to {SCHEMA_VERSION}. It was "
                "probably written by a newer version of bw2data; some data may be missing or "
                "not be read correctly.",
                stacklevel=2,
            )
            return []
        applied = []
        for step in MIGRATIONS[current:]:
            stdout_feedback_logger.info(
                "Applying schema migration %s: %s", step.version, step.description
            )
            step.apply(db, Path(project_dir))
            applied.append(step.version)
        # ``user_version`` is part of the transaction, and can't take a bound parameter
        db.execute_sql(f"PRAGMA main.user_version = {int(SCHEMA_VERSION)}")
    return applied
