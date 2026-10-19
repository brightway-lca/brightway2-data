import json
import pickle

from peewee import BlobField, SqliteDatabase, TextField

from bw2data.configuration import config
from bw2data.logs import stdout_feedback_logger


class PickleField(BlobField):
    def db_value(self, value):
        return super(PickleField, self).db_value(pickle.dumps(value, protocol=4))

    def python_value(self, value):
        return pickle.loads(bytes(value))


class SubstitutableDatabase:
    def __init__(self, filepath, tables):
        self._filepath = filepath
        self._tables = tables
        self._database = self._create_database()

    def _create_database(self):
        db = SqliteDatabase(self._filepath)
        for model in self._tables:
            model.bind(db, bind_refs=False, bind_backrefs=False)
        db.connect()
        db.create_tables(self._tables)
        return db

    @property
    def db(self):
        return self._database

    def change_path(self, filepath):
        import gc
        old_db = self._database
        old_db.close()
        del old_db
        gc.collect()
        self._filepath = filepath
        self._database = self._create_database()

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
