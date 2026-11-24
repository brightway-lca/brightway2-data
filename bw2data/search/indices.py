"""Full-text search index of the nodes of a database.

The index of a database is an FTS5 table in the same SQLite file as its nodes
(``lci/databases.db``), so that the index and the nodes change in one transaction. The table
is *contentless*: it stores the index but not the text, which is already in the nodes. Its
``rowid`` is the id of the node.

A row can only be removed from a contentless table by giving the text which was indexed.
This is read from the stored node, so a node must be removed from the index *before* its row
is changed or deleted, and added *after* its row is written; ``IndexManager.remove`` and
``IndexManager.add`` are used in this order by ``Activity.save`` and the other methods which
change nodes. If nodes were changed in another way (e.g. with ``ActivityDataset`` directly),
``Database.make_searchable(reset=True)`` builds the index again.

Contentless tables without the ``contentless_delete`` option are used on purpose: they are
read by every SQLite version with FTS5, also when the file was written with a newer one.
"""

import hashlib
from collections.abc import Iterable
from contextlib import closing

#: Indexed columns, in the order of the FTS5 table
FIELDS = ("name", "comment", "product", "categories", "synonyms", "location", "code")

TABLE_PREFIX = "searchindex_"

#: Number of ids in one ``IN (...)`` clause
_CHUNK = 500


def index_table(database: str) -> str:
    """Name of the FTS5 table with the search index of ``database``.

    From a hash of the database name, so that every name gives a valid table name which can't
    clash with the shadow tables (``<table>_data`` etc.) of another index."""
    return TABLE_PREFIX + hashlib.md5(database.encode("utf-8")).hexdigest()


def _fix_location(value) -> str:
    if isinstance(value, tuple):
        value = value[1]
    if isinstance(value, str):
        if value.lower() == "none":
            return ""
        return value.lower().strip()
    return ""


def format_dataset(ds) -> dict:
    """The indexed text of the node ``ds``, by column of ``FIELDS``."""
    return dict(
        name=(ds.get("name") or "").lower(),
        comment=(ds.get("comment") or "").lower(),
        product=(ds.get("reference product") or "").lower(),
        categories=", ".join(ds.get("categories") or []).lower(),
        synonyms=", ".join(ds.get("synonyms") or []).lower(),
        location=_fix_location(ds.get("location") or ""),
        code=str(ds.get("code") or ""),
    )


def create_table_sql(table: str, schema: str = "main") -> str:
    columns = ", ".join(f'"{field}"' for field in FIELDS)
    return (
        f'CREATE VIRTUAL TABLE {schema}."{table}" USING fts5('
        f"{columns}, content='', tokenize='unicode61')"
    )


def _chunks(values: list, size: int = _CHUNK):
    for start in range(0, len(values), size):
        yield values[start : start + size]


class IndexManager:
    """The search index of the database ``database``, in the project's ``lci/databases.db``.

    ``database`` is the name of a database; for compatibility with earlier versions, which
    kept one index file per database, the ``filename`` of a registered database is accepted
    as well.

    The index refers to nodes by id, so only nodes which are stored can be indexed."""

    schema = "main"

    def __init__(self, database: str):
        self.database = self._database_name(database)
        self.table = index_table(self.database)

    @staticmethod
    def _database_name(value: str) -> str:
        from bw2data import databases
        from bw_processing import safe_filename

        if value in databases:
            return value
        for name in databases:
            if safe_filename(name) == value:
                return name
        return value

    @property
    def db(self):
        from bw2data.backends import sqlite3_lci_db

        return sqlite3_lci_db

    def get(self):
        return self

    def close(self):
        """Does nothing; the index has no connection of its own."""

    # The table

    def exists(self) -> bool:
        sql = f"SELECT 1 FROM {self.schema}.sqlite_master WHERE type = 'table' AND name = ?"
        with closing(self.db.execute_sql(sql, (self.table,))) as cursor:
            return cursor.fetchone() is not None

    def create(self):
        """Create an empty index, in place of the existing one if there is one."""
        with self.db.atomic():
            self.delete_database()
            self.db.execute_sql(create_table_sql(self.table, self.schema))

    def delete_database(self):
        """Delete the index, if there is one."""
        self.db.execute_sql(f'DROP TABLE IF EXISTS {self.schema}."{self.table}"')

    # Rows

    def _stored(self, ids: Iterable[int]) -> list[tuple[int, str, str, dict]]:
        """``(id, database, code, indexed text)`` of the stored nodes ``ids``, in this order.
        Ids without a node are left out."""
        from bw2data.backends.schema import ActivityDataset as AD

        ids = list(ids)
        found = {}
        for chunk in _chunks(ids):
            query = AD.select(AD.id, AD.database, AD.code, AD.data).where(AD.id.in_(chunk))
            for id_, database, code, data in query.tuples():
                found[id_] = (id_, database, code, format_dataset({**(data or {}), "code": code}))
        return [found[id_] for id_ in ids if id_ in found]

    def _node_ids(self, datasets) -> list[int]:
        from bw2data.backends.schema import ActivityDataset as AD
        from bw2data.errors import UnknownObject

        ids = []
        for ds in datasets:
            id_ = getattr(ds, "id", None) or ds.get("id")
            if id_ is None:
                id_ = (
                    AD.select(AD.id)
                    .where(AD.database == ds["database"], AD.code == ds["code"])
                    .scalar()
                )
            if id_ is None:
                raise UnknownObject(
                    f"Node {(ds['database'], ds['code'])} is not stored, and can't be indexed"
                )
            ids.append(id_)
        return ids

    def _write(self, rows: list[tuple[int, dict]], delete: bool = False) -> None:
        if not rows:
            return
        columns = ", ".join(f'"{field}"' for field in FIELDS)
        marks = ", ".join("?" * len(FIELDS))
        table = f'{self.schema}."{self.table}"'
        if delete:
            sql = (
                f'INSERT INTO {table} ("{self.table}", rowid, {columns}) '
                f"VALUES ('delete', ?, {marks})"
            )
        else:
            sql = f"INSERT INTO {table} (rowid, {columns}) VALUES (?, {marks})"
        connection = self.db.db.connection()
        with closing(connection.cursor()) as cursor:
            cursor.executemany(
                sql, ((id_, *(values[field] for field in FIELDS)) for id_, values in rows)
            )

    def add(self, ids: Iterable[int]) -> None:
        """Index the stored nodes ``ids``, which must not be in the index. Call this after
        their rows are written. Creates the index if needed."""
        rows = [(id_, values) for id_, _, _, values in self._stored(ids)]
        with self.db.atomic():
            if not self.exists():
                self.db.execute_sql(create_table_sql(self.table, self.schema))
            self._write(rows)

    def remove(self, ids: Iterable[int]) -> None:
        """Remove the stored nodes ``ids`` from the index. Call this before their rows are
        changed or deleted: the indexed text is taken from the stored nodes."""
        if not self.exists():
            return
        rows = [(id_, values) for id_, _, _, values in self._stored(ids)]
        with self.db.atomic():
            self._write(rows, delete=True)

    def add_all(self) -> None:
        """Index all stored nodes of the database, in a new index."""
        from bw2data.backends.schema import ActivityDataset as AD

        with self.db.atomic():
            self.create()
            query = AD.select(AD.id, AD.code, AD.data).where(AD.database == self.database)
            self._write(
                [
                    (id_, format_dataset({**(data or {}), "code": code}))
                    for id_, code, data in query.tuples().iterator()
                ]
            )
            # One segment: smaller, and faster to search
            self.db.execute_sql(
                f'INSERT INTO {self.schema}."{self.table}" ("{self.table}") VALUES (\'optimize\')'
            )

    # Datasets; kept from the index which had one file per database

    def add_dataset(self, ds):
        self.add_datasets([ds])

    def add_datasets(self, datasets):
        """Index ``datasets``: nodes, or dictionaries with ``id`` or ``database`` and
        ``code``. They must be stored; their stored values are indexed."""
        self.add(self._node_ids(datasets))

    def delete_dataset(self, ds):
        """Remove the node ``ds`` from the index. Call this before its row is changed or
        deleted."""
        self.remove(self._node_ids([ds]))

    def update_dataset(self, ds):
        """Replace the node ``ds`` in the index with the values of ``ds``. Call this before
        the new values are stored."""
        (id_,) = self._node_ids([ds])
        with self.db.atomic():
            self.remove([id_])
            if not self.exists():
                self.db.execute_sql(create_table_sql(self.table, self.schema))
            self._write([(id_, format_dataset(ds))])

    # Searching

    @staticmethod
    def escape_search_for_fts5(string: str) -> str:
        # Use SQL Escape syntax to properly escape specials chars
        # see https://stackoverflow.com/a/43756146
        #
        # We split each word and enclose it inside "" to escape the specials characters that it may contain
        # If the word was ending with a * wildcard, we keep it but outside the escaped string
        #
        # `Com* cheese cow's` is converted to `"com"* "cheese" "cow's"`

        return " ".join(
            [
                f'"{term[:-1]}"*' if term.endswith("*") else f'"{term}"'
                for term in string.replace('"', "").split()
            ]
        )

    def _matching_ids(self, string, limit=None, weights=None) -> list[int]:
        """Ids of the nodes which match ``string``, best first. ``"*"`` gives all nodes of
        the database."""
        from bw2data.backends.schema import ActivityDataset as AD

        if string == "*":
            query = AD.select(AD.id).where(AD.database == self.database).order_by(AD.id)
            if limit is not None:
                query = query.limit(limit)
            return [id_ for (id_,) in query.tuples()]
        if not self.exists():
            return []
        match = self.escape_search_for_fts5(string)
        if not match:
            return []
        weights = weights or {}
        boosts = ", ".join(repr(float(weights.get(field, 1.0))) for field in FIELDS)
        sql = (
            f'SELECT rowid FROM {self.schema}."{self.table}" WHERE "{self.table}" MATCH ? '
            f'ORDER BY bm25("{self.table}", {boosts})'
        )
        params = [match]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with closing(self.db.execute_sql(sql, params)) as cursor:
            return [row[0] for row in cursor.fetchall()]

    def search(self, string, limit=None, weights=None, mask=None, filter=None):
        """Search the index. Returns one dictionary per node, best match first, with its
        ``id``, ``database`` and ``code``, and the indexed text of the columns ``FIELDS``."""
        # Skip SQL-level limit when post-filtering is needed so we don't
        # prematurely discard results that would survive the filter/mask step.
        sql_limit = None if (filter or mask) else limit
        ids = self._matching_ids(string, limit=sql_limit, weights=weights)
        results = [
            {**values, "database": database, "code": code, "id": id_}
            for id_, database, code, values in self._stored(ids)
            # Not the nodes of an index which was not kept up to date
            if database == self.database
        ]

        if filter:
            normalized = {k: v.lower() if isinstance(v, str) else v for k, v in filter.items()}
            results = [
                r
                for r in results
                if all(str(normalized[k]) in str(r.get(k, "")) for k in normalized)
            ]
        if mask:
            normalized = {k: v.lower() if isinstance(v, str) else v for k, v in mask.items()}
            results = [
                r
                for r in results
                if not any(str(normalized[k]) in str(r.get(k, "")) for k in normalized)
            ]
        if limit is not None:
            results = results[:limit]
        return results
