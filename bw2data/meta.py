import copy
import datetime
import pickle
import warnings
from collections.abc import Mapping, MutableMapping

from peewee import JOIN, Value

from bw2data.serialization import PickledDict
from bw2data.signals import on_database_delete, on_database_metadata_change
from bw2data.snowflake_ids import snowflake_id_generator


_KNOWN_FIELDS = frozenset(
    {
        "backend",
        "depends",
        "dirty",
        "geocollections",
        "modified",
        "number",
        "searchable",
        "version",
    }
)


#: Rows per query, to stay well under SQLite's limit on bound variables
_CHUNK = 300

#: Attempts to insert new geomapping rows when snowflake ids collide (see ``GeoMapping.add``)
_SNOWFLAKE_ATTEMPTS = 5


def _chunks(items, size=_CHUNK):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _write_transaction(model):
    """Transaction which takes the SQLite write lock immediately.

    A default (deferred) transaction which reads before writing can fail at once with
    "database is locked" when another process holds a read lock and also wants to write;
    taking the write lock first makes concurrent writers wait for each other instead."""
    return model._meta.database.atomic("IMMEDIATE")


def _insert_with_snowflake_ids(model, rows, still_missing, what):
    """Insert ``rows`` (dicts without ``id``) into ``model`` with new snowflake ids.

    Rows which conflict with an existing row are ignored. Call within a write transaction.
    ``still_missing(rows)`` returns the rows which aren't stored after an insert: these can only
    have been ignored because their id was already used, as processes on one machine share the
    generator's ``instance`` and can produce the same id in the same millisecond. They are
    retried with new ids."""
    for _ in range(_SNOWFLAKE_ATTEMPTS):
        for batch in _chunks(rows):
            model.insert_many(
                [{**row, "id": next(snowflake_id_generator)} for row in batch]
            ).on_conflict_ignore().execute()
        rows = still_missing(rows)
        if not rows:
            return
    raise RuntimeError(f"Could not assign unique ids to {what} {rows[:20]!r}")


def _canonical_geo_key(key):
    """Return ``key`` as a hashable geomapping key, or raise ``ValueError``.

    Valid keys are a non-empty string (a location without a geocollection, e.g. ``"CH"``), or a
    pair of non-empty strings ``(geocollection, feature id)``, e.g. ``("world", "CH")``. A
    list pair is accepted and returned as a tuple."""
    if isinstance(key, str):
        if key:
            return key
    elif isinstance(key, (tuple, list)) and len(key) == 2:
        collection, location = key
        if isinstance(collection, str) and isinstance(location, str) and collection and location:
            return (collection, location)
    raise ValueError(
        f"Invalid geomapping key {key!r}: must be a location string, or a "
        "`(geocollection, feature id)` tuple of two strings"
    )


def _split_geo_key(key):
    """Split a canonical key into ``(geocollection name or None, location)``."""
    return key if isinstance(key, tuple) else (None, key)


def _join_geo_key(collection, location):
    return location if collection is None else (collection, location)


class GeoMapping(Mapping):
    """Mapping from locations to integers, used in processed datapackages (e.g. as matrix
    ``col`` indices for regionalized characterization factors).

    Stored in the ``Geocollection`` and ``GeoMappingRecord`` tables in the per-project
    ``lci/databases.db``. Previously stored in ``geomapping.pickle``; that file is migrated
    automatically on first access, keeping existing ids, and left in place as a backup.

    Keys are either a location string (``"CH"``), or a ``(geocollection, feature id)`` tuple of
    two strings (``("world", "CH")``). Use ``add`` to add keys; ids are assigned by SQLite, so
    concurrent adds from several processes can't collide, and ids are never reused after a
    key is deleted. ``"GLO"`` and ``config.global_location`` are always present.
    """

    #: Legacy storage file, migrated on first access
    filename = "geomapping.pickle"

    def __init__(self):
        # No database access here: on a project switch, metadata objects are reset before the
        # SQLite files are switched to the new project.
        self._ready = False
        # A key is never reassigned to a different id, so cached ids stay correct. Misses fall
        # through to SQLite, to find keys added by other processes.
        self._cache = {}

    # ------------------------------------------------------------------
    # Mapping interface
    # ------------------------------------------------------------------

    def __getitem__(self, key):
        key = _canonical_geo_key(key)
        self._ensure_ready()
        try:
            return self._cache[key]
        except KeyError:
            pass
        found = self._fetch([key])
        if key not in found:
            raise KeyError(key)
        return found[key]

    def __contains__(self, key):
        try:
            self[key]
        except KeyError:
            return False
        return True

    def __iter__(self):
        return iter(self._load_all())

    def __len__(self):
        self._ensure_ready()
        from bw2data.backends.schema import GeoMappingRecord

        return GeoMappingRecord.select().count()

    def items(self):
        return self._load_all().items()

    def keys(self):
        return self._load_all().keys()

    def values(self):
        return self._load_all().values()

    def __setitem__(self, key, value):
        raise NotImplementedError("Use `geomapping.add()`; ids are assigned automatically")

    def __delitem__(self, key):
        key = _canonical_geo_key(key)
        if key not in self:
            raise KeyError(key)
        self.delete([key])

    def __str__(self):
        return f"Mapping from locations to integer ids, with {len(self)} locations"

    __repr__ = __str__

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def add(self, keys):
        """Add locations. Keys already present are ignored. Geocollections named in the keys
        are created if needed.

        Args:
            * *keys* (iterable): The keys to add.

        """
        from bw2data.backends.schema import GeoMappingRecord

        keys = list(dict.fromkeys(_canonical_geo_key(key) for key in keys))
        self._ensure_ready()
        missing = [key for key in keys if key not in self._cache]
        if not missing:
            return
        # Another process may have added some of these since our cache was filled. Check with
        # a plain read first, so we only take the write lock when there is something to write.
        found = self._fetch(missing)
        missing = [key for key in missing if key not in found]
        if not missing:
            return

        with _write_transaction(GeoMappingRecord):
            # Check again: another process may have added some while we waited for the lock
            found = self._fetch(missing)
            missing = [key for key in missing if key not in found]
            if not missing:
                return
            collection_ids = self._add_collections(
                {key[0] for key in missing if isinstance(key, tuple)}
            )
            names_by_id = {id_: name for name, id_ in collection_ids.items()}

            def still_missing(rows):
                keys = [
                    _join_geo_key(names_by_id.get(row["geocollection"]), row["location"])
                    for row in rows
                ]
                found = self._fetch(keys)
                return [row for row, key in zip(rows, keys) if key not in found]

            rows = [
                {"geocollection": collection_ids.get(collection), "location": location}
                for collection, location in map(_split_geo_key, missing)
            ]
            _insert_with_snowflake_ids(
                GeoMappingRecord, rows, still_missing, "geomapping locations"
            )

    def delete(self, keys):
        """Delete locations. Their ids are not reused. Keys not present are ignored.

        Args:
            *keys* (iterable): The keys to delete.

        """
        from bw2data.backends.schema import GeoMappingRecord

        keys = [_canonical_geo_key(key) for key in keys]
        self._ensure_ready()
        ids = list(self._fetch(keys).values())
        with _write_transaction(GeoMappingRecord):
            for batch in _chunks(ids):
                GeoMappingRecord.delete().where(GeoMappingRecord.id.in_(batch)).execute()
        for key in keys:
            self._cache.pop(key, None)

    def collections(self):
        """Sorted names of all geocollections."""
        self._ensure_ready()
        from bw2data.backends.schema import Geocollection

        return sorted(row.name for row in Geocollection.select(Geocollection.name))

    def locations(self, collection=None):
        """Keys and ids of the locations in ``collection``, or of the locations without a
        geocollection if ``collection`` is ``None``."""
        self._ensure_ready()
        from bw2data.backends.schema import GeoMappingRecord, Geocollection

        query = GeoMappingRecord.select(GeoMappingRecord.id, GeoMappingRecord.location)
        if collection is None:
            query = query.where(GeoMappingRecord.geocollection.is_null())
        else:
            query = query.join(Geocollection).where(Geocollection.name == collection)
        result = {_join_geo_key(collection, location): id_ for id_, location in query.tuples()}
        self._cache.update(result)
        return result

    @property
    def data(self):
        """All locations and their ids, as a plain dict."""
        return dict(self._load_all())

    def flush(self):
        """Deprecated. Changes are saved immediately."""
        warnings.warn(
            "`geomapping.flush()` is deprecated and does nothing; changes are saved immediately.",
            DeprecationWarning,
            stacklevel=2,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_collections(self, names):
        """Create the geocollections in ``names`` which don't exist yet, and return the ids of
        all of them. Call within a write transaction."""
        from bw2data.backends.schema import Geocollection

        def still_missing(rows):
            found = self._collection_ids(row["name"] for row in rows)
            return [row for row in rows if row["name"] not in found]

        names = sorted(names)
        if names:
            _insert_with_snowflake_ids(
                Geocollection, [{"name": name} for name in names], still_missing, "geocollections"
            )
        return self._collection_ids(names)

    def _collection_ids(self, names):
        from bw2data.backends.schema import Geocollection

        names = list(names)
        if not names:
            return {}
        return {
            name: id_
            for id_, name in Geocollection.select(Geocollection.id, Geocollection.name)
            .where(Geocollection.name.in_(names))
            .tuples()
        }

    def _fetch(self, keys):
        """Look up canonical ``keys`` in SQLite and update the cache. Returns the ids found."""
        from bw2data.backends.schema import GeoMappingRecord

        by_collection = {}
        for collection, location in map(_split_geo_key, keys):
            by_collection.setdefault(collection, []).append(location)
        collection_ids = self._collection_ids(c for c in by_collection if c is not None)

        found = {}
        for collection, locations in by_collection.items():
            if collection is None:
                condition = GeoMappingRecord.geocollection.is_null()
            elif collection in collection_ids:
                condition = GeoMappingRecord.geocollection == collection_ids[collection]
            else:
                continue
            for batch in _chunks(locations):
                query = GeoMappingRecord.select(
                    GeoMappingRecord.id, GeoMappingRecord.location
                ).where(condition & GeoMappingRecord.location.in_(batch))
                for id_, location in query.tuples():
                    found[_join_geo_key(collection, location)] = id_
        self._cache.update(found)
        return found

    def _load_all(self):
        """Read all locations from SQLite, replace the cache with them, and return it."""
        self._ensure_ready()
        from bw2data.backends.schema import GeoMappingRecord, Geocollection

        query = (
            GeoMappingRecord.select(
                GeoMappingRecord.id, GeoMappingRecord.location, Geocollection.name
            )
            .join(Geocollection, JOIN.LEFT_OUTER)
            .order_by(GeoMappingRecord.id)
        )
        self._cache = {
            _join_geo_key(name, location): id_ for id_, location, name in query.tuples()
        }
        return self._cache

    def _ensure_ready(self):
        if self._ready:
            return
        self._migrate_from_pickle()
        self._ready = True
        from bw2data.configuration import config

        self.add(["GLO", config.global_location])

    def _migrate_from_pickle(self):
        """One-time migration from ``geomapping.pickle``, keeping existing ids.

        Integer feature ids in ``(geocollection, feature id)`` keys are converted to strings.
        Raises ``ValueError`` without changing anything if the file has keys or ids which
        can't be migrated."""
        from bw2data.backends.schema import GeoMappingRecord, Geocollection
        from bw2data.project import projects

        path = projects.dir / self.filename
        if not path.is_file() or GeoMappingRecord.select().exists():
            return

        try:
            with open(path, "rb") as f:
                legacy = pickle.load(f)
            legacy = dict(legacy)
        except Exception as err:
            warnings.warn(
                f"Can't read {path} ({err}); starting with an empty geomapping. Processed "
                "datapackages which use the old location ids must be processed again.",
                stacklevel=3,
            )
            return

        rows, converted, invalid = {}, 0, []
        for key, id_ in legacy.items():
            if (
                isinstance(key, tuple)
                and len(key) == 2
                and isinstance(key[0], str)
                and isinstance(key[1], int)
                and not isinstance(key[1], bool)
            ):
                key = (key[0], str(key[1]))
                converted += 1
            try:
                key = _canonical_geo_key(key)
            except ValueError:
                invalid.append(key)
                continue
            if not isinstance(id_, int) or isinstance(id_, bool) or id_ < 1:
                invalid.append(key)
            elif key in rows:
                raise ValueError(
                    f"Can't migrate {path}: key {key!r} appears more than once after "
                    "converting integer feature ids to strings. Fix the file and try again."
                )
            else:
                rows[key] = id_
        if invalid:
            raise ValueError(
                f"Can't migrate {path}: invalid keys or ids for {invalid[:20]!r}. Keys must "
                "be a location string or a `(geocollection, feature id)` tuple of two strings, "
                "and ids must be positive integers. Fix the file and try again."
            )
        if len(set(rows.values())) != len(rows):
            raise ValueError(f"Can't migrate {path}: the same id is used for several keys.")

        message = (
            f"Migrating {path} to SQLite (one-time operation). "
            "The original file is kept as a backup."
        )
        if converted:
            message += f" Converted {converted} integer feature ids to strings."
        warnings.warn(message, stacklevel=3)

        with _write_transaction(GeoMappingRecord):
            collection_ids = self._add_collections(
                {key[0] for key in rows if isinstance(key, tuple)}
            )
            records = []
            for key, id_ in rows.items():
                collection, location = _split_geo_key(key)
                records.append(
                    {
                        "id": id_,
                        "geocollection": collection_ids.get(collection),
                        "location": location,
                    }
                )
            for batch in _chunks(records):
                GeoMappingRecord.insert_many(batch).execute()


def _row_as_dict(row, known_fields=_KNOWN_FIELDS) -> dict:
    """Metadata dict for a registry row: set (non-NULL) known fields plus ``extra``."""
    d = {f: getattr(row, f) for f in known_fields if getattr(row, f) is not None}
    d.update(row.extra or {})
    return d


class RecordProxy(MutableMapping):
    """Dict-like view of a single metadata row, e.g. a
    :class:`~bw2data.backends.schema.DatabaseRecord` or a
    :class:`~bw2data.backends.schema.MethodRecord`.

    Known fields are columns of the row; all other keys are stored in its ``extra`` JSON
    column. Changes are kept in memory until :meth:`save` is called, as with the old JSON
    files and their ``flush()``. A registry (``databases``, ``methods``, ...) hands out one
    proxy per name, so e.g. ``Database(name).metadata`` and ``databases[name]`` share pending
    changes.

    .. code-block:: python

        db = Database("my_db")
        db.metadata["description"] = "Example"
        db.metadata.save()
    """

    def __init__(self, row, parent):
        self._parent = parent
        self._load(row)

    def _load(self, row):
        """Point this proxy at ``row`` and treat its values as saved."""
        self._row = row
        self._saved = copy.deepcopy(self._all_fields())

    def _all_fields(self):
        """Return only the fields that have been explicitly set (non-NULL)."""
        return _row_as_dict(self._row, self._parent._known_fields)

    def _changed_keys(self) -> set:
        """Keys whose current value differs from the last loaded or saved value.

        Compares values rather than tracking assignments, so in-place changes to nested values
        (e.g. ``metadata["depends"].append(...)``) are detected too."""
        current = self._all_fields()
        return {
            key
            for key in current.keys() | self._saved.keys()
            if key not in current or key not in self._saved or current[key] != self._saved[key]
        }

    def _has_changes(self) -> bool:
        return bool(self._changed_keys())

    def __getitem__(self, key):
        if key in self._parent._known_fields:
            val = getattr(self._row, key)
            if val is None:
                raise KeyError(key)
            return val
        extra = self._row.extra or {}
        if key in extra:
            return extra[key]
        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self._parent._known_fields:
            setattr(self._row, key, value)
        else:
            extra = dict(self._row.extra or {})
            extra[key] = value
            self._row.extra = extra

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        if key in self._parent._known_fields:
            setattr(self._row, key, None)
        else:
            extra = dict(self._row.extra)
            del extra[key]
            self._row.extra = extra or None

    def save(self, signal: bool = True):
        """Write pending changes to SQLite.

        Only changed keys are written, so changes saved through other code since this proxy
        was loaded are kept. For ``databases``, sends ``on_database_metadata_change`` once on
        event-sourced projects, unless ``signal`` is ``False``."""
        changed = self._changed_keys()
        if not changed:
            return
        send = signal and self._parent._should_signal()
        old = self._parent._as_dict() if send else None
        self._parent._write_changes(self, changed)
        if send:
            self._parent._emit(old)

    def __iter__(self):
        return iter(self._all_fields())

    def __len__(self):
        return len(self._all_fields())

    def __contains__(self, key):
        if key in self._parent._known_fields:
            return getattr(self._row, key) is not None
        return key in (self._row.extra or {})

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __copy__(self):
        return dict(self)

    def __repr__(self):
        return repr(dict(self))



#: Previous name of :class:`RecordProxy`
DatabaseRecordProxy = RecordProxy


class _RecordRegistry(MutableMapping):
    """Dict-like registry of metadata rows in one table of the per-project
    ``lci/databases.db``, with one row per name. Base class of ``Databases`` and of the LCIA
    registries ``methods``, ``weightings`` and ``normalizations``.

    ``registry[name]`` returns a :class:`RecordProxy`; changes to it are saved with its
    ``save()``. Subclasses implement ``_model`` and set ``_known_fields`` (the keys stored in
    their own column; all other keys go into ``extra``), and may override ``_key`` to
    normalize names.
    """

    _known_fields: frozenset = frozenset()
    #: Name of one entry, for error messages
    _what = "Object"
    #: Public name of the registry and how to save one entry, for the deprecation warning of
    #: ``flush``
    _registry_name = "registry"
    _flush_hint = "`registry[name].save()`"

    def __init__(self):
        # One proxy per name, so pending changes survive between accesses
        self._proxies = {}

    def _model(self):
        raise NotImplementedError

    def _key(self, name):
        """``name`` as stored in the table, or ``None`` if it can't be a name here."""
        return name

    def _order(self):
        """Order of iteration, as peewee ``order_by`` arguments."""
        return ()

    def _where(self, key):
        """Condition selecting the row of ``key`` (a name as returned by ``_key``)."""
        return self._model().name == key

    # ------------------------------------------------------------------
    # MutableMapping interface
    # ------------------------------------------------------------------

    def __getitem__(self, name):
        key = self._key(name)
        if key is None:
            raise KeyError(name)
        model = self._model()

        proxy = self._proxies.get(key)
        if proxy is not None and proxy._has_changes():
            return proxy

        row = model.get_or_none(self._where(key))
        if row is None:
            self._proxies.pop(key, None)
            raise KeyError(name)

        # No pending changes, so refresh from SQLite in case it was changed elsewhere
        if proxy is None:
            proxy = self._proxies[key] = RecordProxy(row, self)
        else:
            proxy._load(row)
        return proxy

    def __iter__(self):
        model = self._model()
        # Read completely first, so that entries can be deleted while iterating
        return iter([row.name for row in model.select(model.name).order_by(*self._order())])

    def __len__(self):
        return self._model().select().count()

    def __contains__(self, name):
        key = self._key(name)
        if key is None:
            return False
        model = self._model()
        return model.select().where(self._where(key)).exists()

    @property
    def list(self):
        """Sorted list of names."""
        return sorted(self)

    def random(self):
        """Return a random name, or ``None`` if empty."""
        import random as _random

        names = list(self)
        return _random.choice(names) if names else None

    # ------------------------------------------------------------------
    # Deprecated shims
    # ------------------------------------------------------------------

    def flush(self, signal: bool = True):
        """Deprecated. Saves pending metadata changes of all entries.

        Save the changes of one entry with ``registry[name].save()`` instead."""
        warnings.warn(
            f"`{self._registry_name}.flush()` is deprecated; save changes to an entry "
            f"with {self._flush_hint} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._save_pending(signal=signal)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _save_pending(self, signal: bool = True):
        """Save the pending changes of all proxies."""
        pending = [proxy for proxy in self._proxies.values() if proxy._has_changes()]
        if not pending:
            return
        send = signal and self._should_signal()
        old = self._as_dict() if send else None
        for proxy in pending:
            self._write_changes(proxy, proxy._changed_keys())
        if send:
            self._emit(old)

    def _split(self, value) -> tuple[dict, dict]:
        """Split a metadata dict into ``(known fields, extra)``."""
        value = dict(value)  # copy to avoid mutating caller's dict
        known = {k: value.pop(k) for k in list(value) if k in self._known_fields}
        return known, value

    def _write_changes(self, proxy, changed):
        """Write the ``changed`` keys of ``proxy`` to SQLite, then reload it.

        Known fields are written column by column. Custom keys are merged into the stored
        ``extra`` value, so keys saved elsewhere since ``proxy`` was loaded are kept."""
        from bw2data.errors import UnknownObject

        model = self._model()
        row = proxy._row
        where = self._where(row.name)
        updates = {
            getattr(model, key): getattr(row, key) for key in changed if key in self._known_fields
        }
        extra_changed = [key for key in changed if key not in self._known_fields]

        with model._meta.database.atomic():
            if extra_changed:
                stored = dict(model.select(model.extra).where(where).scalar() or {})
                mine = row.extra or {}
                for key in extra_changed:
                    if key in mine:
                        stored[key] = mine[key]
                    else:
                        stored.pop(key, None)
                updates[model.extra] = stored or None
            if not model.update(updates).where(where).execute():
                raise UnknownObject(f"{self._what} {row.name!r} is not registered")
            proxy._load(model.get(where))

    def _should_signal(self) -> bool:
        """Whether a save should build ``old``/``new`` snapshots and call ``_emit``."""
        return False

    def _emit(self, old):
        """Send a change signal with ``old`` and the current state."""

    def _as_dict(self):
        """Plain dict of all saved metadata, read from SQLite.

        Pending changes that haven't been saved are not included."""
        model = self._model()
        return {
            row.name: _row_as_dict(row, self._known_fields)
            for row in model.select().order_by(*self._order())
        }

    @property
    def data(self):
        """The full saved metadata as a plain dict."""
        return self._as_dict()


class Databases(_RecordRegistry):
    """Dict-like registry of database metadata, backed by the ``DatabaseRecord`` SQLite table
    in the per-project ``lci/databases.db``.

    Previously stored in ``databases.json``; the file is moved into SQLite by schema migration 3
    (see ``bw2data.backends.migrations``) when the project is opened.

    Adding, replacing or deleting a whole entry (``databases[name] = {...}``,
    ``del databases[name]``) is saved immediately. Changes to an entry
    (``databases[name]["key"] = value``) are saved by ``databases[name].save()``.
    """

    _save_signal = on_database_metadata_change

    _known_fields = _KNOWN_FIELDS
    _what = "Database"
    _registry_name = "databases"
    _flush_hint = "`Database(name).metadata.save()` or `databases[name].save()`"

    def _model(self):
        from bw2data.backends.schema import DatabaseRecord

        return DatabaseRecord

    # ------------------------------------------------------------------
    # MutableMapping interface
    # ------------------------------------------------------------------

    def __setitem__(self, name, value):
        from bw2data.backends.schema import DatabaseRecord

        send = self._should_signal()
        old = self._as_dict() if send else None
        value = dict(value)  # copy to avoid mutating caller's dict
        known = {k: value.pop(k) for k in list(value) if k in _KNOWN_FIELDS}
        DatabaseRecord.replace(name=name, extra=value or None, **known).execute()
        if name in self._proxies:
            self._proxies[name]._load(DatabaseRecord.get(DatabaseRecord.name == name))
        if send:
            self._emit(old)

    def __delitem__(self, name: str, signal: bool = True):
        from bw2data import Database

        if name not in self:
            raise KeyError(name)

        try:
            Database(name).delete(warn=False, signal=False)
        except Exception:
            warnings.warn(
                """
Deletion unsuccessful due to database error.
Metadata state is unchanged, but database state is unknown.
            """
            )
            return

        from bw2data.backends.schema import DatabaseRecord

        DatabaseRecord.delete().where(DatabaseRecord.name == name).execute()
        self._proxies.pop(name, None)

        if signal:
            on_database_delete.send(self, name=name)

    def __str__(self):
        names = list(self)
        if not names:
            return "Databases dictionary with 0 objects"
        elif len(names) > 20:
            return (
                "Databases dictionary with {} objects, including:"
                "{}\nUse `list(this object)` to get the complete list."
            ).format(
                len(names),
                "".join(["\n\t{}".format(x) for x in sorted(names)[:10]]),
            )
        else:
            return ("Databases dictionary with {} object(s):{}").format(
                len(names),
                "".join(["\n\t{}".format(x) for x in sorted(names)]),
            )

    def __repr__(self):
        return str(self)

    # ------------------------------------------------------------------
    # Domain methods
    # ------------------------------------------------------------------

    def increment_version(self, database, number=None):
        """Increment the ``database`` version and save its metadata. Returns the new version."""
        metadata = self[database]
        metadata["version"] = (metadata.get("version") or 0) + 1
        if number is not None:
            metadata["number"] = number
        metadata.save()
        return metadata["version"]

    def version(self, database):
        """Return the ``database`` version."""
        return self[database].get("version")

    def set_modified(self, database):
        """Set the ``modified`` timestamp and save the database's metadata."""
        metadata = self[database]
        metadata["modified"] = datetime.datetime.now().isoformat()
        metadata.save()

    def set_dirty(self, database):
        """Mark ``database`` as needing processing and save its metadata."""
        metadata = self[database]
        metadata["modified"] = datetime.datetime.now().isoformat()
        metadata["dirty"] = True
        metadata.save()

    def clean(self):
        """Process all databases marked as dirty."""
        from bw2data import Database

        dirty = [name for name in self if self[name].get("dirty")]
        for name in dirty:
            Database(name).process()
            metadata = self[name]
            metadata.pop("dirty", None)
            metadata.save()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_signal(self) -> bool:
        """Whether a save should build ``old``/``new`` snapshots and emit
        ``on_database_metadata_change``.

        The signal only feeds revision generation, which is skipped on unsourced projects, so
        don't pay for two full-table snapshots per save in that case (see #299)."""
        from bw2data.project import projects

        return projects.dataset.is_sourced

    def _emit(self, old):
        """Emit on_database_metadata_change with the given old state and current new state."""
        on_database_metadata_change.send(self, old=old, new=self._as_dict())

    @_RecordRegistry.data.setter
    def data(self, new_data):
        """Replace all metadata rows. Used by the revision system to replay patches."""
        from bw2data.backends.schema import DatabaseRecord

        DatabaseRecord.delete().execute()
        for name, meta in new_data.items():
            meta = dict(meta)
            known = {k: meta.pop(k) for k in list(meta) if k in _KNOWN_FIELDS}
            DatabaseRecord.replace(name=name, extra=meta or None, **known).execute()
        self._proxies.clear()


class CalculationSetups(PickledDict):
    """A dictionary for calculation setups.

    Keys:
    * `inv`: List of functional units, e.g. ``[{(key): amount}, {(key): amount}]``
    * `ia`: List of LCIA methods, e.g. ``[(method), (method)]``.

    """

    filename = "setups.pickle"


class DynamicCalculationSetups(PickledDict):
    """A dictionary for Dynamic calculation setups.

    Keys:
    * `inv`: List of functional units, e.g. ``[{(key): amount}, {(key): amount}]``
    * `ia`: Dictionary of orst case LCIA method and the relative dynamic LCIA method, e.g. `` [{dLCIA_method_1_worstcase:dLCIA_method_1 , dLCIA_method_2_worstcase:dLCIA_method_2}]``.

    """

    filename = "dynamicsetups.pickle"


class _LCIARegistry(_RecordRegistry):
    """Dict-like registry of LCIA objects of one kind (methods, weightings or
    normalizations), backed by one table in the per-project ``lci/databases.db``.

    Names are non-empty tuples, like ``("ReCiPe", "climate change")``; a list is accepted
    wherever a name is expected and treated as the equal tuple. Previously stored in a JSON
    file in the project directory; the file is moved into SQLite by schema migration 4 (see
    ``bw2data.backends.migrations``) when the project is opened.

    Adding, replacing or deleting a whole entry (``methods[name] = {...}``,
    ``del methods[name]``) is saved immediately. Changes to an entry
    (``methods[name]["key"] = value``) are saved by ``methods[name].save()``, or with
    ``Method(name).metadata.save()``.
    """

    _known_fields = frozenset({"abbreviation"})

    def _key(self, name):
        if isinstance(name, list):
            name = tuple(name)
        if isinstance(name, tuple) and name:
            return name
        return None

    def _order(self):
        return (self._model().id,)

    def _where(self, key):
        # Peewee would otherwise compare with a row value of the tuple's elements
        field = self._model().name
        return field == Value(key, converter=field.db_value, unpack=False)

    def __setitem__(self, name, value):
        key = self._key(name)
        if key is None:
            raise TypeError(f"{self._what} names must be non-empty tuples, not {name!r}")
        known, extra = self._split(value)
        # Replaces all metadata: known fields which aren't given are cleared
        fields = {
            **{field: None for field in self._known_fields},
            **known,
            "extra": extra or None,
        }
        model = self._model()
        with _write_transaction(model):
            row = model.get_or_none(self._where(key))
            if row is None:
                model.insert(id=self._unused_id(model), name=key, **fields).execute()
            else:
                # Updated in place, so that the id, which other rows refer to, is kept
                model.update(**fields).where(model.id == row.id).execute()
        if key in self._proxies:
            self._proxies[key]._load(model.get(self._where(key)))

    def __delitem__(self, name):
        key = self._key(name)
        model = self._model()
        if key is None or not model.delete().where(self._where(key)).execute():
            raise KeyError(name)
        self._proxies.pop(key, None)

    @staticmethod
    def _unused_id(model) -> int:
        """A new snowflake id which isn't used in ``model`` yet. Call within a write
        transaction. Processes on one machine can produce the same id in the same
        millisecond, so an id which is already used is replaced by the next one."""
        for _ in range(_SNOWFLAKE_ATTEMPTS):
            id_ = next(snowflake_id_generator)
            if not model.select().where(model.id == id_).exists():
                return id_
        raise RuntimeError(f"Could not find an unused id for table {model._meta.table_name}")

    def __str__(self):
        names = list(self)
        label = type(self).__name__
        if not names:
            return f"{label} dictionary with 0 objects"
        elif len(names) > 20:
            return (
                "{} dictionary with {} objects, including:"
                "{}\nUse `list(this object)` to get the complete list."
            ).format(label, len(names), "".join(f"\n\t{x}" for x in sorted(names)[:10]))
        return "{} dictionary with {} object(s):{}".format(
            label, len(names), "".join(f"\n\t{x}" for x in sorted(names))
        )

    __repr__ = __str__


class Methods(_LCIARegistry):
    """Registry of LCIA method metadata, in the ``method`` table of ``lci/databases.db``.
    Previously stored in ``methods.json``."""

    _known_fields = frozenset({"abbreviation", "num_cfs", "geocollections"})
    _what = "Method"
    _registry_name = "methods"
    _flush_hint = "`Method(name).metadata.save()` or `methods[name].save()`"

    def _model(self):
        from bw2data.backends.schema import MethodRecord

        return MethodRecord


class WeightingMeta(_LCIARegistry):
    """Registry of LCIA weighting metadata, in the ``weighting`` table of
    ``lci/databases.db``. Previously stored in ``weightings.json``."""

    _what = "Weighting"
    _registry_name = "weightings"
    _flush_hint = "`Weighting(name).metadata.save()` or `weightings[name].save()`"

    def _model(self):
        from bw2data.backends.schema import WeightingRecord

        return WeightingRecord


class NormalizationMeta(_LCIARegistry):
    """Registry of LCIA normalization metadata, in the ``normalization`` table of
    ``lci/databases.db``. Previously stored in ``normalizations.json``."""

    _what = "Normalization"
    _registry_name = "normalizations"
    _flush_hint = "`Normalization(name).metadata.save()` or `normalizations[name].save()`"

    def _model(self):
        from bw2data.backends.schema import NormalizationRecord

        return NormalizationRecord


class Preferences(MutableMapping):
    """A dictionary of project-specific preferences.

    Stored in the ``preference`` table of the per-project ``lci/databases.db``. Keys must be
    strings; values can be any picklable object. Previously stored in ``preferences.pickle``,
    which is migrated when the project is opened (see ``bw2data.backends.migrations``).

    All preferences are read once, on first access; reading never writes to the database.
    Every assignment, deletion or ``flush()`` saves the changes made through this object
    since then, including changes inside a value (``preferences["updates"][key] = True``).
    Only preferences which were changed here are written, so preferences set by another
    process in the meantime are kept, unless both changed the same one. Use ``load()`` to
    read the other process's changes.

    If saving fails, an assignment or deletion is not applied in memory either. This can't be
    done for a save inside a transaction opened by the caller which is rolled back later:
    call ``load()`` after such a rollback."""

    #: Legacy storage file, migrated when the project is opened
    filename = "preferences.pickle"

    #: Values of preferences which are not set. Not stored until they are changed.
    DEFAULTS = {"use_cache": True}

    def __init__(self):
        # No database access here: on a project switch, metadata objects are reset before the
        # SQLite files are switched to the new project.
        self._data = None
        # Pickled values as last read from or written to SQLite, to find what has changed
        self._saved = {}

    @property
    def data(self) -> dict:
        """The preferences as a plain dictionary. Can be changed in place (followed by
        ``flush()``), but not replaced."""
        if self._data is None:
            self.load()
        return self._data

    @staticmethod
    def _pickled(data: dict) -> dict:
        for key in data:
            if not isinstance(key, str):
                raise TypeError(f"Preference keys must be strings, not {type(key).__name__}")
        return {key: pickle.dumps(value, protocol=4) for key, value in data.items()}

    def load(self) -> None:
        """Read all preferences from SQLite, discarding unsaved changes."""
        from bw2data.backends.schema import Preference

        data = {key: value for key, value in Preference.select().tuples()}
        for key, value in self.DEFAULTS.items():
            data.setdefault(key, copy.deepcopy(value))
        self._saved = self._pickled(data)
        self._data = data

    def _save(self, data: dict) -> None:
        """Write the differences between ``data`` and what was last read or written."""
        from bw2data.backends.schema import Preference

        # Before anything is written: a value which can't be pickled must change nothing
        pickled = self._pickled(data)
        changed = [key for key, value in pickled.items() if self._saved.get(key) != value]
        removed = [key for key in self._saved if key not in pickled]
        if not changed and not removed:
            return
        with _write_transaction(Preference):
            for chunk in _chunks(removed):
                Preference.delete().where(Preference.key.in_(chunk)).execute()
            for chunk in _chunks(changed):
                Preference.replace_many(
                    [{"key": key, "value": data[key]} for key in chunk]
                ).execute()
        self._saved = pickled

    def flush(self) -> None:
        """Save all changes to SQLite, including changes made inside values."""
        self._save(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        # Saved first, so that a failed save leaves the preferences in memory as they were
        self._save({**self.data, key: value})
        self._data[key] = value

    def __delitem__(self, key):
        data = dict(self.data)
        del data[key]
        self._save(data)
        del self._data[key]

    def __contains__(self, key):
        return key in self.data

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    @property
    def list(self):
        """Sorted list of preference names."""
        return sorted(self.data)

    def __repr__(self):
        return f"Preferences dictionary with {len(self)} object(s): {self.list}"

    __str__ = __repr__


databases = Databases()
geomapping = GeoMapping()
methods = Methods()
normalizations = NormalizationMeta()
preferences = Preferences()
weightings = WeightingMeta()
calculation_setups = CalculationSetups()
dynamic_calculation_setups = DynamicCalculationSetups()
