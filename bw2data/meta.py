import copy
import hashlib
import datetime
import json
import math
import numbers
import pickle
import warnings
from collections.abc import Mapping, MutableMapping

from peewee import JOIN, SQL, Value

from bw2data.signals import (
    on_database_delete,
    on_database_metadata_change,
    on_extension_attached,
    on_extension_detached,
)
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

#: Most nodes in the functional units of one calculation setup. Their rows are inserted with
#: one query, of six bound variables per row; SQLite allows 32,766 (since version 3.32).
MAX_SETUP_NODES = 5000

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


#: The geocollection of locations given without one, e.g. ``"CH"``; stored without a
#: geocollection. ``("world", "CH")`` is the same location as ``"CH"``.
DEFAULT_GEOCOLLECTION = "world"
#: Names of the global location (in ``DEFAULT_GEOCOLLECTION``), stored as ``"GLO"``. A location
#: given as ``None`` is global as well. ``config.global_location`` is not one of them: a project
#: can make any location its global one.
GLOBAL_LOCATIONS = frozenset({"", "GLO", "Global"})


def _canonical_geo_key(key):
    """Return ``key`` as it is stored in ``geomapping``, or raise ``ValueError``.

    Locations are identified by their names: a location string (``"CH"``, in
    ``DEFAULT_GEOCOLLECTION``), or a ``(geocollection, location)`` pair of strings
    (``("ecoinvent", "RER w/o CH")``). ``(None, "CH")`` and ``("world", "CH")`` are the same as
    ``"CH"``, and are returned as ``"CH"``; ``None``, ``""``, ``"Global"`` and ``"GLO"`` (also in
    ``DEFAULT_GEOCOLLECTION``) are the global location, returned as ``"GLO"``. A list pair is
    accepted and returned as a tuple. Strings are compared as they are: ``"RoW"`` is a location
    like any other."""
    if key is None:
        return "GLO"
    if isinstance(key, str):
        return "GLO" if key in GLOBAL_LOCATIONS else key
    if isinstance(key, (tuple, list)) and len(key) == 2:
        collection, location = key
        if collection is None or collection == DEFAULT_GEOCOLLECTION:
            if location is None or isinstance(location, str):
                return _canonical_geo_key(location)
        elif isinstance(collection, str) and collection and isinstance(location, str) and location:
            return (collection, location)
    raise ValueError(
        f"Invalid geomapping key {key!r}: must be a location string, or a "
        "`(geocollection, location)` tuple of two strings"
    )


def location_id(key) -> int:
    """The id of the location ``key`` in ``geomapping``, the same in every project and every
    file: the first 63 bits of the SHA-256 hash of its geocollection and name. Processed
    datapackages made in different projects, or stored in extension files, can therefore be
    used together without translating their location ids."""
    collection, location = _split_geo_key(_canonical_geo_key(key))
    text = f"{collection or DEFAULT_GEOCOLLECTION}\0{location}"
    value = int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big") >> 1
    return value or 1


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
    automatically on first access, and left in place as a backup.

    Keys are a location string (``"CH"``) or a ``(geocollection, location)`` tuple of two strings
    (``("ecoinvent", "RER w/o CH")``); see ``_canonical_geo_key`` for the forms which name the
    same location (e.g. ``("world", "CH")`` and ``"CH"``, or ``"Global"`` and ``"GLO"``). Use
    ``add`` to add keys. The id of a location is derived from its name (``location_id``), so it
    is the same in every project. ``"GLO"`` and ``config.global_location`` are always present.
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
            ids = {}
            for key in missing:
                id_ = location_id(key)
                if id_ in ids:
                    raise ValueError(
                        f"Locations {ids[id_]!r} and {key!r} have the same id {id_}; one of "
                        "them must be renamed"
                    )
                ids[id_] = key
            for batch in _chunks(list(ids)):
                taken = GeoMappingRecord.select(GeoMappingRecord.id).where(
                    GeoMappingRecord.id.in_(batch)
                )
                for (id_,) in taken.tuples():
                    raise ValueError(
                        f"Location {ids[id_]!r} has the same id {id_} as a location which is "
                        "already in `geomapping`; one of them must be renamed"
                    )
            collection_ids = self._add_collections(
                {key[0] for key in missing if isinstance(key, tuple)}
            )
            rows = [
                {
                    "id": id_,
                    "geocollection": collection_ids.get(_split_geo_key(key)[0]),
                    "location": _split_geo_key(key)[1],
                }
                for id_, key in ids.items()
            ]
            for batch in _chunks(rows):
                GeoMappingRecord.insert_many(batch).execute()

    def delete(self, keys):
        """Delete locations. A location which is added again gets the same id (see
        ``location_id``). Keys not present are ignored.

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
        """Keys and ids of the locations in ``collection``, or of the locations in
        ``DEFAULT_GEOCOLLECTION`` (stored without a geocollection) if ``collection`` is ``None``
        or ``DEFAULT_GEOCOLLECTION``."""
        if collection == DEFAULT_GEOCOLLECTION:
            collection = None
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
        from bw2data.configuration import config

        # Set first, as ``add`` calls this method; not ready if it fails, to try again
        self._ready = True
        try:
            self._migrate_from_pickle()
            self.add(["GLO", config.global_location])
        except BaseException:
            self._ready = False
            raise

    def _migrate_from_pickle(self):
        """One-time migration from ``geomapping.pickle``: its locations are added. Their ids are
        derived from their names (``location_id``), as for every location; the ids in the file
        are not kept, and schema migration 10 made the processed datapackages which use them be
        processed again. Integer feature ids in ``(geocollection, feature id)`` keys are
        converted to strings; keys which still aren't locations are skipped with a warning."""
        from bw2data.backends.schema import GeoMappingRecord
        from bw2data.project import projects

        path = projects.dir / self.filename
        if not path.is_file() or GeoMappingRecord.select().exists():
            return

        try:
            with open(path, "rb") as f:
                legacy = dict(pickle.load(f))
        except Exception as err:
            warnings.warn(
                f"Can't read {path} ({err}); starting with an empty geomapping.",
                stacklevel=3,
            )
            return

        keys, invalid = [], []
        for key in legacy:
            if (
                isinstance(key, tuple)
                and len(key) == 2
                and isinstance(key[0], str)
                and isinstance(key[1], int)
                and not isinstance(key[1], bool)
            ):
                key = (key[0], str(key[1]))
            try:
                keys.append(_canonical_geo_key(key))
            except ValueError:
                invalid.append(key)
        message = (
            f"Migrating {path} to SQLite (one-time operation). The original file is kept as a "
            "backup."
        )
        if invalid:
            message += f" Keys which are not locations are skipped: {invalid[:20]!r}"
        warnings.warn(message, stacklevel=3)
        self.add(keys)


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

    def _get_row(self, name):
        """The saved row of ``name``, or ``None``."""
        key = self._key(name)
        if key is None:
            return None
        return self._model().get_or_none(self._where(key))

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
            # The row count can't tell: an update through the view of a read-only extension
            # (``bw2data.backends.extensions``) always reports 0 rows, as SQLite doesn't count
            # changes made by triggers
            model.update(updates).where(where).execute()
            if not model.select().where(where).exists():
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
        from bw2data.backends.extensions import check_writable
        from bw2data.backends.schema import DatabaseRecord

        check_writable(name, "its metadata can't be replaced")
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
        from bw2data.backends.extensions import check_writable

        if name not in self:
            raise KeyError(name)
        # Before anything is changed, e.g. calculation setups
        check_writable(name, "can't be deleted; `projects.detach_extension` removes the extension")

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

    def _write_changes(self, proxy, changed):
        """As ``_RecordRegistry._write_changes``, but a database stored in a read-only extension
        only takes changes to its project-local keys; other changes are discarded."""
        from bw2data.backends.extensions import LOCAL_METADATA_KEYS, extension_of
        from bw2data.backends.schema import DatabaseRecord
        from bw2data.errors import ReadOnlyExtensionError

        name = proxy._row.name
        extension = extension_of(name)
        if extension is not None:
            other = sorted(set(changed) - LOCAL_METADATA_KEYS)
            if other:
                proxy._load(DatabaseRecord.get(DatabaseRecord.name == name))
                raise ReadOnlyExtensionError(
                    f"Database {name!r} is stored in the read-only {extension}, and only its "
                    f"keys {', '.join(sorted(LOCAL_METADATA_KEYS))} can be changed, not "
                    f"{', '.join(other)}. The unsaved changes of its metadata were discarded."
                )
        super()._write_changes(proxy, changed)

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
        """Replace all metadata rows. Used by the revision system to replay patches.

        The records of databases stored in read-only extensions are left as they are, with
        their project-local state."""
        from bw2data.backends import sqlite3_lci_db
        from bw2data.backends.schema import DatabaseRecord

        in_extensions = set(sqlite3_lci_db.extension_databases())
        query = DatabaseRecord.delete()
        if in_extensions:
            query = query.where(DatabaseRecord.name.not_in(in_extensions))
        query.execute()
        for name, meta in new_data.items():
            if name in in_extensions:
                continue
            meta = dict(meta)
            known = {k: meta.pop(k) for k in list(meta) if k in _KNOWN_FIELDS}
            DatabaseRecord.replace(name=name, extra=meta or None, **known).execute()
        self._proxies.clear()


def _atomic(database):
    """``database.atomic()``, or nothing if a transaction is open already: one started with
    ``begin()`` (as ``Database.write`` does) is not known to peewee's ``atomic()``, which would
    try to start another."""
    from contextlib import nullcontext

    if database.in_transaction() or getattr(database, "transaction_token", lambda: None)():
        return nullcontext()
    return database.atomic()


class CalculationSetupProxy(MutableMapping):
    """Dict-like view of one calculation setup, as returned by ``calculation_setups[name]``.

    Changes, also in place (``setup["inv"].append(...)``), are kept in memory until
    :meth:`save` is called. The registry hands out one proxy per name, so pending changes
    survive between accesses."""

    def __init__(self, registry, name: str, value: dict):
        self._registry = registry
        self._name = name
        self._load(value)

    def _load(self, value: dict) -> None:
        self._value = value
        self._saved = copy.deepcopy(value)

    def _has_changes(self) -> bool:
        return self._value != self._saved

    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        self._value[key] = value

    def __delitem__(self, key):
        del self._value[key]

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

    def save(self) -> None:
        """Write the setup to SQLite, if it changed."""
        if self._has_changes():
            self._registry._write(self._name, self._value)
            self._load(self._registry._read(self._name))

    def __copy__(self):
        return dict(self._value)

    def __deepcopy__(self, memo):
        return copy.deepcopy(self._value, memo)

    def __eq__(self, other):
        if isinstance(other, CalculationSetupProxy):
            other = other._value
        return self._value == other

    def __repr__(self):
        return repr(self._value)


class _CalculationSetups(MutableMapping):
    """Dict-like registry of calculation setups, stored in the ``calculationsetup`` and
    ``calculationsetupdemand`` tables of ``lci/databases.db`` (see
    ``bw2data.backends.schema.CalculationSetupRecord``). Before, they were pickled to a file
    in the project directory, which schema migration 8 moves into SQLite.

    A setup is a dict, usually with:

    * ``inv``: a list of functional units, each a dict ``{node: amount}``. A node is given by
      its key ``(database, code)`` or by its id, and is read back the same way. Nodes are
      stored by id: renaming a node or its database changes the key which is read back, and
      deleting a node or a database removes it from every setup (and functional units which
      are then empty). A key must be the key of an existing node.
    * ``ia``: stored as it is given.

    Other keys are stored as they are, as tagged JSON (see ``bw2data.codec``).

    ``registry[name] = setup`` and ``del registry[name]`` are saved right away;
    ``registry[name]`` returns a :class:`CalculationSetupProxy`, whose changes are saved with
    its ``save()``. Names are strings."""

    _kind = ""
    _registry_name = "calculation_setups"

    def __init__(self):
        self._proxies = {}

    @staticmethod
    def _models():
        from bw2data.backends.schema import CalculationSetupDemand, CalculationSetupRecord

        return CalculationSetupRecord, CalculationSetupDemand

    def _record(self, name):
        record, _ = self._models()
        if not isinstance(name, str):
            return None
        return record.get_or_none((record.kind == self._kind) & (record.name == name))

    # Reading

    def _read(self, name: str) -> dict:
        row = self._record(name)
        if row is None:
            raise KeyError(name)
        return self._as_dict(row)

    def _as_dict(self, row) -> dict:
        from bw2data.backends.schema import ActivityDataset

        _, demand = self._models()
        value = dict(row.extra or {})
        if row.ia is not None:
            value["ia"] = row.ia
        if row.units is not None:
            rows = list(
                demand.select(
                    demand.unit, demand.node, demand.amount, demand.key_database, demand.key_code
                )
                .where(demand.setup == row.id)
                # In the order they were written
                .order_by(SQL("rowid"))
                .tuples()
            )
            by_key = [node for _, node, _, database, _ in rows if database is not None]
            keys = {}
            for start in range(0, len(by_key), _CHUNK):
                query = ActivityDataset.select(
                    ActivityDataset.id, ActivityDataset.database, ActivityDataset.code
                ).where(ActivityDataset.id.in_(by_key[start : start + _CHUNK]))
                keys.update((id_, (database, code)) for id_, database, code in query.tuples())
            units = [{} for _ in range(row.units)]
            for unit, node, amount, database, code in rows:
                if database is None:
                    units[unit][node] = amount
                else:
                    # The key as it is now; the one it was written with if the node is not
                    # there (e.g. in an extension which is not attached)
                    units[unit][keys.get(node, (database, code))] = amount
            value["inv"] = units
        return value

    def __getitem__(self, name):
        proxy = self._proxies.get(name)
        if proxy is not None and proxy._has_changes():
            return proxy
        try:
            value = self._read(name)
        except KeyError:
            self._proxies.pop(name, None)
            raise
        # No pending changes, so read again in case it was changed elsewhere
        if proxy is None:
            proxy = self._proxies[name] = CalculationSetupProxy(self, name, value)
        else:
            proxy._load(value)
        return proxy

    def __iter__(self):
        record, _ = self._models()
        # Read completely first, so that setups can be deleted while iterating
        query = record.select(record.name).where(record.kind == self._kind).order_by(record.name)
        return iter([name for (name,) in query.tuples()])

    def __len__(self):
        record, _ = self._models()
        return record.select().where(record.kind == self._kind).count()

    def __contains__(self, name):
        return self._record(name) is not None

    # Writing

    def __setitem__(self, name, value):
        self._write(name, value)
        self._proxies.pop(name, None)

    def __delitem__(self, name):
        row = self._record(name)
        if row is None:
            raise KeyError(name)
        record, demand = self._models()
        with _atomic(record._meta.database):
            demand.delete().where(demand.setup == row.id).execute()
            record.delete().where(record.id == row.id).execute()
        self._proxies.pop(name, None)

    def _write(self, name, value) -> None:
        """Store ``value`` as the setup ``name``, replacing it if it exists. Raises
        ``TypeError`` or ``ValueError`` for a setup which can't be stored, and
        ``UnknownObject`` for a key of a node which doesn't exist; nothing is changed then."""
        from bw2data.snowflake_ids import new_snowflake_id

        if not isinstance(name, str):
            raise TypeError(f"Calculation setup names must be strings, not {name!r}")
        if isinstance(value, CalculationSetupProxy):
            value = value._value
        if not isinstance(value, Mapping):
            raise TypeError(f"A calculation setup must be a dict, not {type(value).__name__}")
        value = dict(value)
        inv = value.pop("inv", None)
        ia = value.pop("ia", None)
        rows = None if inv is None else _setup_demand_rows(name, inv)
        if rows and len(rows) > MAX_SETUP_NODES:
            raise ValueError(
                f"Calculation setup {name!r} has {len(rows)} nodes in its functional units; "
                f"at most {MAX_SETUP_NODES} can be stored. Split it into several setups."
            )
        record, demand = self._models()
        with _atomic(record._meta.database):
            row = self._record(name)
            setup_id = row.id if row is not None else new_snowflake_id()
            fields = {
                "units": None if inv is None else len(inv),
                "ia": ia,
                "extra": value or None,
            }
            if row is None:
                record.insert(id=setup_id, kind=self._kind, name=name, **fields).execute()
            else:
                record.update(**fields).where(record.id == setup_id).execute()
                demand.delete().where(demand.setup == setup_id).execute()
            if rows:
                demand.insert_many([dict(row, setup=setup_id) for row in rows]).execute()

    # Compatibility

    def flush(self, signal: bool = True):
        """Deprecated. Saves the pending changes of all setups; save one with
        ``registry[name].save()`` instead."""
        warnings.warn(
            f"`{self._registry_name}.flush()` is deprecated; save changes to a setup with "
            f"`{self._registry_name}[name].save()` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        for proxy in list(self._proxies.values()):
            proxy.save()

    @property
    def data(self) -> dict:
        """All saved setups, as plain dicts."""
        record, _ = self._models()
        query = record.select().where(record.kind == self._kind).order_by(record.name)
        return {row.name: self._as_dict(row) for row in query}

    @property
    def list(self):
        """Sorted list of names."""
        return sorted(self)

    def random(self):
        """Return a random name, or ``None`` if empty."""
        import random as _random

        names = list(self)
        return _random.choice(names) if names else None

    def __repr__(self):
        return f"{type(self).__name__} with {len(self)} setup(s): {self.list}"


def _setup_demand_rows(name: str, inv) -> list[dict]:
    """``CalculationSetupDemand`` rows (without ``setup``) for the functional units ``inv``.
    Keys are looked up as node ids; raises ``UnknownObject`` for a key of no node."""
    from bw2data.backends.schema import ActivityDataset
    from bw2data.errors import UnknownObject

    if not isinstance(inv, (list, tuple)):
        raise TypeError(f"`inv` of calculation setup {name!r} must be a list of dicts")
    keys = set()
    for unit in inv:
        if not isinstance(unit, Mapping):
            raise TypeError(f"`inv` of calculation setup {name!r} must be a list of dicts")
        for node in unit:
            if isinstance(node, (tuple, list)):
                if len(node) != 2 or not all(isinstance(part, str) for part in node):
                    raise ValueError(f"Not a node key in calculation setup {name!r}: {node!r}")
                keys.add(tuple(node))
            elif not isinstance(node, numbers.Integral) or isinstance(node, bool):
                raise TypeError(
                    f"Nodes in calculation setup {name!r} must be given by key or id, not "
                    f"{node!r}"
                )
    ids = {}
    for database in {database for database, _ in keys}:
        codes = [code for db, code in keys if db == database]
        for start in range(0, len(codes), _CHUNK):
            query = ActivityDataset.select(ActivityDataset.code, ActivityDataset.id).where(
                (ActivityDataset.database == database)
                & ActivityDataset.code.in_(codes[start : start + _CHUNK])
            )
            ids.update(((database, code), id_) for code, id_ in query.tuples())
    missing = sorted(keys.difference(ids))
    if missing:
        raise UnknownObject(
            f"Calculation setup {name!r} refers to nodes which don't exist: "
            + ", ".join(map(repr, missing[:5]))
        )
    rows = []
    for position, unit in enumerate(inv):
        seen = set()
        for node, amount in unit.items():
            key = tuple(node) if isinstance(node, (tuple, list)) else None
            # ``int``: also numpy integers
            node_id = ids[key] if key is not None else int(node)
            if node_id in seen:
                raise ValueError(
                    f"Functional unit {position} of calculation setup {name!r} has node "
                    f"{node_id} twice (by key and by id)"
                )
            seen.add(node_id)
            if hasattr(amount, "item") and not isinstance(amount, (int, float)):
                # E.g. a numpy scalar, which SQLite can't store
                amount = amount.item()
            if amount is None or (isinstance(amount, float) and math.isnan(amount)):
                # SQLite stores NaN as ``NULL``
                raise ValueError(
                    f"Functional unit {position} of calculation setup {name!r} has no "
                    f"amount for node {node!r}: {amount!r}"
                )
            rows.append(
                {
                    "unit": position,
                    "node": node_id,
                    "amount": amount,
                    "key_database": key[0] if key else None,
                    "key_code": key[1] if key else None,
                }
            )
    return rows


def remove_nodes_from_calculation_setups(nodes, message: str) -> None:
    """Remove the nodes ``nodes`` (activity ids, or a query selecting them) from every
    calculation setup, static and dynamic, and functional units which are then empty.
    ``message`` is logged as a warning for each setup which changes, with ``%s`` for its
    name."""
    from bw2data.backends.schema import CalculationSetupDemand as demand
    from bw2data.backends.schema import CalculationSetupRecord as record
    from bw2data.logs import stdout_feedback_logger

    if not isinstance(nodes, (list, tuple, set, frozenset)) and not hasattr(nodes, "sql"):
        nodes = list(nodes)
    with _atomic(record._meta.database):
        affected = list(
            record.select(record.id, record.kind, record.name)
            .where(
                record.id.in_(demand.select(demand.setup).where(demand.node.in_(nodes)).distinct())
            )
            .tuples()
        )
        if not affected:
            return
        demand.delete().where(demand.node.in_(nodes)).execute()
        for setup_id, kind, name in affected:
            stdout_feedback_logger.warning(message, name)
            units = [
                unit
                for (unit,) in demand.select(demand.unit)
                .where(demand.setup == setup_id)
                .distinct()
                .order_by(demand.unit)
                .tuples()
            ]
            # In increasing order, so a unit never moves onto one which is still there
            for position, unit in enumerate(units):
                if position != unit:
                    demand.update(unit=position).where(
                        (demand.setup == setup_id) & (demand.unit == unit)
                    ).execute()
            record.update(units=len(units)).where(record.id == setup_id).execute()
            registry = calculation_setups if kind == "static" else dynamic_calculation_setups
            registry._proxies.pop(name, None)


def move_database_in_calculation_setups(old: str, new: str) -> None:
    """Point the nodes of database ``old`` in calculation setups at the nodes with the same
    code in database ``new``, and their keys at ``new``. For ``Database.rename``, which
    writes the database again under the new name (with new ids) before it deletes the old
    one. Nodes without such a node are left as they are. Both databases are in the project's
    own file."""
    from bw2data.backends.schema import CalculationSetupDemand as demand
    from bw2data.backends.schema import CalculationSetupRecord as record

    database = record._meta.database
    with _atomic(database):
        database.execute_sql(
            'UPDATE main."calculationsetupdemand" SET '
            '"node" = (SELECT n."id" FROM main."activitydataset" AS n '
            'JOIN main."activitydataset" AS o ON n."code" = o."code" '
            'WHERE o."id" = "calculationsetupdemand"."node" AND n."database" = ?), '
            '"key_database" = CASE WHEN "key_database" IS NULL THEN NULL ELSE ? END '
            'WHERE "node" IN (SELECT o."id" FROM main."activitydataset" AS o '
            'JOIN main."activitydataset" AS n ON n."code" = o."code" '
            'WHERE o."database" = ? AND n."database" = ?)',
            (new, new, old, new),
        )
    for registry in (calculation_setups, dynamic_calculation_setups):
        registry._proxies.clear()


class CalculationSetups(_CalculationSetups):
    """Calculation setups: ``inv`` is a list of functional units, e.g.
    ``[{(key): amount}, {(key): amount}]``, and ``ia`` a list of LCIA methods, e.g.
    ``[(method), (method)]``. See ``_CalculationSetups``."""

    _kind = "static"
    _registry_name = "calculation_setups"


class DynamicCalculationSetups(_CalculationSetups):
    """Dynamic calculation setups: ``inv`` as for ``CalculationSetups``; ``ia`` is a list of
    dicts from worst-case LCIA methods to the dynamic LCIA methods, e.g.
    ``[{dLCIA_method_1_worstcase: dLCIA_method_1, dLCIA_method_2_worstcase: dLCIA_method_2}]``.
    See ``_CalculationSetups``."""

    _kind = "dynamic"
    _registry_name = "dynamic_calculation_setups"


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
        if not isinstance(name, tuple) or not name:
            return None
        try:
            # Names are dictionary keys here and JSON lists in the table
            hash(name)
            json.dumps(name)
        except (TypeError, ValueError):
            return None
        return name

    def _order(self):
        return (self._model().id,)

    def _where(self, key):
        # Peewee would otherwise compare with a row value of the tuple's elements
        field = self._model().name
        return field == Value(key, converter=field.db_value, unpack=False)

    def __setitem__(self, name, value):
        key = self._key(name)
        if key is None:
            raise TypeError(
                f"{self._what} names must be non-empty tuples of values which can be stored "
                f"as JSON (normally strings), not {name!r}"
            )
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
                if fields.get("abbreviation") is None:
                    # Set when the object was registered, and needed to find its files:
                    # ``Method(name).metadata = {...}`` must not remove it
                    del fields["abbreviation"]
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


def _forget_database_proxies(sender, **kwargs):
    """Attaching or detaching a read-only extension adds or removes databases. Proxies with
    unsaved changes are otherwise kept as they are, also for a database which is gone. Those of
    the databases which are still there are kept, with their unsaved changes."""
    for name in list(databases._proxies):
        if name not in databases:
            del databases._proxies[name]


on_extension_attached.connect(_forget_database_proxies)
on_extension_detached.connect(_forget_database_proxies)
