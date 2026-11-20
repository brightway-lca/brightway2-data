import copy
import datetime
import pickle
import warnings
from collections.abc import Mapping, MutableMapping

from peewee import JOIN

from bw2data.serialization import CompoundJSONDict, PickledDict
from bw2data.signals import on_database_delete, on_database_metadata_change, should_emit


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


def _chunks(items, size=_CHUNK):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _write_transaction(model):
    """Transaction which takes the SQLite write lock immediately.

    A default (deferred) transaction which reads before writing can fail at once with
    "database is locked" when another process holds a read lock and also wants to write;
    taking the write lock first makes concurrent writers wait for each other instead."""
    return model._meta.database.atomic("IMMEDIATE")


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
        from bw2data.backends.schema import GeoMappingRecord, Geocollection

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
            names = sorted({key[0] for key in missing if isinstance(key, tuple)})
            if names:
                Geocollection.insert_many(
                    [{"name": name} for name in names]
                ).on_conflict_ignore().execute()
            collection_ids = self._collection_ids(names)
            rows = [
                {"geocollection": collection_ids.get(collection), "location": location}
                for collection, location in map(_split_geo_key, missing)
            ]
            for batch in _chunks(rows):
                GeoMappingRecord.insert_many(batch).on_conflict_ignore().execute()
            self._fetch(missing)

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
            names = sorted({key[0] for key in rows if isinstance(key, tuple)})
            if names:
                Geocollection.insert_many([{"name": name} for name in names]).execute()
            collection_ids = self._collection_ids(names)
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


def _row_as_dict(row) -> dict:
    """Metadata dict for a ``DatabaseRecord`` row: set (non-NULL) known fields plus ``extra``."""
    d = {f: getattr(row, f) for f in _KNOWN_FIELDS if getattr(row, f) is not None}
    d.update(row.extra or {})
    return d


class DatabaseRecordProxy(MutableMapping):
    """Dict-like view of a single :class:`~bw2data.backends.schema.DatabaseRecord` row.

    Changes are kept in memory until :meth:`save` is called, as with the old ``databases.json``
    dict and ``databases.flush()``. ``Databases`` hands out one proxy per database name, so
    ``Database(name).metadata`` and ``databases[name]`` share pending changes.

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
        return _row_as_dict(self._row)

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
        if key in _KNOWN_FIELDS:
            val = getattr(self._row, key)
            if val is None:
                raise KeyError(key)
            return val
        extra = self._row.extra or {}
        if key in extra:
            return extra[key]
        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in _KNOWN_FIELDS:
            setattr(self._row, key, value)
        else:
            extra = dict(self._row.extra or {})
            extra[key] = value
            self._row.extra = extra

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        if key in _KNOWN_FIELDS:
            setattr(self._row, key, None)
        else:
            extra = dict(self._row.extra)
            del extra[key]
            self._row.extra = extra or None

    def save(self, signal: bool = True):
        """Write pending changes for this database to SQLite.

        Only changed keys are written, so changes saved through other code since this proxy
        was loaded are kept. Sends ``on_database_metadata_change`` once on event-sourced
        projects or when signals are turned on with ``bw2data.signals.set_signalled()``, unless
        ``signal`` is ``False``."""
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
        if key in _KNOWN_FIELDS:
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


class Databases(MutableMapping):
    """Dict-like registry of database metadata, backed by the ``DatabaseRecord`` SQLite table
    in the per-project ``lci/databases.db``.

    Previously stored in ``databases.json``; the file is migrated automatically on first access.

    Adding, replacing or deleting a whole entry (``databases[name] = {...}``,
    ``del databases[name]``) is saved immediately. Changes to an entry
    (``databases[name]["key"] = value``) are saved by ``databases[name].save()``.
    """

    _save_signal = on_database_metadata_change

    def __init__(self):
        self._migrated = False
        # One proxy per database name, so pending changes survive between accesses
        self._proxies = {}

    # ------------------------------------------------------------------
    # MutableMapping interface
    # ------------------------------------------------------------------

    def __getitem__(self, name):
        self._ensure_migrated()
        from bw2data.backends.schema import DatabaseRecord
        from peewee import DoesNotExist

        proxy = self._proxies.get(name)
        if proxy is not None and proxy._has_changes():
            return proxy

        try:
            row = DatabaseRecord.get(DatabaseRecord.name == name)
        except DoesNotExist:
            self._proxies.pop(name, None)
            raise KeyError(name)

        # No pending changes, so refresh from SQLite in case it was changed elsewhere
        if proxy is None:
            proxy = self._proxies[name] = DatabaseRecordProxy(row, self)
        else:
            proxy._load(row)
        return proxy

    def __setitem__(self, name, value):
        self._ensure_migrated()
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

    def __iter__(self):
        self._ensure_migrated()
        from bw2data.backends.schema import DatabaseRecord

        return (row.name for row in DatabaseRecord.select(DatabaseRecord.name))

    def __len__(self):
        self._ensure_migrated()
        from bw2data.backends.schema import DatabaseRecord

        return DatabaseRecord.select().count()

    def __contains__(self, name):
        self._ensure_migrated()
        from bw2data.backends.schema import DatabaseRecord

        return DatabaseRecord.select().where(DatabaseRecord.name == name).exists()

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

    @property
    def list(self):
        """List the keys of the dictionary."""
        return sorted(self)

    def random(self):
        """Return a random database name, or ``None`` if empty."""
        import random as _random

        names = list(self)
        return _random.choice(names) if names else None

    # ------------------------------------------------------------------
    # Deprecated shims
    # ------------------------------------------------------------------

    def flush(self, signal: bool = True):
        """Deprecated. Saves pending metadata changes for all databases.

        Use ``Database(name).metadata.save()`` or ``databases[name].save()`` instead."""
        warnings.warn(
            "`databases.flush()` is deprecated; save a database's metadata with "
            "`Database(name).metadata.save()` or `databases[name].save()` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        pending = [proxy for proxy in self._proxies.values() if proxy._has_changes()]
        if not pending:
            return
        send = signal and self._should_signal()
        old = self._as_dict() if send else None
        for proxy in pending:
            self._write_changes(proxy, proxy._changed_keys())
        if send:
            self._emit(old)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_changes(self, proxy, changed):
        """Write the ``changed`` keys of ``proxy`` to SQLite, then reload it.

        Known fields are written column by column. Custom keys are merged into the stored
        ``extra`` value, so keys saved elsewhere since ``proxy`` was loaded are kept."""
        from bw2data.backends.schema import DatabaseRecord
        from bw2data.errors import UnknownObject

        row = proxy._row
        where = DatabaseRecord.name == row.name
        updates = {
            getattr(DatabaseRecord, key): getattr(row, key)
            for key in changed
            if key in _KNOWN_FIELDS
        }
        extra_changed = [key for key in changed if key not in _KNOWN_FIELDS]

        with DatabaseRecord._meta.database.atomic():
            if extra_changed:
                stored = dict(DatabaseRecord.select(DatabaseRecord.extra).where(where).scalar() or {})
                mine = row.extra or {}
                for key in extra_changed:
                    if key in mine:
                        stored[key] = mine[key]
                    else:
                        stored.pop(key, None)
                updates[DatabaseRecord.extra] = stored or None
            if not DatabaseRecord.update(updates).where(where).execute():
                raise UnknownObject(f"Database {row.name!r} is not registered")
            proxy._load(DatabaseRecord.get(where))

    def _should_signal(self) -> bool:
        """Whether a save should build ``old``/``new`` snapshots and emit
        ``on_database_metadata_change``.

        Each save costs two full-table snapshots, so only pay for them when the project is event
        sourced or a listener asked for signals with `bw2data.signals.set_signalled` (see #299)."""
        return should_emit()

    def _emit(self, old):
        """Emit on_database_metadata_change with the given old state and current new state."""
        on_database_metadata_change.send(self, old=old, new=self._as_dict())

    def _as_dict(self):
        """Plain dict of all saved metadata, read from SQLite (used for signal old/new payloads).

        Pending changes that haven't been saved are not included."""
        from bw2data.backends.schema import DatabaseRecord

        return {row.name: _row_as_dict(row) for row in DatabaseRecord.select()}

    @property
    def data(self):
        """Return the full saved metadata as a plain dict. Used by the revision system."""
        return self._as_dict()

    @data.setter
    def data(self, new_data):
        """Replace all metadata rows. Used by the revision system to replay patches."""
        from bw2data.backends.schema import DatabaseRecord

        DatabaseRecord.delete().execute()
        for name, meta in new_data.items():
            meta = dict(meta)
            known = {k: meta.pop(k) for k in list(meta) if k in _KNOWN_FIELDS}
            DatabaseRecord.replace(name=name, extra=meta or None, **known).execute()
        self._proxies.clear()

    def _ensure_migrated(self):
        if not self._migrated:
            self._migrated = True
            self._migrate_from_json()

    def _migrate_from_json(self):
        """One-time migration from ``databases.json`` to SQLite on first access."""
        from bw2data.project import projects
        from bw2data.backends.schema import DatabaseRecord
        import json

        json_path = projects.dir / "databases.json"
        if not json_path.exists():
            return
        if DatabaseRecord.select().count() > 0:
            return

        warnings.warn(
            f"Migrating {json_path} to SQLite (one-time operation). "
            "The original file is kept as a backup.",
            stacklevel=2,
        )
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)

        for name, meta in data.items():
            meta = dict(meta)
            known = {k: meta.pop(k) for k in list(meta) if k in _KNOWN_FIELDS}
            DatabaseRecord.replace(name=name, extra=meta or None, **known).execute()


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


class Methods(CompoundJSONDict):
    """A dictionary for method metadata. File data is saved in ``methods.json``."""

    filename = "methods.json"


class WeightingMeta(Methods):
    """A dictionary for weighting metadata. File data is saved in ``methods.json``."""

    filename = "weightings.json"


class NormalizationMeta(Methods):
    """A dictionary for normalization metadata. File data is saved in ``methods.json``."""

    filename = "normalizations.json"


class Preferences(PickledDict):
    """A dictionary of project-specific preferences."""

    filename = "preferences.pickle"

    def __init__(self, *args, **kwargs):
        super(Preferences, self).__init__(*args, **kwargs)

        # Default preferences
        if "use_cache" not in self:
            self["use_cache"] = True


databases = Databases()
geomapping = GeoMapping()
methods = Methods()
normalizations = NormalizationMeta()
preferences = Preferences()
weightings = WeightingMeta()
calculation_setups = CalculationSetups()
dynamic_calculation_setups = DynamicCalculationSetups()
