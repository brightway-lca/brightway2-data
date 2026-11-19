import copy
import datetime
import warnings
from collections.abc import MutableMapping

from bw2data.serialization import CompoundJSONDict, PickledDict
from bw2data.signals import on_database_delete, on_database_metadata_change


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


class GeoMapping(PickledDict):
    """A dictionary that maps location codes to integers. Needed because parameter arrays have integer ``geo`` fields.

    File data is stored in ``geomapping.pickle``.

    This dictionary does not support setting items directly; instead, use the ``add`` method to add multiple keys.
    """

    filename = "geomapping.pickle"

    def __init__(self, *args, **kwargs):
        super(GeoMapping, self).__init__(*args, **kwargs)
        # At a minimum, "GLO" should always be present
        if "GLO" not in self:
            self.add(["GLO"])

    def add(self, keys):
        """Add a set of keys. These keys can already be in the mapping; only new keys will be added.

        Args:
            * *keys* (list): The keys to add.

        """
        index = max(self.data.values()) if self.data else 0
        for i, key in enumerate(keys):
            if key not in self.data:
                self.data[key] = index + i + 1
        self.flush()

    def delete(self, keys):
        """Delete a set of keys.

        Args:
            *keys* (list): The keys to delete.

        """
        for key in keys:
            del self.data[key]
        self.flush()

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __str__(self):
        return "Mapping from databases and methods to parameter indices."

    def __len__(self):
        return len(self.data)


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
        projects, unless ``signal`` is ``False``."""
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

        The signal only feeds revision generation, which is skipped on unsourced projects, so
        don't pay for two full-table snapshots per save in that case (see #299)."""
        from bw2data.project import projects

        return projects.dataset.is_sourced

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
