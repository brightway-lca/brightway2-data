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


class DatabaseRecordProxy(MutableMapping):
    """Dict-like view of a single :class:`~bw2data.backends.schema.DatabaseRecord` row.

    Writes are immediately persisted to SQLite. Signals are NOT emitted by this
    class — callers that need to signal a change should call
    ``databases._emit(old_state)`` explicitly after mutating the proxy.
    """

    def __init__(self, row, parent):
        self._row = row
        self._parent = parent

    def _all_fields(self):
        """Return only the fields that have been explicitly set (non-NULL)."""
        d = {f: getattr(self._row, f) for f in _KNOWN_FIELDS if getattr(self._row, f) is not None}
        d.update(self._row.extra or {})
        return d

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
        suppress = self._parent._suppress_signals
        old = None if suppress else self._parent._as_dict()
        if key in _KNOWN_FIELDS:
            setattr(self._row, key, value)
        else:
            extra = dict(self._row.extra or {})
            extra[key] = value
            self._row.extra = extra
        self._row.save()
        if not suppress:
            self._parent._emit(old)

    def __delitem__(self, key):
        suppress = self._parent._suppress_signals
        old = None if suppress else self._parent._as_dict()
        if key in _KNOWN_FIELDS:
            setattr(self._row, key, None)
        else:
            extra = dict(self._row.extra or {})
            if key not in extra:
                raise KeyError(key)
            del extra[key]
            self._row.extra = extra
        self._row.save()
        if not suppress:
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
    """

    _save_signal = on_database_metadata_change

    def __init__(self):
        self._migrated = False
        self._suppress_signals = False

    # ------------------------------------------------------------------
    # MutableMapping interface
    # ------------------------------------------------------------------

    def __getitem__(self, name):
        self._ensure_migrated()
        from bw2data.backends.schema import DatabaseRecord
        from peewee import DoesNotExist

        try:
            return DatabaseRecordProxy(
                DatabaseRecord.get(DatabaseRecord.name == name), self
            )
        except DoesNotExist:
            raise KeyError(name)

    def __setitem__(self, name, value):
        self._ensure_migrated()
        from bw2data.backends.schema import DatabaseRecord

        old = self._as_dict()
        value = dict(value)  # copy to avoid mutating caller's dict
        known = {k: value.pop(k) for k in list(value) if k in _KNOWN_FIELDS}
        DatabaseRecord.replace(name=name, extra=value or None, **known).execute()
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
        """Increment the ``database`` version. Returns the new version."""
        from bw2data.backends.schema import DatabaseRecord

        row = DatabaseRecord.get(DatabaseRecord.name == database)
        row.version = (row.version or 0) + 1
        if number is not None:
            row.number = number
        row.save()
        return row.version

    def version(self, database):
        """Return the ``database`` version."""
        from bw2data.backends.schema import DatabaseRecord

        return DatabaseRecord.get(DatabaseRecord.name == database).version

    def set_modified(self, database):
        from bw2data.backends.schema import DatabaseRecord

        row = DatabaseRecord.get(DatabaseRecord.name == database)
        row.modified = datetime.datetime.now().isoformat()
        row.save()

    def set_dirty(self, database):
        from bw2data.backends.schema import DatabaseRecord

        row = DatabaseRecord.get(DatabaseRecord.name == database)
        row.modified = datetime.datetime.now().isoformat()
        if not row.dirty:
            row.dirty = True
        row.save()

    def clean(self):
        from bw2data import Database
        from bw2data.backends.schema import DatabaseRecord

        dirty = [
            r.name for r in DatabaseRecord.select().where(DatabaseRecord.dirty == True)
        ]
        if not dirty:
            return
        for name in dirty:
            Database(name).process()
        DatabaseRecord.update(dirty=None).where(DatabaseRecord.name << dirty).execute()

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
        """Deprecated no-op. Data is auto-persisted to SQLite and signals fire on each write."""
        warnings.warn(
            "Databases.flush() is deprecated; metadata is now auto-persisted to SQLite.",
            DeprecationWarning,
            stacklevel=2,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _emit(self, old):
        """Emit on_database_metadata_change with the given old state and current new state."""
        on_database_metadata_change.send(self, old=old, new=self._as_dict())

    def _as_dict(self):
        """Reconstruct a plain dict of all metadata (used for signal old/new payloads)."""
        return {name: dict(self[name]) for name in self}

    @property
    def data(self):
        """Return the full metadata as a plain dict. Used by the revision system."""
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
