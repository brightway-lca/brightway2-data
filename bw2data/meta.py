import os
import shutil
import tempfile
import warnings
from collections.abc import Iterable
from pathlib import Path
from threading import ThreadError
import itertools
import pickle
import json

import appdirs
import wrapt
from bw_processing import safe_filename
from fasteners import InterProcessLock
from peewee import BooleanField, DoesNotExist, Model, TextField, chunked

from . import config
from .filesystem import create_dir
from .sqlite import PickleField, SubstitutableDatabase
from .utils import maybe_path
from .database import DatabaseChooser
from .backends import SQLiteBackend

import datetime

from .serialization import CompoundJSONDict, PickledDict, SerializedDict


class GeoMapping:
    """A dictionary that maps location codes to integers. Needed because parameter arrays have integer ``geo`` fields.

    File data is stored in SQLite database.

    This dictionary does not support setting items directly; instead, use the ``add`` method to add multiple keys."""
    def flush(self):
        warnings.warn("This function is obsolete and no longer necessary", DeprecationWarning)
        pass

    def add(self, keys):
        from .backends import Location

        warnings.warn("Use `Location.add_many` instead", DeprecationWarning)
        Location.add_many(keys)

    def delete(self, keys):
        from .backends import Location

        warnings.warn("Use `Location.delete_many` instead", DeprecationWarning)
        Location.delete_many(keys)

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __str__(self):
        return "Obsolete mapping from database and method locations to indices"

    def __len__(self):
        from .backends import Location

        warnings.warn("Use `len(Location)` instead", DeprecationWarning)
        return Location.select().count()

    def migrate_to_sqlite(self, reset=True):
        from . import projects
        from .backends import Location

        data = pickle.load(open(projects.dir / "geomapping.pickle", "rb"))
        if reset:
            Location.delete().execute()
        Location.add_many(data.keys())


class Databases:
    def increment_version(self, database, number=None):
        warnings.warn("This function is obsolete and has no effect", DeprecationWarning)

    def version(self, database):
        warnings.warn("This function is obsolete and always returns -1", DeprecationWarning)
        return -1

    def set_modified(self, database):
        warnings.warn("Use `Database.set_modified()` instead", DeprecationWarning)

        from .database import DatabaseChooser
        DatabaseChooser(database).set_modified()

    def set_dirty(self, database):
        warnings.warn("Use `Database.set_dirty()` instead", DeprecationWarning)

        from .database import DatabaseChooser
        DatabaseChooser(database).set_dirty()

    def clean(self):
        from .database import DatabaseChooser
        from .backends import SQLiteBackend

        for db in SQLiteBackend.select().where(SQLiteBackend.dirty == True):
            DatabaseChooser(db.name).process()

    def __getitem__(self, name):
        warnings.warn("Use `Database.metadata` instead", DeprecationWarning)
        return DatabaseChooser(name).metadata

    def __delitem__(self, name):
        warnings.warn("Use `Database.delete()` instead", DeprecationWarning)
        DatabaseChooser(name).delete()

    def __contains__(self, name):
        return bool(SQLiteBackend.select().where(SQLiteBackend.name == name).count())

    def __str__(self):
        return "Obsolete database metadata store. Use `Database.metadata` instead."

    def __len__(self):
        return SQLiteBackend.select().count()

    def __iter__(self):
        return (obj.name for obj in SQLiteBackend.select())

    def migrate_to_sqlite(self):
        from . import projects

        objs = json.load(open(projects.dir / "databases.json"))

        for name, metadata in objs.items():
            if "processed" in metadata:
                del metadata['processed']
            backend = metadata.pop('backend', 'sqlite')

            # A bit of a hack, but this will also work for IOTables
            SQLiteBackend.create(name=name, dirty=True, backend=backend, metadata=metadata)


class CalculationSetups(PickledDict):
    """A dictionary for calculation setups.

    Keys:
    * `inv`: List of functional units, e.g. ``[{(key): amount}, {(key): amount}]``
    * `ia`: List of LCIA methods, e.g. ``[(method), (method)]``.

    """

    filename = "setups.pickle"


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


# Compatibility
