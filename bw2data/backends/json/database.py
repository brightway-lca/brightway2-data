# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ... import config, mapping, geomapping, databases, projects, preferences
from .proxies import Activity
from ..base import LCIBackend
from .sync_json_dict import SynchronousJSONDict
import os


class JSONDatabase(LCIBackend):
    """
    A data store for LCI databases. Stores each dataset in a separate file, serialized to JSON.

    Instead of loading all the data at once, ``.load()`` creates a :class:`.SynchronousJSONDict`, which loads values on demand.

    Use this backend by setting ``"backend":"json"`` in the database metadata. This is done automatically if you call ``.register()`` from this class.
    """
    backend = u"json"

    def filepath_intermediate(self):
        return os.path.join(
            projects.dir,
            u"intermediate",
            self.filename
        )

    def load(self, as_dict=False, *args, **kwargs):
        """Instantiate :class:`.SynchronousJSONDict` for this database."""
        self.register()

        if config.p.get("use_cache"):
            try:
                dct = config.cache[self.name]
            except KeyError:
                dct = SynchronousJSONDict(self.filepath_intermediate(),
                                          self.name)
                config.cache[self.name] = dct
        else:
            dct = SynchronousJSONDict(self.filepath_intermediate(), self.name)

        if as_dict:
            return {key: dict(value) for key, value in dct.items()}
        else:
            return dct

    def __iter__(self):
        json_dict = self.load()
        for key in json_dict:
            yield Activity(key, json_dict[key])

    def get(self, code):
        """Get Activity proxy for this dataset"""
        key = (self.name, code)
        data = self.load()[key]
        return Activity(key, data)

    def register(self, **kwargs):
        """Register a database with the metadata store, using the correct value for ``backend``, and creates database directory."""
        super(JSONDatabase, self).register(**kwargs)
        if not os.path.exists(self.filepath_intermediate()):
            os.mkdir(self.filepath_intermediate())

    def write(self, data, process=True):
        """Serialize data to disk. Most of the time, this data has already been saved to disk, so this is a no-op. The only exception is if ``data`` is a new database dictionary.

        Normalizes units when found.

        Args:
            * *data* (dict): Inventory data

        """
        self.register()
        databases[self.name]["number"] = len(data)
        databases.flush()

        mapping.add(data.keys())
        geomapping.add({x[u"location"] for x in data.values() if
                       x.get(u"location", False)})

        if preferences.get('allow incomplete imports'):
            mapping.add({exc['input'] for ds in data.values() for exc in ds.get('exchanges', [])})
            mapping.add({exc['output'] for ds in data.values() for exc in ds.get('exchanges', [])})

        if isinstance(data, SynchronousJSONDict) and \
                data.dirpath == self.filepath_intermediate():
            # SynchronousJSONDict automatically syncs changes; no-op
            pass
        else:
            new_dict = self.load()
            for key, value in data.items():
                new_dict[key] = value
        if process:
            self.process()
