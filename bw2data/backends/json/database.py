# -*- coding: utf-8 -*-
from ... import config, mapping, geomapping, databases
from ...units import normalize_units
from ..base import LCIBackend
from .sync_json_dict import SynchronousJSONDict
import os


class JSONDatabase(LCIBackend):
    """
    A data store for LCI databases. Stores each dataset in a separate file, serialized to JSON.

    Instead of loading all the data at once, ``.load()`` creates a ``SynchronousJSONDict``, which loads values on demand, and saves changes as they are made. In order to make sure that changes are saved correctly, each dataset is returned as a ``frozendict``, which doesn't allow modifications. Any modifications must be done by creating a new dictionary, e.g.:

    .. code-block:: python

        >>> my_db = JSONDatabase("some database")
        >>> my_ds = my_db["some key"]
        >>> my_ds["new key"] = "new value"
        AttributeError: A frozendict cannot be modified
        >> my_new_ds = dict(my_ds)  # Create new object for modifications
        >> my_new_ds["new key"] = "new value"
        >> my_db["some key"] = my_new_ds  # New data saved to disk

    Use this backend by setting ``"backend": "json"`` in the database metadata. This is done automatically if you call ``.register()`` from this class.
    """

    def filepath_intermediate(self):
        return os.path.join(
            config.dir,
            u"intermediate",
            self.filename
        )

    def load(self, *args, **kwargs):
        """Instantiate ``SynchronousJSONDict`` for this database."""
        self.assert_registered()
        return SynchronousJSONDict(self.filepath_intermediate(), self.name)

    def register(self, *args, **kwargs):
        """Register a database with the metadata store, using the correct value for ``backend``, and creates database directory."""
        kwargs[u"backend"] = u"json"
        super(JSONDatabase, self).register(*args, **kwargs)
        if not os.path.exists(self.filepath_intermediate()):
            os.mkdir(self.filepath_intermediate())

    def write(self, data):
        """Serialize data to disk. Most of the time, this data has already been saved to disk, so this is a no-op. The only exception is if ``data`` is a new database, instead of the result of a previous ``.load()`` call.

        Normalizes units when found.

        Args:
            * *data* (dict): Inventory data

        """
        self.assert_registered()

        databases[self.name]["number"] = len(data)
        databases.flush()

        mapping.add(data.keys())
        for ds in data.values():
            if u'unit' in ds:
                ds[u"unit"] = normalize_units(ds[u"unit"])
        geomapping.add({x[u"location"] for x in data.values() if
                       x.get(u"location", False)})

        if isinstance(data, SynchronousJSONDict) and \
                data.dirpath == self.filepath_intermediate():
            # SynchronousJSONDict automatically syncs changes; no-op
            pass
        else:
            new_dict = self.load()
            for key, value in data.iteritems():
                new_dict[key] = value
