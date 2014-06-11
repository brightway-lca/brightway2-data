# -*- coding: utf-8 -*-
from ... import config, mapping, geomapping
from ...units import normalize_units
from ..base import LCIBackend
from .sync_json_dict import SynchronousJSONDict
import os


class JSONDatabase(LCIBackend):
    """
    A data store for LCI databases.
    """

    def filepath_intermediate(self):
        return os.path.join(
            config.dir,
            u"intermediate",
            self.filename
        )

    def load(self, *args, **kwargs):
        """Instantiate SynchronousJSONDict for this database."""
        self.assert_registered()
        return SynchronousJSONDict(self.filepath_intermediate(), self.name)

    def register(self, *args, **kwargs):
        """Register a database with the metadata store, and creates database directory."""
        super(JSONDatabase, self).register(*args, **kwargs)
        if not os.path.exists(self.filepath_intermediate()):
            os.mkdir(self.filepath_intermediate())

    def write(self, data):
        """Serialize data to disk.

        Normalizes units when found.

        Args:
            * *data* (dict): Inventory data

        """
        self.assert_registered()

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
