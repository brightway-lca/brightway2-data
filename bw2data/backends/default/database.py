# -*- coding: utf-8 -*-
from ... import databases, config, mapping, geomapping
from ...errors import MissingIntermediateData
from ...units import normalize_units
from ...utils import natural_sort, safe_filename
from ...validate import db_validator
import datetime
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle
from ..base import LCIBackend


class SingleFileDatabase(LCIBackend):
    """
    A data store for LCI databases where each database is stored as a ``pickle`` file.

    Databases are automatically versioned. See below for reversion, etc. methods

    Args:
        *name* (str): Name of the database to manage.

    """
    validator = db_validator

    @property
    def filename(self):
        return self.filename_for_version()

    def filename_for_version(self, version=None):
        """Filename for given version; Default is current.

        Returns:
            Filename (not path)

        """
        return u"%s.%i" % (
            safe_filename(self.name),
            version or self.version
        )

    def filepath_intermediate(self, version=None):
        return os.path.join(
            config.dir,
            u"intermediate",
            self.filename_for_version(version) + u".pickle"
        )

    def load(self, version=None):
        """Load the intermediate data for this database.

        Can also load previous versions of this database's intermediate data.

        Args:
            * *version* (int): Version of the database to load. Default is *None*, for the latest version.

        Returns:
            The intermediate data, a dictionary.

        """
        if version is not None:
            try:
                version = int(version)
            except:
                raise ValueError("Version number must be an integer")
        self.assert_registered()
        if version is None and config.p.get(u"use_cache", False) and \
                self.name in config.cache:
            return config.cache[self.name]
        try:
            data = pickle.load(open(self.filepath_intermediate(version), "rb"))
            if version is None and config.p.get(u"use_cache", False):
                config.cache[self.name] = data
            return data
        except (OSError, IOError):
            raise MissingIntermediateData("This version (%i) not found" % version)

    def make_latest_version(self):
        """Make the current version the latest version.

        Requires loading data because a new intermediate data file is created."""
        data = self.load()
        databases[self.name][u"version"] = self.versions()[-1][0]
        self.write(data)

    def register(self, **kwargs):
        """Register a database with the metadata store.

        Databases must be registered before data can be written.

        `kwargs` can include `depends` and `version`.

        """
        kwargs.update(
            version=kwargs.get(u'version', None) or 0
        )
        super(SingleFileDatabase, self).register(**kwargs)

    def revert(self, version):
        """Return data to a previous state.

        .. warning:: Reverted changes can be overwritten.

        Args:
            * *version* (int): Number of the version to revert to.

        """
        assert version in [x[0] for x in self.versions()], "Version not found"
        self.backup()
        databases[self.name][u"version"] = version
        if config.p.get(u"use_cache", False) and self.name in config.cache:
            config.cache[self.name] = self.load(version)
        self.process(version)

    @property
    def version(self):
        """The current version number (integer) of this database.

        Returns:
            Version number

        """
        return databases.version(self.name)

    def versions(self):
        """Get a list of available versions of this database.

        Returns:
            List of (version, datetime created) tuples.

        """
        directory = os.path.join(config.dir, u"intermediate")
        files = natural_sort(filter(
            lambda x: ".".join(x.split(".")[:-2]) == safe_filename(self.name),
            os.listdir(directory)))
        return sorted([(int(name.split(".")[-2]),
            datetime.datetime.fromtimestamp(os.stat(os.path.join(
            config.dir, directory, name)).st_mtime)) for name in files])

    def write(self, data):
        """Serialize data to disk.

        Normalizes units when found.

        Args:
            * *data* (dict): Inventory data

        """
        self.assert_registered()
        databases.increment_version(self.name, len(data))

        mapping.add(data.keys())
        for ds in data.values():
            if u'unit' in ds:
                ds[u"unit"] = normalize_units(ds[u"unit"])
        geomapping.add({x[u"location"] for x in data.values() if
                       x.get(u"location", False)})

        if config.p.get(u"use_cache", False) and self.name in config.cache:
            config.cache[self.name] = data
        with open(self.filepath_intermediate(), "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
