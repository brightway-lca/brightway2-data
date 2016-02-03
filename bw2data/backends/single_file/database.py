# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ... import databases, config, mapping, geomapping, projects, preferences
from ...errors import MissingIntermediateData
from ...fatomic import open as atomic_open
from ...project import writable_project
from ...utils import natural_sort, safe_filename
from ...validate import db_validator
from .proxies import Activity
from ..base import LCIBackend
import datetime
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle


class SingleFileDatabase(LCIBackend):
    """
    A data store for LCI databases where each database is stored as a ``pickle`` file.

    Databases are automatically versioned. See below for reversion, etc. methods

    Args:
        *name* (str): Name of the database to manage.

    """
    validator = db_validator
    backend = u"singlefile"

    def __iter__(self):
        for k, v in self.load().items():
            yield Activity(k, v)

    def get(self, code):
        """Get Activity proxy for this dataset"""
        key = (self.name, code)
        return Activity(key, self.load()[key])

    @property
    def filename(self):
        return self.filename_for_version()

    def filename_for_version(self, version=None):
        """Filename for given version; Default is current version.

        Returns:
            Filename (not path)

        """
        return u"%s.%i" % (
            safe_filename(self.name),
            version or self.version
        )

    def filepath_intermediate(self, version=None):
        return os.path.join(
            projects.dir,
            u"intermediate",
            self.filename_for_version(version) + u".pickle"
        )

    def load(self, version=None, **kwargs):
        """Load the intermediate data for this database.

        Can also load previous versions of this database's intermediate data.

        Args:
            * *version* (int): Version of the database to load. Default ``version`` is the latest version.

        Returns:
            The intermediate data, a dictionary.

        """
        if version is not None:
            try:
                version = int(version)
            except:
                raise ValueError("Version number must be an integer")
        self.register()

        try:
            if (version is None
                and config.p.get("use_cache", False)
                and self.name in config.cache):
                return config.cache[self.name]
            else:
                data = pickle.load(open(self.filepath_intermediate(version), "rb"))
                if (version is None
                    and config.p.get("use_cache", False)):
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

        """
        kwargs.update(version=kwargs.get(u'version', None) or 0)
        super(SingleFileDatabase, self).register(**kwargs)

    def revert(self, version):
        """Return data to a previous state.

        .. warning:: Reverting can lead to data loss, e.g. if you revert from version 3 to version 1, and then save your database, you will overwrite version 2. Use :meth:`.make_latest_version` before saving, which will set the current version to 4.

        Args:
            * *version* (int): Number of the version to revert to.

        """
        assert version in [x[0] for x in self.versions()], "Version not found"
        self.backup()
        databases[self.name][u"version"] = version
        if (config.p.get(u"use_cache", False)
            and self.name in config.cache):
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
        directory = os.path.join(projects.dir, u"intermediate")
        files = natural_sort(filter(
            lambda x: ".".join(x.split(".")[:-2]) == safe_filename(self.name),
            os.listdir(directory)))
        return sorted([(int(name.split(".")[-2]),
            datetime.datetime.fromtimestamp(os.stat(os.path.join(
            projects.dir, directory, name)).st_mtime)) for name in files])

    @writable_project
    def write(self, data, process=True):
        """Serialize data to disk.

        Args:
            * *data* (dict): Inventory data

        """
        self.register()

        # Need to use iterator to reduce memory usage
        try:  # Py2
            itr = data.iteritems()
        except AttributeError:  # Py3
            itr = data.items()
        for key, obj in itr:
            obj['database'] = key[0]
            obj['code'] = key[1]

        if (config.p.get(u"use_cache", False)
            and self.name in config.cache):
            config.cache[self.name] = data

        databases.increment_version(self.name, len(data))

        mapping.add(data.keys())
        geomapping.add({x[u"location"] for x in data.values() if
                       x.get(u"location", False)})

        if preferences.get('allow incomplete imports'):
            mapping.add({exc['input'] for ds in data.values() for exc in ds.get('exchanges', [])})
            mapping.add({exc['output'] for ds in data.values() for exc in ds.get('exchanges', [])})

        with atomic_open(self.filepath_intermediate(), "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

        if process:
            self.process()
