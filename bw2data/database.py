# -*- coding: utf-8 -*-
from . import databases, config, mapping, geomapping
from .errors import MissingIntermediateData
from .query import Query
from .data_store import DataStore
from .units import normalize_units
from .utils import natural_sort, MAX_INT_32, TYPE_DICTIONARY
from .validate import db_validator
from time import time
import datetime
import numpy as np
import os
import random
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle


class Database(DataStore):
    """
    A data store for LCI databases.

    Databases are automatically versioned.

    Instantiation does not load any data. If this database is not yet registered in the metadata store, a warning is written to ``stdout``.

    The data schema for databases is:

    .. code-block:: python

        Schema({valid_tuple: {
            Required("name"): basestring,
            Required("type"): basestring,
            Required("exchanges"): [{
                Required("input"): valid_tuple,
                Required("type"): basestring,
                Required("amount"): Any(float, int),
                **uncertainty_fields
                }],
            "categories": Any(list, tuple),
            "location": object,
            "unit": basestring
            }}, extra=True)

    where:
        * ``valid_tuple`` is a dataset identifier, like ``("ecoinvent", "super strong steel")``
        * ``uncertainty_fields`` are fields from an uncertainty dictionary

    The data format is explained in more depth in the `Brightway2 documentation <http://brightway2.readthedocs.org/en/latest/key-concepts.html#documents>`_.

    Processing a Database actually produces two parameter arrays: one for the exchanges, which make up the technosphere and biosphere matrices, and a geomapping array which links activities to locations.

    Args:
        *name* (str): Name of the database to manage.

    """
    metadata = databases
    valdiator = db_validator
    dtype_fields = [
        ('input', np.uint32),
        ('output', np.uint32),
        ('row', np.uint32),
        ('col', np.uint32),
        ('type', np.uint8),
    ]

    dtype_fields_geomapping = [
        ('activity', np.uint32),
        ('geo', np.uint32),
        ('row', np.uint32),
        ('col', np.uint32),
    ]


    def backup(self):
        """Save a backup to ``backups`` folder.

        Returns:
            File path of backup.

        """
        from .io import BW2PackageExporter
        return BW2PackageExporter.export_database(self.name,
            folder="backups", extra_string="." + str(int(time()))
            )

    def copy(self, name):
        """Make a copy of the database.

        Internal links within the database will be updated to match the new database name, i.e. ``("old name", "some id")`` will be converted to ``("new name", "some id")`` for all exchanges.

        Args:
            * *name* (str): Name of the new database. Must not already exist.

        """
        assert name not in databases, ValueError("This database exists")
        data = self.relabel_data(self.load(), name)
        new_database = Database(name)
        new_database.register(
            format="Brightway2 copy",
            depends=databases[self.name]["depends"],
            num_processes=len(data)
        )
        new_database.write(data)
        return new_database

    @property
    def filename(self):
        return self.filename_for_version()

    def filename_for_version(self, version=None):
        """Filename for given version; Default is current.

        Returns:
            Filename (not path)

        """
        return "%s.%i" % (
            self.name,
            version or self.version
        )

    def load(self, version=None):
        """Load the intermediate data for this database.

        Can also load previous versions of this database's intermediate data.

        Args:
            * *version* (int): Version of the database to load. Default is *None*, for the latest version.

        Returns:
            The intermediate data, a dictionary.

        """
        self.assert_registered()
        if version is None and config.p.get("use_cache", False) and \
                self.name in config.cache:
            return config.cache[self.name]
        try:
            data = pickle.load(open(os.path.join(
                config.dir,
                u"intermediate",
                self.filename_for_version(version) + u".pickle"
            ), "rb"))
            if version is None and config.p.get("use_cache", False):
                config.cache[self.name] = data
            return data
        except OSError:
            raise MissingIntermediateData("This version (%i) not found" % version)

    def process(self, version=None):
        """
Process inventory documents.

Creates both a parameter array for exchanges, and a geomapping parameter array linking inventory activities to locations.

Args:
    * *version* (int, optional): The version of the database to process

Doesn't return anything, but writes two files to disk.

        """
        data = self.load(version)
        num_exchanges = sum([len(obj["exchanges"]) for obj in data.values()])

        gl = config.global_location

        # Create geomapping array
        arr = np.zeros((len(data), ), dtype=self.dtype_fields_geomapping + self.base_uncertainty_fields)
        for index, key in enumerate(sorted(data.keys(), key=lambda x: x[1])):
            arr[index] = (
                mapping[key],
                geomapping[data[key].get("location", gl) or gl],
                MAX_INT_32, MAX_INT_32,
                0, 1, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, False
            )

        filepath = os.path.join(
            config.dir,
            u"processed",
            self.name + u".geomapping.pickle"
        )
        with open(filepath, "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

        arr = np.zeros((num_exchanges + len(data), ), dtype=self.dtype)
        count = 0
        for key in sorted(data.keys(), key=lambda x: x[1]):
            production_found = False
            for exc in sorted(
                    data[key]["exchanges"],
                    key=lambda x: x["input"][1]):
                if key == exc["input"]:
                    production_found = True
                arr[count] = (
                    mapping[exc["input"]],
                    mapping[key],
                    MAX_INT_32,
                    MAX_INT_32,
                    TYPE_DICTIONARY[exc["type"]],
                    exc.get("uncertainty type", 0),
                    exc["amount"],
                    exc.get("loc", np.NaN),
                    exc.get("scale", np.NaN),
                    exc.get("shape", np.NaN),
                    exc.get("minimum", np.NaN),
                    exc.get("maximum", np.NaN),
                    exc["amount"] < 0
                )
                count += 1
            if not production_found and data[key]["type"] == "process":
                # Add amount produced for each process (default 1)
                arr[count] = (
                    mapping[key], mapping[key],
                    MAX_INT_32, MAX_INT_32, TYPE_DICTIONARY["production"],
                    0, 1, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, False
                )
                count += 1

        # The array is too big, because it can include a default production
        # amount for each activity. Trim to actual size.
        arr = arr[:count]
        filepath = os.path.join(
            config.dir,
            u"processed",
            self.name + u".pickle"
        )
        with open(filepath, "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

    def query(self, *queries):
        """Search through the database. See :class:`query.Query` for details."""
        return Query(*queries)(self.load())

    def random(self):
        """Return a random activity key.

        Returns a random activity key, or ``None`` (and issues a warning) if the current database is empty."""
        keys = self.load().keys()
        if not keys:
            warnings.warn("This database is empty")
            return None
        else:
            return random.choice(keys)

    def register(self, depends=None, **kwargs):
        """Register a database with the metadata store.

        Databases must be registered before data can be written.

        Args:
            * *format* (str): Format that the database was converted from, e.g. "Ecospold"
            * *depends* (list): Names of the databases that this database references, e.g. "biosphere"
            * *num_processes* (int): Number of processes in this database.

        """
        kwargs.update(
            depends=depends or [],
            version=kwargs.get('version', None) or 0
        )
        super(Database, self).register(**kwargs)

    def relabel_data(self, data, new_name):
        """Relabel database keys and exchanges.

        In a database which internally refer to the same database, update to new database name ``new_name``.

        Needed to copy a database completely or cut out a section of a database.

        For example:

        .. code-block:: python

            data = {
                ("old and boring", 1):
                    {"exchanges": [
                        {"input": ("old and boring", 42),
                        "amount": 1.0},
                        ]
                    },
                ("old and boring", 2):
                    {"exchanges": [
                        {"input": ("old and boring", 1),
                        "amount": 4.0}
                        ]
                    }
                }
            print relabel_database(data, "shiny new")
            >> {
                ("shiny new", 1):
                    {"exchanges": [
                        {"input": ("old and boring", 42),
                        "amount": 1.0},
                        ]
                    },
                ("shiny new", 2):
                    {"exchanges": [
                        {"input": ("shiny new", 1),
                        "amount": 4.0}
                        ]
                    }
                }

        In the example, the exchange to ``("old and boring", 42)`` does not change, as this is not part of the updated data.

        Args:
            * *data* (dict): The database data to modify
            * *new_name* (str): The name of the modified database

        Returns:
            The modified database

        """
        def relabel_exchanges(obj, new_name):
            for e in obj['exchanges']:
                if e["input"] in data:
                    e["input"] = (new_name, e["input"][1])
            return obj

        return dict([((new_name, k[1]), relabel_exchanges(v, new_name)) \
            for k, v in data.iteritems()])

    def rename(self, name):
        """Rename a database. Modifies exchanges to link to new name. Deregisters old database.

        Args:
            * *name* (str): New name.

        Returns:
            New ``Database`` object.

        """
        old_name = self.name
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            new_db = Database(name)
            databases[name] = databases[old_name]
        new_data = self.relabel_data(self.load(), name)
        new_db.write(new_data)
        new_db.process()
        del databases[old_name]
        return new_db

    def revert(self, version):
        """Return data to a previous state.

        .. warning:: Reverted changes can be overwritten.

        Args:
            * *version* (int): Number of the version to revert to.

        """
        assert version in [x[0] for x in self.versions()], "Version not found"
        self.backup()
        databases[self.name]["version"] = version
        if config.p.get("use_cache", False) and self.name in config.cache:
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
        directory = os.path.join(config.dir, "intermediate")
        files = natural_sort(filter(
            lambda x: ".".join(x.split(".")[:-2]) == self.name,
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
            if 'unit' in ds:
                ds["unit"] = normalize_units(ds["unit"])
        geomapping.add({x["location"] for x in data.values() if
                       x.get("location", False)})
        if config.p.get("use_cache", False) and self.name in config.cache:
            config.cache[self.name] = data
        filepath = os.path.join(
            config.dir,
            u"intermediate",
            self.filename + u".pickle"
        )
        with open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
