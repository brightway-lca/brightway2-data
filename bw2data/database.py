# -*- coding: utf-8 -*-
from . import databases, config, mapping, geomapping
from .errors import MissingIntermediateData, UnknownObject
from .query import Query
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


class Database(object):
    """A manager for a database. This class can register or deregister databases, write intermediate data, process data to parameter arrays, query, validate, and copy databases.

    Databases are automatically versioned.

    The Database class never holds intermediate data, but it can load or write intermediate data. The only attribute is *database*, which is the name of the database being managed.

    Instantiation does not load any data. If this database is not yet registered in the metadata store, a warning is written to ``stdout``.

    Args:
        *database* (str): Name of the database to manage.

    """
    def __init__(self, database):
        """Instantiate a Database object.

        Does not load any data. If this database is not yet registered in the metadata store, a warning is written to **stdout**.


        """
        self.database = database
        if self.database not in databases and not \
                getattr(config, "dont_warn", False):
            warnings.warn("\n\t%s not a currently installed database" % \
                database, UserWarning)

    def __unicode__(self):
        return u"Brightway2 database %s" % self.database

    def __str__(self):
        return unicode(self).encode('utf-8')

    def backup(self):
        """Save a backup to ``backups`` folder.

        Returns:
            File path of backup.

        """
        from .io import BW2PackageExporter
        return BW2PackageExporter.export_database(self.database,
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
            depends=databases[self.database]["depends"],
            num_processes=len(data))
        new_database.write(data)
        return new_database

    def deregister(self):
        """Remove a database from the metadata store. Does not delete any files."""
        del databases[self.database]

    def filename(self, version=None):
        """Filename for given version; Default is current.

        Returns:
            Filename (not path)

        """
        return "%s.%i.pickle" % (
            self.database,
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
        if self.database not in databases:
            raise UnknownObject("This database is not yet registered")
        if version is None and config.p.get("use_cache", False) and \
                self.database in config.cache:
            return config.cache[self.database]
        try:
            data = pickle.load(open(os.path.join(
                config.dir,
                "intermediate",
                self.filename(version)
            ), "rb"))
            if version is None and config.p.get("use_cache", False):
                config.cache[self.database] = data
            return data
        except OSError:
            raise MissingIntermediateData("This version (%i) not found" % version)

    def process(self, version=None):
        """
Process intermediate data from a Python dictionary to a `stats_arrays <https://pypi.python.org/pypi/stats_arrays/>`_ array, which is a `NumPy <http://numpy.scipy.org/>`_ `Structured <http://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html#numpy.recarray>`_ `Array <http://docs.scipy.org/doc/numpy/user/basics.rec.html>`_. A structured array (also called record array) is a heterogeneous array, where each column has a different label and data type.

Processed arrays are saved in the ``processed`` directory.

The structure for processed inventory databases includes additional columns beyond the basic ``stats_arrays`` format:

================ ======== ===================================
Column name      Type     Description
================ ======== ===================================
uncertainty_type uint8    integer type defined in `stats_arrays.uncertainty_choices`
input            uint32   integer value from `Mapping`
output           uint32   integer value from `Mapping`
geo              uint32   integer value from `GeoMapping`
row              uint32   column filled with `NaN` values, used for matrix construction
col              uint32   column filled with `NaN` values, used for matrix construction
type             uint8    integer type defined in `bw2data.utils.TYPE_DICTIONARY`
amount           float32  amount without uncertainty
loc              float32  location parameter, e.g. mean
scale            float32  scale parameter, e.g. standard deviation
shape            float32  shape parameter
minimum          float32  minimum bound
maximum          float32  maximum bound
negative         bool     `amount` < 0
================ ======== ===================================

See also `NumPy data types <http://docs.scipy.org/doc/numpy/user/basics.types.html>`_.

Args:
    * *version* (int, optional): The version of the database to process

Doesn't return anything, but writes a file to disk.

        """
        data = self.load(version)
        num_exchanges = sum([len(obj["exchanges"]) for obj in data.values()])
        assert data
        dtype = [
            ('uncertainty_type', np.uint8),
            ('input', np.uint32),
            ('output', np.uint32),
            ('geo', np.uint32),
            ('row', np.uint32),
            ('col', np.uint32),
            ('type', np.uint8),
            ('amount', np.float32),
            ('loc', np.float32),
            ('scale', np.float32),
            ('shape', np.float32),
            ('minimum', np.float32),
            ('maximum', np.float32),
            ('negative', np.bool)
        ]
        arr = np.zeros((num_exchanges + len(data), ), dtype=dtype)
        count = 0
        for key in sorted(data.keys(), key=lambda x: x[1]):
            production_found = False
            for exc in sorted(
                    data[key]["exchanges"],
                    key=lambda x: x["input"][1]):
                if key == exc["input"]:
                    production_found = True
                arr[count] = (
                    exc["uncertainty type"],
                    mapping[exc["input"]],
                    mapping[key],
                    geomapping[data[key].get("location", "GLO") or "GLO"],
                    MAX_INT_32,
                    MAX_INT_32,
                    TYPE_DICTIONARY[exc["type"]],
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
                    0, mapping[key], mapping[key],
                    geomapping[data[key].get("location", "GLO") or "GLO"],
                    MAX_INT_32, MAX_INT_32, TYPE_DICTIONARY["production"],
                    1, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, False
                )
                count += 1

        # The array is too big, because it can include a default production
        # amount for each activity. Trim to actual size.
        arr = arr[:count]
        filepath = os.path.join(
            config.dir,
            "processed",
            "%s.pickle" % self.database
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

    def register(self, format, depends, num_processes, version=None):
        """Register a database with the metadata store.

        Databases must be registered before data can be written.

        Args:
            * *format* (str): Format that the database was converted from, e.g. "Ecospold"
            * *depends* (list): Names of the databases that this database references, e.g. "biosphere"
            * *num_processes* (int): Number of processes in this database.

        """
        assert self.database not in databases
        databases[self.database] = {
            "from format": format,
            "depends": depends,
            "number": num_processes,
            "version": version or 0
        }

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
        old_name = self.database
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
        databases[self.database]["version"] = version
        if config.p.get("use_cache", False) and self.database in config.cache:
            config.cache[self.database] = self.load(version)
        self.process(version)

    def validate(self, data):
        """Validate data. Must be called manually.

        Raises ``voluptuous.Invalid`` if data does not validate.

        Args:
            * *data* (dict): The data, in its processed form.

        """
        db_validator(data)

    @property
    def version(self):
        """The current version number (integer) of this database.

        Returns:
            Version number

        """
        return databases.version(self.database)

    def versions(self):
        """Get a list of available versions of this database.

        Returns:
            List of (version, datetime created) tuples.

        """
        directory = os.path.join(config.dir, "intermediate")
        files = natural_sort(filter(
            lambda x: ".".join(x.split(".")[:-2]) == self.database,
            os.listdir(directory)))
        return sorted([(int(name.split(".")[-2]),
            datetime.datetime.fromtimestamp(os.stat(os.path.join(
            config.dir, directory, name)).st_mtime)) for name in files])

    def write(self, data):
        """Serialize data to disk.

        Args:
            * *data* (dict): Inventory data

        """
        if self.database not in databases:
            raise UnknownObject("This database is not yet registered")
        databases.increment_version(self.database, len(data))
        mapping.add(data.keys())
        for ds in data.values():
            ds["unit"] = normalize_units(ds["unit"])
        geomapping.add([x["location"] for x in data.values() if
                       x.get("location", False)])
        if config.p.get("use_cache", False) and self.database in config.cache:
            config.cache[self.database] = data
        filepath = os.path.join(config.dir, "intermediate", self.filename())
        with open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
