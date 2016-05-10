# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .. import (
    config,
    database_parameters,
    databases,
    geomapping,
    mapping,
    projects,
)
from ..data_store import ProcessedDataStore
from ..database_parameters import database_parameters, DatabaseParameterSet
from ..errors import UntypedExchange, InvalidExchange, UnknownObject
from ..query import Query
from ..utils import MAX_INT_32, TYPE_DICTIONARY, safe_filename, numpy_string
import copy
import numpy as np
import os
import random
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle


class LCIBackend(ProcessedDataStore):
    """
    A base class for LCI backends.

    Subclasses must support at least the following calls:

    * ``load()``
    * ``write(data)``

    In addition, they should specify their backend with the ``backend`` attribute (a unicode string).

    ``LCIBackend`` provides the following, which should not need to be modified:

    * ``rename``
    * ``copy``
    * ``find_dependents``
    * ``random``
    * ``process``

    For new classes to be recognized by the ``DatabaseChooser``, they need to be registered with the ``config`` object, e.g.:

    .. code-block:: python

        config.backends['backend type string'] = BackendClass

    Instantiation does not load any data. If this database is not yet registered in the metadata store, a warning is written to ``stdout``.

    The data schema for databases in voluptuous is:

    .. code-block:: python

        exchange = {
                Required("input"): valid_tuple,
                Required("type"): basestring,
                }
        exchange.update(uncertainty_dict)
        lci_dataset = {
            Optional("categories"): Any(list, tuple),
            Optional("location"): object,
            Optional("unit"): basestring,
            Optional("name"): basestring,
            Optional("type"): basestring,
            Optional("exchanges"): [exchange]
        }
        db_validator = Schema({valid_tuple: lci_dataset}, extra=True)

    where:
        * ``valid_tuple`` is a :ref:`dataset identifier <dataset-codes>`, like ``("ecoinvent", "super strong steel")``
        * ``uncertainty_fields`` are fields from an :ref:`uncertainty dictionary <uncertainty-type>`.

    Processing a Database actually produces two parameter arrays: one for the exchanges, which make up the technosphere and biosphere matrices, and a geomapping array which links activities to locations.

    Args:
        *name* (unicode string): Name of the database to manage.

    """
    _metadata = databases
    validator = None
    dtype_fields = [
        (numpy_string('input'), np.uint32),
        (numpy_string('output'), np.uint32),
        (numpy_string('row'), np.uint32),
        (numpy_string('col'), np.uint32),
        (numpy_string('type'), np.uint8),
    ]
    dtype_fields_geomapping = [
        (numpy_string('activity'), np.uint32),
        (numpy_string('geo'), np.uint32),
        (numpy_string('row'), np.uint32),
        (numpy_string('col'), np.uint32),
    ]

    def copy(self, name):
        """Make a copy of the database.

        Internal links within the database will be updated to match the new database name, i.e. ``("old name", "some id")`` will be converted to ``("new name", "some id")`` for all exchanges.

        Args:
            * *name* (str): Name of the new database. Must not already exist.

        """
        assert name not in databases, ValueError("This database exists")
        data = self.relabel_data(copy.deepcopy(self.load()), name)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            new_database = self.__class__(name)
            new_database.register(
                format="Brightway2 copy",
            )

        new_database.write(data)
        return new_database

    @property
    def parameters(self):
        if not hasattr(self, "_database_parameters"):
            self._database_parameters = DatabaseParameterSet(
                self.name,
                database_parameters.get(self.name, {})
            )
        return self._database_parameters

    @property
    def filename(self):
        return safe_filename(self.name)

    def filepath_intermediate(self):
        raise NotImplementedError

    def filepath_geomapping(self):
        return os.path.join(
            projects.dir,
            "processed",
            self.filename + ".geomapping.pickle"
        )

    def find_dependents(self, data=None, ignore=None):
        """Get sorted list of direct dependent databases (databases linked from exchanges).

        Args:
            * *data* (dict, optional): Inventory data
            * *ignore* (list): List of database names to ignore

        Returns:
            List of database names

        """
        ignore = set(ignore if ignore is not None else [])
        if data is None:
            data = self.load()
            ignore.add(self.name)
        dependents = {
            exc.get('input')[0]
            for ds in data.values()
            for exc in ds.get('exchanges', [])
            if ds.get('type', 'process') == 'process'
            and exc.get('type') != "unknown"
            and exc.get('input', [None])[0] is not None
            and exc.get('input', [None])[0] not in ignore
        }
        return sorted(dependents)

    def find_graph_dependents(self):
        """Recursively get list of all dependent databases.

        Returns:
            A set of database names

        """
        def extend(seeds):
            return set.union(seeds,
                             set.union(*[set(databases[obj]['depends'])
                                         for obj in seeds]))

        seed, extended = {self.name}, extend({self.name})
        while extended != seed:
            seed, extended = extended, extend(extended)
        return extended

    def delete(self):
        """Delete data from this instance. For the base class, only clears cached data."""
        if self.name in config.cache:
            del config.cache[self.name]

    def load(self, *args, **kwargs):
        """Load the intermediate data for this database.

        If ``load()`` does not return a dictionary, then the returned object must have at least the following dictionary-like methods:

        * ``__iter__``
        * ``__contains__``
        * ``__getitem__``
        * ``__setitem__``
        * ``__delitem__``
        * ``__len__``
        * ``keys()``
        * ``values()``
        * ``items()``
        * ``items()``

        However, this method **must** support the keyword argument ``as_dict``, and ``.load(as_dict=True)`` must return a normal dictionary with all Database data. This is necessary for JSON serialization.

        It is recommended to subclass ``collections.MutableMapping`` (see ``SynchronousJSONDict`` for an example of data loaded on demand).

        """
        raise NotImplementedError

    def process(self, *args, **kwargs):
        """
Process inventory documents.

Creates both a parameter array for exchanges, and a geomapping parameter array linking inventory activities to locations.

If the uncertainty type is no uncertainty, undefined, or not specified, then the 'amount' value is used for 'loc' as well. This is needed for the random number generator.

Args:
    * *version* (int, optional): The version of the database to process

Doesn't return anything, but writes two files to disk.

        """
        data = self.load(as_dict=True, *args, **kwargs)
        num_exchanges = sum([
            len(obj.get("exchanges", []))
            for obj in data.values()
            if obj.get("type", "process") == "process"
        ])

        gl = config.global_location

        # Create geomapping array
        count = 0
        arr = np.zeros((len(data), ), dtype=self.dtype_fields_geomapping + self.base_uncertainty_fields)
        for key in sorted(data.keys(), key=lambda x: x[1]):
            if data[key].get('type', 'process') == 'process':
                arr[count] = (
                    mapping[key],
                    geomapping[data[key].get("location", gl) or gl],
                    MAX_INT_32, MAX_INT_32,
                    0, 1, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, False
                )
                count += 1

        with open(self.filepath_geomapping(), "wb") as f:
            pickle.dump(arr[:count], f, protocol=pickle.HIGHEST_PROTOCOL)

        arr = np.zeros((num_exchanges + len(data), ), dtype=self.dtype)
        count = 0

        for key in sorted(data.keys(), key=lambda x: x[1]):
            production_found = False
            if data[key].get('type', 'process') != "process":
                continue
            for exc in sorted(
                    data[key].get("exchanges", []),
                    key=lambda x: x.get("input")):

                if "amount" not in exc or "input" not in exc:
                    raise InvalidExchange
                if "type" not in exc:
                    raise UntypedExchange
                if np.isnan(exc['amount']) or np.isinf(exc['amount']):
                    raise ValueError("Invalid amount in exchange {}".format(data))

                if exc['type'] == 'production':
                    production_found = True
                try:
                    arr[count] = (
                        mapping[exc["input"]],
                        mapping[key],
                        MAX_INT_32,
                        MAX_INT_32,
                        TYPE_DICTIONARY[exc["type"]],
                        exc.get("uncertainty type", 0),
                        exc["amount"],
                        exc["amount"] \
                            if exc.get("uncertainty type", 0) in (0,1) \
                            else exc.get("loc", np.NaN),
                        exc.get("scale", np.NaN),
                        exc.get("shape", np.NaN),
                        exc.get("minimum", np.NaN),
                        exc.get("maximum", np.NaN),
                        exc["amount"] < 0
                    )

                except KeyError:
                    raise UnknownObject(("Exchange between {} and {} is invalid "
                        "- {} is unknown (i.e. doesn't exist as a process dataset)"
                        ).format(exc["input"], key, exc["input"])
                    )

                count += 1
            if not production_found:
                # Add amount produced for each process (default 1)
                arr[count] = (
                    mapping[key], mapping[key],
                    MAX_INT_32, MAX_INT_32, TYPE_DICTIONARY["production"],
                    0, 1, 1, np.NaN, np.NaN, np.NaN, np.NaN, False
                )
                count += 1

        # Automatically set 'depends'
        self.metadata['depends'] = self.find_dependents()
        self._metadata.flush()

        # The array is too big, because it can include a default production
        # amount for each activity. Trim to actual size.
        arr = arr[:count]
        with open(self.filepath_processed(), "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

    def query(self, *queries):
        """Search through the database."""
        return Query(*queries)(self.load())

    def random(self):
        """Return a random activity key.

        Returns a random activity key, or ``None`` (and issues a warning) if the current database is empty."""
        keys = [x for x in mapping if x and x[0] == self.name]
        if not keys:
            warnings.warn("This database is empty")
            return None
        else:
            return self.get(random.choice(keys)[1])

    def register(self, **kwargs):
        """Register a database with the metadata store.

        Databases must be registered before data can be written.

        Writing data automatically sets the following metadata:
            * *depends*: Names of the databases that this database references, e.g. "biosphere"
            * *number*: Number of processes in this database.

        Args:
            * *format* (str, optional): Format that the database was converted from, e.g. "Ecospold"

        """
        if 'depends' not in kwargs:
            kwargs['depends'] = []
        kwargs["backend"] = self.backend
        super(LCIBackend, self).register(**kwargs)

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
            print(relabel_database(data, "shiny new"))
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
            * *data* (dict): The data to modify
            * *new_name* (str): The name of the modified database

        Returns:
            The modified data

        """
        def relabel_exchanges(obj, new_name):
            for e in obj.get('exchanges', []):
                if e["input"] in data:
                    e["input"] = (new_name, e["input"][1])
            return obj

        return dict([((new_name, k[1]), relabel_exchanges(v, new_name)) \
            for k, v in data.items()])

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
            new_db = self.__class__(name)
            databases[name] = databases[old_name]
        new_data = self.relabel_data(self.load(), name)
        new_db.write(new_data)
        del databases[old_name]
        return new_db

    def write(self, data):
        """Serialize data to disk.

        ``data`` must be a dictionary of the form::

            {
                ('database name', 'dataset code'): {dataset}
            }

        """
        raise NotImplementedError
