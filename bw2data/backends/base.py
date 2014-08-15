# -*- coding: utf-8 -*-
from .. import databases, config, mapping, geomapping
from ..query import Query
from ..data_store import DataStore
from ..utils import MAX_INT_32, TYPE_DICTIONARY, safe_filename
from ..errors import UntypedExchange, InvalidExchange
import copy
import numpy as np
import os
import random
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle


class LCIBackend(DataStore):
    """
    A base class for LCI backends.

    Subclasses must support at least the following calls:

    * ``load()``
    * ``write(data)``

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

    The data format is explained in more depth in the `Brightway2 key concepts documentation <http://brightway2.readthedocs.org/en/latest/key-concepts/data-formats.html#database-documents>`_.

    Processing a Database actually produces two parameter arrays: one for the exchanges, which make up the technosphere and biosphere matrices, and a geomapping array which links activities to locations.

    Args:
        *name* (str): Name of the database to manage.

    """
    metadata = databases
    validator = None
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
    def filename(self):
        return safe_filename(self.name)

    def filepath_intermediate(self):
        raise NotImplementedError

    def filepath_processed(self):
        return os.path.join(
            config.dir,
            u"processed",
            self.filename + u".pickle"
        )

    def filepath_geomapping(self):
        return os.path.join(
            config.dir,
            u"processed",
            self.filename + u".geomapping.pickle"
        )

    def find_dependents(self, data=None, ignore=None):
        """Get sorted list of dependent databases (databases linked from exchanges).

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
            exc.get(u'input')[0]
            for ds in data.values()
            for exc in ds.get(u'exchanges', [])
            if ds.get(u'type', u'process') == u'process'
            and exc.get(u'type') != u"unknown"
            and exc.get(u'input', [None])[0] is not None
            and exc.get(u'input', [None])[0] not in ignore
        }
        return sorted(dependents)

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
        * ``iteritems()``

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
        data = self.load(*args, **kwargs)
        num_exchanges = sum([
            len(obj.get(u"exchanges", []))
            for obj in data.values()
            if obj.get(u"type", u"process") == u"process"
        ])

        gl = config.global_location

        # Create geomapping array
        count = 0
        arr = np.zeros((len(data), ), dtype=self.dtype_fields_geomapping + self.base_uncertainty_fields)
        for key in sorted(data.keys(), key=lambda x: x[1]):
            if data[key].get(u'type', u'process') == u'process':
                arr[count] = (
                    mapping[key],
                    geomapping[data[key].get(u"location", gl) or gl],
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
            if data[key].get('type', u'process') != u"process":
                continue
            for exc in sorted(
                    data[key].get(u"exchanges", []),
                    key=lambda x: x.get(u"input")):

                if u"amount" not in exc or u"input" not in exc:
                    raise InvalidExchange
                if u"type" not in exc:
                    raise UntypedExchange

                if key == exc[u"input"]:
                    production_found = True
                arr[count] = (
                    mapping[exc[u"input"]],
                    mapping[key],
                    MAX_INT_32,
                    MAX_INT_32,
                    TYPE_DICTIONARY[exc[u"type"]],
                    exc.get(u"uncertainty type", 0),
                    exc[u"amount"],
                    exc[u"amount"] \
                        if exc.get(u"uncertainty type", 0) in (0,1) \
                        else exc.get(u"loc", np.NaN),
                    exc.get(u"scale", np.NaN),
                    exc.get(u"shape", np.NaN),
                    exc.get(u"minimum", np.NaN),
                    exc.get(u"maximum", np.NaN),
                    exc[u"amount"] < 0
                )
                count += 1
            if not production_found:
                # Add amount produced for each process (default 1)
                arr[count] = (
                    mapping[key], mapping[key],
                    MAX_INT_32, MAX_INT_32, TYPE_DICTIONARY[u"production"],
                    0, 1, 1, np.NaN, np.NaN, np.NaN, np.NaN, False
                )
                count += 1

        # Automatically set 'depends'
        self.metadata[self.name]['depends'] = self.find_dependents()
        self.metadata.flush()

        # The array is too big, because it can include a default production
        # amount for each activity. Trim to actual size.
        arr = arr[:count]
        with open(self.filepath_processed(), "wb") as f:
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

    def register(self, **kwargs):
        """Register a database with the metadata store.

        Databases must be registered before data can be written.

        Writing data automatically sets the following metadata:
            * *depends*: Names of the databases that this database references, e.g. "biosphere"
            * *number*: Number of processes in this database.

        Args:
            * *format* (str): Format that the database was converted from, e.g. "Ecospold"

        """
        if u'depends' not in kwargs:
            kwargs[u'depends'] = []
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
            for e in obj.get(u'exchanges', []):
                if e[u"input"] in data:
                    e[u"input"] = (new_name, e[u"input"][1])
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
            new_db = self.__class__(name)
            databases[name] = databases[old_name]
        new_data = self.relabel_data(self.load(), name)
        new_db.write(new_data)
        new_db.process()
        del databases[old_name]
        return new_db

    def write(self, data):
        """Serialize data to disk.

        Args:
            * *data* (dict): Inventory data

        """
        raise NotImplementedError
