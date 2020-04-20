# -*- coding: utf-8 -*-
from .. import (
    config,
    databases,
    geomapping,
    mapping,
    projects,
)
from ..data_store import ProcessedDataStore
from ..errors import UnknownObject
from ..query import Query
from ..utils import as_uncertainty_dict
from .utils import check_exchange
from bw_processing import clean_datapackage_name, create_calculation_package
import copy
import datetime
import random
import warnings


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
            new_database.register(format="Brightway2 copy",)

        new_database.write(data)
        return new_database

    def filepath_intermediate(self):
        raise NotImplementedError

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
            exc.get("input")[0]
            for ds in data.values()
            for exc in ds.get("exchanges", [])
            if ds.get("type", "process") == "process"
            and exc.get("type") != "unknown"
            and exc.get("input", [None])[0] is not None
            and exc.get("input", [None])[0] not in ignore
        }
        return sorted(dependents)

    def find_graph_dependents(self):
        """Recursively get list of all dependent databases.

        Returns:
            A set of database names

        """

        def extend(seeds):
            return set.union(
                seeds, set.union(*[set(databases[obj]["depends"]) for obj in seeds])
            )

        seed, extended = {self.name}, extend({self.name})
        while extended != seed:
            seed, extended = extended, extend(extended)
        return extended

    def delete(self, **kwargs):
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

    def exchange_generator(self, data, kinds, add_production=True):
        """Yield exchanges, and also missing implicit production exchanges"""
        for key, obj in data.items():
            # Skip unknown types, e.g. 'product'. Strong assumption.
            if obj.get("type", "process") != "process":
                continue

            production_found = False
            for exc in obj.get("exchanges", []):
                if exc["type"] in ("production", "generic production"):
                    production_found = True
                if exc["type"] not in kinds:
                    continue
                check_exchange(exc)
                try:
                    yield {
                        **as_uncertainty_dict(exc),
                        "row": mapping[exc["input"]],
                        "col": mapping[key],
                        "flip": exc["type"] in ("technosphere", "generic consumption"),
                    }
                except KeyError:
                    raise UnknownObject(
                        (
                            "Exchange between {} and {} is invalid "
                            "- one of these objects is unknown (i.e. doesn't exist as a process dataset)"
                        ).format(exc["input"], key)
                    )

            if not production_found and add_production:
                yield {"row": mapping[key], "amount": 1}

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
        resources = []

        resources.append(
            {
                "name": clean_datapackage_name(
                    self.name + " inventory geomapping matrix"
                ),
                "matrix": "inv_mapping_matrix",
                "path": "inv_geomapping_matrix.npy",
                "data": (
                    {
                        "row": mapping[key],
                        "col": geomapping[
                            data[key].get("location") or config.global_location
                        ],
                        "amount": 1,
                    }
                    for key in data
                ),
                "nrows": len(data),
            }
        )
        resources.append(
            {
                "name": clean_datapackage_name(self.name + " technosphere matrix"),
                "matrix": "technosphere_matrix",
                "path": "technosphere_matrix.npy",
                "data": self.exchange_generator(
                    data,
                    (
                        "technosphere",
                        "generic consumption",
                        "production",
                        "substitution",
                        "generic production",
                    ),
                ),
            }
        )
        resources.append(
            {
                "name": clean_datapackage_name(self.name + " biosphere matrix"),
                "matrix": "biosphere_matrix",
                "path": "biosphere_matrix.npy",
                "data": self.exchange_generator(data, ("biosphere",), False),
            }
        )
        create_calculation_package(
            name=self.filename_processed(),
            resources=resources,
            path=self.dirpath_processed(),
            compress=True,
        )

        # Automatically set 'depends'
        self.metadata["depends"] = self.find_dependents()
        self.metadata["processed"] = datetime.datetime.now().isoformat()
        self._metadata.flush()

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
        if "depends" not in kwargs:
            kwargs["depends"] = []
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
            for e in obj.get("exchanges", []):
                if e["input"] in data:
                    e["input"] = (new_name, e["input"][1])
            return obj

        return dict(
            [
                ((new_name, k[1]), relabel_exchanges(v, new_name))
                for k, v in data.items()
            ]
        )

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
        self.name = name
        return new_db

    def write(self, data):
        """Serialize data to disk.

        ``data`` must be a dictionary of the form::

            {
                ('database name', 'dataset code'): {dataset}
            }

        """
        raise NotImplementedError
