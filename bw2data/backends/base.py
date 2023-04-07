import copy
import functools
import itertools
import os
import pickle
import pprint
import random
import sqlite3
import uuid
import warnings
from collections import defaultdict
from datetime import datetime
from typing import Callable, List, Optional

import numpy as np
import pandas as pd
import pyprind
from bw_processing import (
    clean_datapackage_name,
    create_datapackage,
    load_datapackage,
    safe_filename,
)
from fs.zipfs import ZipFS
from peewee import BooleanField, DoesNotExist, Model, TextField, fn

from .. import config, geomapping, projects
from ..errors import (
    DuplicateNode,
    InvalidExchange,
    UnknownObject,
    UntypedExchange,
    WrongDatabase,
)
from ..query import Query
from ..search import IndexManager, Searcher
from ..sqlite import JSONField
from ..utils import as_uncertainty_dict, get_geocollection, get_node
from .iotable import IOTableActivity, IOTableExchanges
from .proxies import Activity
from .schema import ActivityDataset, ExchangeDataset, get_id
from .utils import (
    check_exchange_amount,
    dict_as_activitydataset,
    dict_as_exchangedataset,
    get_csv_data_dict,
    retupleize_geo_strings,
)

try:
    import psutil
    monitor = True
except ImportError:
    monitor = False


_VALID_KEYS = {"location", "name", "product", "type"}


class Database(Model):
    """
    A base class for SQLite backends.

    Subclasses must support at least the following calls:

    * ``load()``
    * ``write(data)``

    In addition, they should specify their backend with the ``backend`` attribute (a unicode string).

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

    name = TextField(null=False, unique=True)
    backend = TextField(null=False, default="sqlite")
    depends = JSONField(null=False, default=[])
    geocollections = JSONField(null=False, default=[])
    dirty = BooleanField(default=True)
    searchable = BooleanField(default=True)
    extra = JSONField(null=False, default={})

    validator = None

    def __init__(self, name=None, *args, **kwargs):
        super().__init__(name=name, *args, **kwargs)

        if name and not args and not kwargs and Database.exists(name):
            # We want to maintain compatibility with `Database(name)`
            other = Database.get(Database.name == name)
            for field in (
                "id",
                "backend",
                "depends",
                "geocollections",
                "dirty",
                "searchable",
                "extra",
            ):
                setattr(self, field, getattr(other, field))

        self._filters = {}
        self._order_by = None

    def __str__(self):
        return "Brightway2 Database: {} ({})".format(self.name, self.backend)

    __repr__ = lambda self: str(self)

    def __lt__(self, other):
        if not isinstance(other, Database):
            raise TypeError
        else:
            return self.name < other.name

    @property
    def node_class(self):
        CLASSES = {"sqlite": Activity, "iotable": IOTableActivity}
        return CLASSES[self.backend]

    @classmethod
    def exists(cls, name):
        return bool(cls.select().where(cls.name == name).count())

    @classmethod
    def set_dirty(cls, name):
        cls.update(dirty=True).where(cls.name == name).execute()

    ### Generic LCI backend methods
    ###############################

    def copy(self, name):
        """Make a copy of the database.

        Internal links within the database will be updated to match the new database name, i.e. ``("old name", "some id")`` will be converted to ``("new name", "some id")`` for all exchanges.

        Args:
            * *name* (str): Name of the new database. Must not already exist.

        """
        assert not Database.exists(name), ValueError("This database exists")
        data = self.relabel_data(copy.deepcopy(self.load()), name)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            new_database = self.__class__(name)
            new_database.register(
                format="Brightway2 copy",
            )

        new_database.write(data)
        return new_database

    def dirpath_processed(self):
        return projects.dir / "processed"

    def filepath_intermediate(self):
        warnings.warn("`filepath_intermediate` is deprecated", DeprecationWarning)
        raise NotImplementedError

    @property
    def filename(self):
        """Remove filesystem-unsafe characters and perform unicode normalization on ``self.name`` using :func:`.filesystem.safe_filename`."""
        return safe_filename(self.name)

    def filename_processed(self):
        return clean_datapackage_name(self.filename + ".zip")

    def filepath_processed(self, clean=True):
        if self.dirty and clean:
            self.process()
        return self.dirpath_processed() / self.filename_processed()

    def datapackage(self):
        return load_datapackage(ZipFS(self.filepath_processed()))

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
                seeds,
                set.union(
                    *[set(Database.get(Database.name == obj).depends) for obj in seeds]
                ),
            )

        seed, extended = {self.name}, extend({self.name})
        while extended != seed:
            seed, extended = extended, extend(extended)
        return extended

    def query(self, *queries):
        """Search through the database."""
        return Query(*queries)(self.load())

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
        """Rename a database. Modifies exchanges to link to new name.

        Args:
            * *name* (str): New name.

        Returns:
            self  # Backwards compatibility

        """
        from . import sqlite3_lci_db

        old_name, new_name = self.name, name
        with sqlite3_lci_db.transaction():
            ActivityDataset.update(database=new_name).where(
                ActivityDataset.database == old_name
            ).execute()
            ExchangeDataset.update(input_database=new_name).where(
                ExchangeDataset.input_database == old_name
            ).execute()
            ExchangeDataset.update(output_database=new_name).where(
                ExchangeDataset.output_database == old_name
            ).execute()
        self.name = new_name
        self.save()
        self.process()
        return self

    ### Iteration, filtering, and ordering
    ######################################

    # Private methods

    def __iter__(self):
        for ds in self._get_queryset():
            yield self.node_class(ds)

    def __len__(self):
        return self._get_queryset().count()

    def __contains__(self, obj):
        return self._get_queryset(filters={"code": obj[1]}).count() > 0

    def _get_queryset(self, random=False, filters=True):
        qs = ActivityDataset.select().where(ActivityDataset.database == self.name)
        if filters:
            if isinstance(filters, dict):
                for key, value in filters.items():
                    qs = qs.where(getattr(ActivityDataset, key) == value)
            if self.filters:
                print("Using the following database filters:")
                pprint.pprint(self.filters)
                for key, value in self.filters.items():
                    qs = qs.where(getattr(ActivityDataset, key) == value)
        if self.order_by and not random:
            qs = qs.order_by(getattr(ActivityDataset, self.order_by))
        else:
            qs = qs.order_by(fn.Random())
        return qs

    def _get_filters(self):
        return self._filters

    def _set_filters(self, filters):
        if not filters:
            self._filters = {}
        else:
            print(
                "Filters will effect all database queries"
                " until unset (`.filters = None`)"
            )
            assert isinstance(filters, dict), "Filter must be a dictionary"
            for key in filters:
                assert key in _VALID_KEYS, "Filter key {} is invalid".format(key)
                self._filters = filters
        return self

    def _get_order_by(self):
        return self._order_by

    def _set_order_by(self, field):
        if not field:
            self._order_by = None
        else:
            assert field in _VALID_KEYS, "order_by field {} is invalid".format(field)
            self._order_by = field
        return self

    # Public API

    filters = property(_get_filters, _set_filters)
    order_by = property(_get_order_by, _set_order_by)

    def random(self, filters=True, true_random=False):
        """True random requires loading and sorting data in SQLite, and can be resource-intensive."""
        try:
            if true_random:
                return self.node_class(
                    self._get_queryset(random=True, filters=filters).get()
                )
            else:
                return self.node_class(
                    self._get_queryset(filters=filters)
                    .offset(random.randint(0, len(self)))
                    .get()
                )
        except DoesNotExist:
            warnings.warn("This database is empty")
            return None

    def get_node(self, code=None, **kwargs):
        kwargs["database"] = self.name
        if code is not None:
            kwargs["code"] = code
        return get_node(**kwargs)

    ### Data management
    ###################

    # Private methods

    def _drop_indices(self):
        from . import sqlite3_lci_db

        with sqlite3_lci_db.transaction():
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "activitydataset_key"')
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_input"')
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_output"')

    def _add_indices(self):
        from . import sqlite3_lci_db

        with sqlite3_lci_db.transaction():
            sqlite3_lci_db.execute_sql(
                'CREATE UNIQUE INDEX IF NOT EXISTS "activitydataset_key" ON "activitydataset" ("database", "code")'
            )
            sqlite3_lci_db.execute_sql(
                'CREATE INDEX IF NOT EXISTS "exchangedataset_input" ON "exchangedataset" ("input_database", "input_code")'
            )
            sqlite3_lci_db.execute_sql(
                'CREATE INDEX IF NOT EXISTS "exchangedataset_output" ON "exchangedataset" ("output_database", "output_code")'
            )

    def _efficient_write_dataset(self, index, key, ds, exchanges, activities):
        for exchange in ds.get("exchanges", []):
            if "input" not in exchange or "amount" not in exchange:
                raise InvalidExchange
            if "type" not in exchange:
                raise UntypedExchange
            exchange["output"] = key
            exchanges.append(dict_as_exchangedataset(exchange))

            # Query gets passed as INSERT INTO x VALUES ('?', '?'...)
            # SQLite3 has a limit of 999 variables,
            # So 6 fields * 125 is under the limit
            # Otherwise get the following:
            # peewee.OperationalError: too many SQL variables
            if len(exchanges) > 125:
                ExchangeDataset.insert_many(exchanges).execute()
                exchanges = []

        ds = {k: v for k, v in ds.items() if k != "exchanges"}
        ds["database"] = key[0]
        ds["code"] = key[1]

        activities.append(dict_as_activitydataset(ds))

        if len(activities) > 125:
            ActivityDataset.insert_many(activities).execute()
            activities = []

        if not getattr(config, "is_test", None):
            self.pbar.update()

        return exchanges, activities

    def _efficient_write_many_data(self, data, indices=True):
        from . import sqlite3_lci_db

        be_complicated = len(data) >= 100 and indices
        if be_complicated:
            self._drop_indices()
        sqlite3_lci_db.db.autocommit = False
        try:
            sqlite3_lci_db.db.begin()
            self.delete_data(keep_params=True, warn=False)
            exchanges, activities = [], []

            if not getattr(config, "is_test", None):
                self.pbar = pyprind.ProgBar(
                    len(data),
                    title="Writing activities to SQLite3 database:",
                    monitor=monitor,
                )

            for index, (key, ds) in enumerate(data.items()):
                exchanges, activities = self._efficient_write_dataset(
                    index, key, ds, exchanges, activities
                )

            if not getattr(config, "is_test", None):
                print(self.pbar)
                del self.pbar

            if activities:
                ActivityDataset.insert_many(activities).execute()
            if exchanges:
                ExchangeDataset.insert_many(exchanges).execute()
            sqlite3_lci_db.db.commit()
        except:
            sqlite3_lci_db.db.rollback()
            raise
        finally:
            sqlite3_lci_db.db.autocommit = True
            if be_complicated:
                self._add_indices()

    # Public API

    def write(self, data, process=True):
        """Write ``data`` to database.

        ``data`` must be a dictionary of the form::

            {
                ('database name', 'dataset code'): {dataset}
            }

        Writing a database will first deletes all existing data."""
        if self.backend == "iotable":
            process = False

        wrong_database = {key[0] for key in data}.difference({self.name})
        if wrong_database:
            raise WrongDatabase(
                "Can't write activities in databases {} to database {}".format(
                    wrong_database, self.name
                )
            )

        geocollections = {
            get_geocollection(x.get("location"))
            for x in data.values()
            if x.get("type", "process") == "process"
        }
        if None in geocollections:
            print(
                "Not able to determine geocollections for all datasets. This database is not ready for regionalization."
            )
            geocollections.discard(None)
        self.geocollections = sorted(geocollections)

        geomapping.add({x["location"] for x in data.values() if x.get("location")})
        if data:
            try:
                self._efficient_write_many_data(data)
            except:
                self.delete_data()
                raise

        self.save()
        self.make_searchable(reset=True)

        if process:
            self.process()

    def write_exchanges(self, technosphere, biosphere, dependents):
        """

        Write IO data directly to processed arrays.

        Product data is stored in SQLite as normal activities.
        Exchange data is written directly to NumPy structured arrays.

        Technosphere and biosphere data has format ``(row id, col id, value, flip)``.

        """
        if self.backend != "iotable":
            raise ValueError("Wrong kind of database")
        print("Starting IO table write")

        # create empty datapackage
        dp = create_datapackage(
            fs=ZipFS(str(self.filepath_processed()), write=True),
            name=clean_datapackage_name(self.name),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )

        # add geomapping
        dp.add_persistent_vector_from_iterator(
            dict_iterator=(
                {
                    "row": obj.id,
                    "col": geomapping[
                        obj.get("location", None) or config.global_location
                    ],
                    "amount": 1,
                }
                for obj in self
            ),
            matrix="inv_geomapping_matrix",
            name=clean_datapackage_name(self.name + " inventory geomapping matrix"),
            nrows=len(self),
        )

        print("Adding technosphere matrix")
        # if technosphere is a dictionary pass it's keys & values
        if isinstance(technosphere, dict):
            dp.add_persistent_vector(
                matrix="technosphere_matrix",
                name=clean_datapackage_name(self.name + " technosphere matrix"),
                **technosphere,
            )
        # if it is an iterable, convert to right format
        elif hasattr(technosphere, "__iter__"):
            dp.add_persistent_vector_from_iterator(
                matrix="technosphere_matrix",
                name=clean_datapackage_name(self.name + " technosphere matrix"),
                dict_iterator=technosphere,
            )
        else:
            raise Exception(
                f"Error: Unsupported technosphere type: {type(technosphere)}"
            )

        print("Adding biosphere matrix")
        # if biosphere is a dictionary pass it's keys & values
        if isinstance(biosphere, dict):
            dp.add_persistent_vector(
                matrix="biosphere_matrix",
                name=clean_datapackage_name(self.name + " biosphere matrix"),
                **biosphere,
            )
        # if it is an iterable, convert to right format
        elif hasattr(biosphere, "__iter__"):
            dp.add_persistent_vector_from_iterator(
                matrix="biosphere_matrix",
                name=clean_datapackage_name(self.name + " biosphere matrix"),
                dict_iterator=biosphere,
            )
        else:
            raise Exception(f"Error: Unsupported biosphere type: {type(technosphere)}")

        # finalize
        print("Finalizing serialization")
        dp.finalize_serialization()

        self.depends = sorted(set(dependents).difference({self.name}))
        self.save()

    def load(self, *args, **kwargs):
        # Should not be used, in general; relatively slow
        def act_formatter(dct):
            data = dct["data"]
            COLUMNS = {"code", "database", "location", "name", "type"}
            data.update({key: dct.get(key) for key in COLUMNS})
            data["reference product"] = dct.get("product")
            data["exchanges"] = []
            return (dct["database"], dct["code"]), data

        activities = dict(
            act_formatter(dct) for dct in self._get_queryset().dicts().iterator()
        )

        exchange_qs = (
            ExchangeDataset.select()
            .where(ExchangeDataset.output_database == self.name)
            .dicts()
            .iterator()
        )

        def exc_formatter(exc):
            data = exc["data"]
            data["type"] = exc["type"]
            data["input"] = (exc["input_database"], exc["input_code"])
            data["output"] = (exc["output_database"], exc["output_code"])
            return data

        for exc in exchange_qs:
            exc = exc_formatter(exc)
            try:
                activities[exc["output"]]["exchanges"].append(exc)
            except KeyError:
                # This exchange not in the reduced set of activities returned
                # by _get_queryset
                pass
        return activities

    def new_activity(self, code, **kwargs):
        return self.new_node(code, **kwargs)

    def new_node(self, code=None, **kwargs):
        obj = self.node_class()
        obj["database"] = self.name
        obj["code"] = str(code or uuid.uuid4().hex)

        if (
            ActivityDataset.select()
            .where(
                (ActivityDataset.database == self.name)
                & (ActivityDataset.code == obj["code"])
            )
            .count()
        ):
            raise DuplicateNode("Node with this database / code combo already exists")
        if (
            "id" in kwargs
            and ActivityDataset.select()
            .where(ActivityDataset.id == int("id" in kwargs))
            .count()
        ):
            raise DuplicateNode("Node with this id already exists")

        obj["location"] = config.global_location
        obj.update(kwargs)
        return obj

    def make_searchable(self, reset=False):
        if not self.id:
            raise UnknownObject(
                "This `Database` instance is not yet saved to the SQLite database"
            )

        if self.searchable and not reset:
            print("This database is already searchable")
            return
        IndexManager(self.filename).delete_database()
        IndexManager(self.filename).add_datasets(self)
        self.searchable = True
        self.save()

    def make_unsearchable(self):
        IndexManager(self.filename).delete_database()
        self.searchable = False
        self.save()

    def delete_instance(self):
        self.delete_data()
        super().delete_instance()

    def delete_data(self, keep_params=False, warn=True):
        """Delete all data from SQLite database and Whoosh index"""
        from . import sqlite3_lci_db

        vacuum_needed = len(self) > 500

        ActivityDataset.delete().where(ActivityDataset.database == self.name).execute()
        ExchangeDataset.delete().where(
            ExchangeDataset.output_database == self.name
        ).execute()
        IndexManager(self.filename).delete_database()

        if not keep_params:
            from ..parameters import (
                ActivityParameter,
                DatabaseParameter,
                ParameterizedExchange,
            )

            groups = tuple(
                {
                    o[0]
                    for o in ActivityParameter.select(ActivityParameter.group)
                    .where(ActivityParameter.database == self.name)
                    .tuples()
                }
            )
            ParameterizedExchange.delete().where(
                ParameterizedExchange.group << groups
            ).execute()
            ActivityParameter.delete().where(
                ActivityParameter.database == self.name
            ).execute()
            DatabaseParameter.delete().where(
                DatabaseParameter.database == self.name
            ).execute()

        if vacuum_needed:
            sqlite3_lci_db.vacuum()

    def exchange_data_iterator(self, sql, dependents, flip=False):
        """Iterate over exchanges and format for ``bw_processing`` arrays.

        ``dependents`` is a set of dependent database names.

        ``flip`` means flip the numeric sign; see ``bw_processing`` docs.

        Uses raw sqlite3 to retrieve data for ~2x speed boost."""
        from . import sqlite3_lci_db

        connection = sqlite3.connect(sqlite3_lci_db._filepath)
        cursor = connection.cursor()
        for line in cursor.execute(sql, (self.name,)):
            (
                data,
                row,
                col,
                input_database,
                input_code,
                output_database,
                output_code,
            ) = line
            # Modify ``dependents`` in place
            if input_database != output_database:
                dependents.add(input_database)
            data = pickle.loads(bytes(data))
            check_exchange_amount(data)
            if row is None or col is None:
                raise UnknownObject(
                    (
                        "Exchange between {} and {} is invalid "
                        "- one of these objects is unknown (i.e. doesn't exist "
                        "as a process dataset)"
                    ).format(
                        (input_database, input_code), (output_database, output_code)
                    )
                )
            yield {
                **as_uncertainty_dict(data),
                "row": row,
                "col": col,
                "flip": flip,
            }

    @classmethod
    def clean_all(cls):
        for db in cls.select().where(cls.dirty == True):
            db.process()

    def process(self, csv=False):
        """Create structured arrays for the technosphere and biosphere matrices.

        Uses ``bw_processing`` for array creation and metadata serialization.

        Also creates a ``geomapping`` array, linking activities to locations. Used for regionalized calculations.

        Use a raw SQLite3 cursor instead of Peewee for a ~2 times speed advantage.

        """
        if self.backend == "iotable":
            self.dirty = False
            self.save()
            return

        # Get number of exchanges and processes to set
        # initial Numpy array size (still have to include)
        # implicit production exchanges
        dependents = set()

        # Create geomapping array, from dataset interger ids to locations
        inv_mapping_qs = ActivityDataset.select(
            ActivityDataset.id, ActivityDataset.location
        ).where(
            ActivityDataset.database == self.name, ActivityDataset.type == "process"
        )

        # self.filepath_processed checks if data is dirty,
        # and processes if it is. This causes an infinite loop.
        # So we construct the filepath ourselves.
        fp = str(self.dirpath_processed() / self.filename_processed())

        dp = create_datapackage(
            fs=ZipFS(fp, write=True),
            name=clean_datapackage_name(self.name),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )
        dp.add_persistent_vector_from_iterator(
            matrix="inv_geomapping_matrix",
            name=clean_datapackage_name(self.name + " inventory geomapping matrix"),
            dict_iterator=(
                {
                    "row": row[0],
                    "col": geomapping[
                        retupleize_geo_strings(row[1]) or config.global_location
                    ],
                    "amount": 1,
                }
                for row in inv_mapping_qs.tuples()
            ),
            nrows=inv_mapping_qs.count(),
        )

        BIOSPHERE_SQL = """SELECT e.data, a.id, b.id, e.input_database, e.input_code, e.output_database, e.output_code
                FROM exchangedataset as e
                LEFT JOIN activitydataset as a ON a.code == e.input_code AND a.database == e.input_database
                LEFT JOIN activitydataset as b ON b.code == e.output_code AND b.database == e.output_database
                WHERE e.output_database = ?
                AND e.type = 'biosphere'
        """
        dp.add_persistent_vector_from_iterator(
            matrix="biosphere_matrix",
            name=clean_datapackage_name(self.name + " biosphere matrix"),
            dict_iterator=self.exchange_data_iterator(BIOSPHERE_SQL, dependents),
        )

        # Figure out when the production exchanges are implicit
        implicit_production = (
            {"row": get_id((self.name, x[0])), "amount": 1}
            # Get all codes
            for x in ActivityDataset.select(ActivityDataset.code)
            .where(
                # Get correct database name
                ActivityDataset.database == self.name,
                # Only consider `process` type activities
                ActivityDataset.type << ("process", None),
                # But exclude activities that already have production exchanges
                ~(
                    ActivityDataset.code
                    << ExchangeDataset.select(
                        # Get codes to exclude
                        ExchangeDataset.output_code
                    ).where(
                        ExchangeDataset.output_database == self.name,
                        ExchangeDataset.type << ("production", "generic production"),
                    )
                ),
            )
            .tuples()
        )

        TECHNOSPHERE_POSITIVE_SQL = """SELECT e.data, a.id, b.id, e.input_database, e.input_code, e.output_database, e.output_code
                FROM exchangedataset as e
                LEFT JOIN activitydataset as a ON a.code == e.input_code AND a.database == e.input_database
                LEFT JOIN activitydataset as b ON b.code == e.output_code AND b.database == e.output_database
                WHERE e.output_database = ?
                AND e.type IN ('production', 'substitution', 'generic production')
        """
        TECHNOSPHERE_NEGATIVE_SQL = """SELECT e.data, a.id, b.id, e.input_database, e.input_code, e.output_database, e.output_code
                FROM exchangedataset as e
                LEFT JOIN activitydataset as a ON a.code == e.input_code AND a.database == e.input_database
                LEFT JOIN activitydataset as b ON b.code == e.output_code AND b.database == e.output_database
                WHERE e.output_database = ?
                AND e.type IN ('technosphere', 'generic consumption')
        """

        dp.add_persistent_vector_from_iterator(
            matrix="technosphere_matrix",
            name=clean_datapackage_name(self.name + " technosphere matrix"),
            dict_iterator=itertools.chain(
                self.exchange_data_iterator(
                    TECHNOSPHERE_NEGATIVE_SQL, dependents, flip=True
                ),
                self.exchange_data_iterator(TECHNOSPHERE_POSITIVE_SQL, dependents),
                implicit_production,
            ),
        )
        if csv:
            df = pd.DataFrame([get_csv_data_dict(ds) for ds in self])
            dp.add_csv_metadata(
                dataframe=df,
                valid_for=[
                    (
                        clean_datapackage_name(self.name + " technosphere matrix"),
                        "cols",
                    ),
                    (clean_datapackage_name(self.name + " biosphere matrix"), "cols"),
                ],
                name=clean_datapackage_name(self.name + " activity metadata"),
            )

        dp.finalize_serialization()

        # Remove any possibility of datetime being in different timezone or otherwise different than filesystem
        self.dirty = False
        self.depends = sorted(dependents)
        self.save()

    def search(self, string, **kwargs):
        """Search this database for ``string``.

        The searcher include the following fields:

        * name
        * comment
        * categories
        * location
        * reference product

        ``string`` can include wild cards, e.g. ``"trans*"``.

        By default, the ``name`` field is given the most weight. The full weighting set is called the ``boost`` dictionary, and the default weights are::

            {
                "name": 5,
                "comment": 1,
                "product": 3,
                "categories": 2,
                "location": 3
            }

        Optional keyword arguments:

        * ``limit``: Number of results to return.
        * ``boosts``: Dictionary of field names and numeric boosts - see default boost values above. New values must be in the same format, but with different weights.
        * ``filter``: Dictionary of criteria that search results must meet, e.g. ``{'categories': 'air'}``. Keys must be one of the above fields.
        * ``mask``: Dictionary of criteria that exclude search results. Same format as ``filter``.
        * ``facet``: Field to facet results. Must be one of ``name``, ``product``, ``categories``, ``location``, or ``database``.
        * ``proxy``: Return ``Activity`` proxies instead of raw Whoosh documents. Default is ``True``.

        Returns a list of ``Activity`` datasets."""
        with Searcher(self.filename) as s:
            results = s.search(string=string, **kwargs)
        return results

    def set_geocollections(self):
        """Set ``geocollections`` attribute for databases which don't currently have it."""
        geocollections = {
            get_geocollection(x.get("location"))
            for x in self
            if x.get("type", "process") == "process"
        }
        if None in geocollections:
            print(
                "Not able to determine geocollections for all datasets. Not setting `geocollections`."
            )
            geocollections.discard(None)
        else:
            self.geocollections = sorted(geocollections)
            self.save()

    def graph_technosphere(self, filename=None, **kwargs):
        from bw2analyzer.matrix_grapher import SparseMatrixGrapher
        from bw2calc import LCA

        lca = LCA({self.random(): 1})
        lca.lci()

        smg = SparseMatrixGrapher(lca.technosphere_matrix)
        return smg.ordered_graph(filename, **kwargs)

    def delete_duplicate_exchanges(self, fields=["amount", "type"]):
        """Delete exchanges which are exact duplicates. Useful if you accidentally ran your input data notebook twice.

        To determine uniqueness, we look at the exchange input and output nodes, and at the exchanges values for fields ``fields``."""

        def get_uniqueness_key(exchange, fields):
            lst = [exchange.input.key, exchange.output.key]
            for field in fields:
                lst.append(exchange.get(field))
            return tuple(lst)

        exchange_mapping = defaultdict(list)

        for act in self:
            for exchange in act.exchanges():
                exchange_mapping[get_uniqueness_key(exchange, fields)].append(exchange)

        for lst in exchange_mapping.values():
            if len(lst) > 1:
                for exc in lst[-1:0:-1]:
                    print("Deleting exchange:", exc)
                    exc.delete()

    def backup(self):
        """Save a backup to ``backups`` folder.

        Returns:
            File path of backup.

        """
        try:
            from bw2io import BW2Package

            return BW2Package.export_obj(self)
        except ImportError:
            print("bw2io not installed")

    def nodes_to_dataframe(
        self, columns: Optional[List[str]] = None, return_sorted: bool = True
    ) -> pd.DataFrame:
        """Return a pandas DataFrame with all database nodes. Uses the provided node attributes by default,  such as name, unit, location.

        By default, returns a DataFrame sorted by name, reference product, location, and unit. Set ``return_sorted`` to ``False`` to skip sorting.

        Specify ``columns`` to get custom columns. You will need to write your own function to get more customization, there are endless possibilities here.

        Returns a pandas ``DataFrame``.

        """
        if columns is None:
            # Feels like magic
            df = pd.DataFrame(self)
        else:
            df = pd.DataFrame(
                [{field: obj.get(field) for field in columns} for obj in self]
            )
        if return_sorted:
            sort_columns = ["name", "reference product", "location", "unit"]
            df = df.sort_values(
                by=[column for column in sort_columns if column in df.columns]
            )
        return df

    def edges_to_dataframe(
        self, categorical: bool = True, formatters: Optional[List[Callable]] = None
    ) -> pd.DataFrame:
        """Return a pandas DataFrame with all database exchanges. Standard DataFrame columns are:

            target_id: int,
            target_database: str,
            target_code: str,
            target_name: Optional[str],
            target_reference_product: Optional[str],
            target_location: Optional[str],
            target_unit: Optional[str],
            target_type: Optional[str]
            source_id: int,
            source_database: str,
            source_code: str,
            source_name: Optional[str],
            source_product: Optional[str],  # Note different label
            source_location: Optional[str],
            source_unit: Optional[str],
            source_categories: Optional[str]  # Tuple concatenated with "::" as in `bw2io`
            edge_amount: float,
            edge_type: str,

        Target is the node consuming the edge, source is the node or flow being consumed. The terms target and source were chosen because they also work well for biosphere edges.

        Args:

        ``categorical`` will turn each string column in a `pandas Categorical Series <https://pandas.pydata.org/docs/reference/api/pandas.Categorical.html>`__. This takes 1-2 extra seconds, but saves around 50% of the memory consumption.

        ``formatters`` is a list of callables that modify each row. These functions must take the following keyword arguments, and use the `Wurst internal data format <https://wurst.readthedocs.io/#internal-data-format>`__:

            * ``node``: The target node, as a dict
            * ``edge``: The edge, including attributes of the source node
            * ``row``: The current row dict being modified.

        The functions in ``formatters`` don't need to return anything, they modify ``row`` in place.

        Returns a pandas ``DataFrame``.

        """
        if self.backend == "sqlite":
            return self._sqlite_edges_to_dataframe(
                categorical=categorical, formatters=formatters
            )
        elif self.backend == "iotable":
            return self._iotable_edges_to_dataframe()

    def _iotable_edges_to_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame with all database exchanges. DataFrame columns are:

            target_id: int,
            target_database: str,
            target_code: str,
            target_name: Optional[str],
            target_reference_product: Optional[str],
            target_location: Optional[str],
            target_unit: Optional[str],
            target_type: Optional[str]
            source_id: int,
            source_database: str,
            source_code: str,
            source_name: Optional[str],
            source_product: Optional[str],  # Note different label
            source_location: Optional[str],
            source_unit: Optional[str],
            source_categories: Optional[str]  # Tuple concatenated with "::" as in `bw2io`
            edge_amount: float,
            edge_type: str,

        Target is the node consuming the edge, source is the node or flow being consumed. The terms target and source were chosen because they also work well for biosphere edges.

        As IO Tables are normally quite large, the DataFrame building will operate directly on Numpy arrays, and therefore special formatters are not supported in this function.

        Returns a pandas ``DataFrame``.

        """
        from .. import get_activity

        @functools.lru_cache(10000)
        def cached_lookup(id_):
            return get_activity(id=id_)

        print("Retrieving metadata")
        activities = {o.id: o for o in self}

        def get(id_):
            try:
                return activities[id_]
            except KeyError:
                return cached_lookup(id_)

        def metadata_dataframe(ids, prefix="target_"):
            def dict_for_obj(obj, prefix):
                dct = {
                    f"{prefix}id": obj["id"],
                    f"{prefix}database": obj["database"],
                    f"{prefix}code": obj["code"],
                    f"{prefix}name": obj.get("name"),
                    f"{prefix}location": obj.get("location"),
                    f"{prefix}unit": obj.get("unit"),
                }
                if prefix == "target_":
                    dct["target_type"] = obj.get("type", "process")
                    dct["target_reference_product"] = obj.get("reference product")
                else:
                    dct["source_categories"] = (
                        "::".join(obj["categories"]) if obj.get("categories") else None
                    )
                    dct["source_product"] = obj.get("product")
                return dct

            return pd.DataFrame(
                [dict_for_obj(get(id_), prefix) for id_ in np.unique(ids)]
            )

        def get_edge_types(exchanges):
            arrays = []
            for resource in exchanges.resources:
                if resource["data"]["matrix"] == "biosphere_matrix":
                    arrays.append(
                        np.array(["biosphere"] * len(resource["data"]["array"]))
                    )
                else:
                    arr = np.array(["technosphere"] * len(resource["data"]["array"]))
                    arr[resource["flip"]["positive"]] = "production"
                    arrays.append(arr)

            return np.hstack(arrays)

        print("Loading datapackage")
        exchanges = IOTableExchanges(datapackage=self.datapackage())

        target_ids = np.hstack(
            [resource["indices"]["array"]["col"] for resource in exchanges.resources]
        )
        source_ids = np.hstack(
            [resource["indices"]["array"]["row"] for resource in exchanges.resources]
        )
        edge_amounts = np.hstack(
            [resource["data"]["array"] for resource in exchanges.resources]
        )
        edge_types = get_edge_types(exchanges)

        print("Creating metadata dataframes")
        target_metadata = metadata_dataframe(target_ids)
        source_metadata = metadata_dataframe(source_ids, "source_")

        print("Building merged dataframe")
        df = pd.DataFrame(
            {
                "target_id": target_ids,
                "source_id": source_ids,
                "edge_amount": edge_amounts,
                "edge_type": edge_types,
            }
        )
        df = df.merge(target_metadata, on="target_id")
        df = df.merge(source_metadata, on="source_id")

        categorical_columns = [
            "target_database",
            "target_name",
            "target_reference_product",
            "target_location",
            "target_unit",
            "target_type",
            "source_database",
            "source_code",
            "source_name",
            "source_product",
            "source_location",
            "source_unit",
            "source_categories",
            "edge_type",
        ]
        print("Compressing DataFrame")
        for column in categorical_columns:
            if column in df.columns:
                df[column] = df[column].astype("category")

        return df

    def _sqlite_edges_to_dataframe(
        self, categorical: bool = True, formatters: Optional[List[Callable]] = None
    ) -> pd.DataFrame:
        from .wurst_extraction import extract_brightway_databases

        result = []

        for target in extract_brightway_databases(self.name, add_identifiers=True):
            for edge in target["exchanges"]:
                row = {
                    "target_id": target["id"],
                    "target_database": target["database"],
                    "target_code": target["code"],
                    "target_name": target.get("name"),
                    "target_reference_product": target.get("reference product"),
                    "target_location": target.get("location"),
                    "target_unit": target.get("unit"),
                    "target_type": target.get("type", "process"),
                    "source_id": edge["id"],
                    "source_database": edge["database"],
                    "source_code": edge["code"],
                    "source_name": edge.get("name"),
                    "source_product": edge.get("product"),
                    "source_location": edge.get("location"),
                    "source_unit": edge.get("unit"),
                    "source_categories": "::".join(edge["categories"])
                    if edge.get("categories")
                    else None,
                    "edge_amount": edge["amount"],
                    "edge_type": edge["type"],
                }
                if formatters is not None:
                    for func in formatters:
                        func(node=target, edge=edge, row=row)
                result.append(row)

        print("Creating DataFrame")
        df = pd.DataFrame(result)

        if categorical:
            categorical_columns = [
                "target_database",
                "target_name",
                "target_reference_product",
                "target_location",
                "target_unit",
                "target_type",
                "source_database",
                "source_code",
                "source_name",
                "source_product",
                "source_location",
                "source_unit",
                "source_categories",
                "edge_type",
            ]
            print("Compressing DataFrame")
            for column in categorical_columns:
                if column in df.columns:
                    df[column] = df[column].astype("category")

        return df

    # Retained for compatibility but do nothing

    def validate(self, data):
        warnings.warn(
            "`Database.validate` is obsolete and does nothing", DeprecationWarning
        )
        return True

    def add_geomappings(self, data):
        warnings.warn(
            "`Database.add_geomappings` is obsolete and does nothing",
            DeprecationWarning,
        )

    @property
    def metadata(self):
        warnings.warn(
            "Database.metadata` is deprecated, use `Database` object attributes directly",
            DeprecationWarning,
        )

        # This property exists for backwards compatibility for when this
        # data was stored in a separate file. To maintain the same behaviour,
        # where the data can be updated separate from the life cycle of this object,
        # we get the current values from the database.
        # This can be confusing, as these values could differ from
        # what the user has set manually, but this method is deprecated in any case...
        obj = Database.get(Database.name == self.name)
        fp = obj.filepath_processed(clean=False)
        if fp.exists():
            modified = datetime.fromtimestamp(os.path.getmtime(fp)).isoformat()
        else:
            modified = None
        return {
            "backend": obj.backend,
            "depends": obj.depends,
            "searchable": obj.searchable,
            "number": len(obj),
            "geocollections": obj.geocollections,
            "dirty": obj.dirty,
            "processed": modified,
            "modified": modified,
        }

    @property
    def registered(self):
        warnings.warn(
            "The concept of registration is obsolete, `registered` is deprecated",
            DeprecationWarning,
        )
        return bool(self.id)

    def register(self, write_empty=True, **kwargs):
        """Legacy method to register a database with the metadata store.
        Writing data automatically sets the following metadata:
            * *depends*: Names of the databases that this database references, e.g. "biosphere"
            * *number*: Number of processes in this database.
        """
        warnings.warn(
            "Registration is no longer necessary, set the metadata directly and save the database object",
            DeprecationWarning,
        )
        self.save()

    def deregister(self):
        """Legacy method to remove an object from the metadata store. Does not delete any data."""
        warnings.warn(
            "This method is obsolete; use `Database.delete_instance()` instead",
            DeprecationWarning,
        )

        if self.id is not None:
            self.delete_instance()

    @property
    def _metadata(self):
        warnings.warn(
            "`Database._metadata` is very obsolete and should be immediately removed",
            DeprecationWarning,
        )
        from .. import databases

        return databases


SQLiteBackend = Database
