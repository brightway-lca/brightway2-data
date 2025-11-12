import copy
import datetime
import itertools
import pprint
import random
import uuid
import warnings
from collections import defaultdict
from functools import partial
from typing import Callable, Iterable, List, Optional, Union

import pandas
from bw_processing import Datapackage, clean_datapackage_name, create_datapackage
from fsspec.implementations.zip import ZipFileSystem
from peewee import JOIN, DoesNotExist, fn
from tqdm import tqdm

from bw2data import calculation_setups, config, databases, geomapping
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.proxies import Activity
from bw2data.backends.schema import ActivityDataset, ExchangeDataset, get_id
from bw2data.backends.typos import (
    check_activity_keys,
    check_activity_type,
    check_exchange_keys,
    check_exchange_type,
)
from bw2data.backends.utils import (
    check_exchange,
    dict_as_activitydataset,
    dict_as_exchangedataset,
    get_csv_data_dict,
    retupleize_geo_strings,
)
from bw2data.configuration import labels
from bw2data.data_store import ProcessedDataStore
from bw2data.errors import (
    DuplicateNode,
    InvalidExchange,
    UnknownObject,
    UntypedExchange,
    WrongDatabase,
)
from bw2data.logs import stdout_feedback_logger
from bw2data.query import Query
from bw2data.search import IndexManager, Searcher
from bw2data.signals import on_database_reset, on_database_write
from bw2data.utils import as_uncertainty_dict, get_geocollection, get_node, set_correct_process_type

_VALID_KEYS = {"location", "name", "product", "type"}


def tqdm_wrapper(iterable, is_test):
    if is_test:
        return iterable
    else:
        return tqdm(iterable)


def get_technosphere_qs(database_name: str, edge_types: Iterable[str]) -> Iterable:
    Source = ActivityDataset.alias()
    Target = ActivityDataset.alias()
    return (
        ExchangeDataset.select(
            ExchangeDataset.data,
            Source.id,
            Target.id,
            ExchangeDataset.input_database,
            ExchangeDataset.input_code,
            ExchangeDataset.output_database,
            ExchangeDataset.output_code,
        )
        .join(
            Source,
            # Use a left join to get invalid edges and raise error
            join_type=JOIN.LEFT_OUTER,
            on=(
                (ExchangeDataset.input_code == Source.code)
                & (ExchangeDataset.input_database == Source.database)
            ),
        )
        .switch(ExchangeDataset)
        .join(
            Target,
            join_type=JOIN.LEFT_OUTER,
            on=(
                (ExchangeDataset.output_code == Target.code)
                & (ExchangeDataset.output_database == Target.database)
            ),
        )
        .where(
            (ExchangeDataset.output_database == database_name)
            & (ExchangeDataset.type << edge_types)
            & (Target.type << labels.process_node_types)
        )
        .tuples()
        .iterator()
    )


get_technosphere_positive_qs = partial(
    get_technosphere_qs, edge_types=labels.technosphere_positive_edge_types
)
get_technosphere_negative_qs = partial(
    get_technosphere_qs, edge_types=labels.technosphere_negative_edge_types
)


def get_biosphere_qs(database_name: str) -> Iterable:
    Source = ActivityDataset.alias()
    Target = ActivityDataset.alias()
    return (
        ExchangeDataset.select(
            ExchangeDataset.data,
            Source.id,
            Target.id,
            ExchangeDataset.input_database,
            ExchangeDataset.input_code,
            ExchangeDataset.output_database,
            ExchangeDataset.output_code,
        )
        .join(
            Source,
            join_type=JOIN.LEFT_OUTER,
            on=(
                (ExchangeDataset.input_code == Source.code)
                & (ExchangeDataset.input_database == Source.database)
            ),
        )
        .switch(ExchangeDataset)
        .join(
            Target,
            join_type=JOIN.LEFT_OUTER,
            on=(
                (ExchangeDataset.output_code == Target.code)
                & (ExchangeDataset.output_database == Target.database)
            ),
        )
        .where(
            (ExchangeDataset.output_database == database_name)
            & (ExchangeDataset.type << labels.biosphere_edge_types)
            & (Target.type << labels.process_node_types)
        )
        .tuples()
        .iterator()
    )


class SQLiteBackend(ProcessedDataStore):
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

    _metadata = databases
    validator = None
    backend = "sqlite"
    node_class = Activity

    def __init__(self, *args, **kwargs):
        super(SQLiteBackend, self).__init__(*args, **kwargs)

        self._filters = {}
        self._order_by = None

    ### Generic LCI backend methods
    ###############################

    def copy(self, name):
        """Make a copy of the database.

        Internal links within the database will be updated to match the new database name, i.e. ``("old name", "some id")`` will be converted to ``("new name", "some id")`` for all exchanges.

        Args:
            * *name* (str): Name of the new database. Must not already exist.

        """
        assert name not in databases, ValueError("This database exists")
        data = self.relabel_data(copy.deepcopy(self.load()), self.name, name)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            new_database = self.__class__(name)
            metadata = copy.copy(self.metadata)
            metadata["format"] = f"Copied from '{self.name}'"
            new_database.register(**metadata)

        new_database.write(data, searchable=databases[name].get("searchable"))
        return new_database

    def copy_activities(
        self, activities: List[Activity], target_database: str, signal: bool = True
    ) -> List[Activity]:
        """Copy multiple Activity instances and their exchanges to a new database.

        This method copies the given activities and all their exchanges to the target database.
        Edges (exchanges) always have an input and output. Edges from copied activities are
        resolved to the new copied activities if they point to activities in the input list;
        otherwise, they remain pointing to the original database.

        If input activities have type "process" and have functional edges (input or output) to
        "product" nodes, those product nodes are also copied to the new database. Product nodes
        that will be copied are logged.

        Args:
            activities: List of Activity instances to copy
            target_database: Name of the target database (must already exist)
            signal: Whether to emit signals during save operations

        Returns:
            List of new Activity instances in the target database

        Raises:
            ValueError: If target_database does not exist
        """
        if target_database not in databases:
            raise ValueError(f"Target database '{target_database}' does not exist")
        elif target_database == self.name:
            raise ValueError(
                f"Target database '{target_database}' must be different from the source database"
            )

        # Create mapping from old keys to new keys
        old_to_new_key = {}
        new_activities = []
        nodes_to_copy = list(activities)  # Will be extended with product nodes

        # First pass: identify product nodes that need to be copied
        for activity in filter(lambda x: x.get("type") in labels.process_node_types, activities):
            for exc in filter(lambda x: x.get("functional"), activity.exchanges()):
                if (
                    exc.input.get("type") in labels.product_node_types
                    and exc.input not in nodes_to_copy
                ):
                    nodes_to_copy.append(exc.input)
                    stdout_feedback_logger.info(
                        "Product node %s will be copied due to functional edge from process %s",
                        exc.input,
                        activity,
                    )

        if any(node["database"] == target_database for node in nodes_to_copy):
            raise ValueError(
                "Input activities or associated products can't already be in the target database"
            )

        # Second pass: copy all activities
        for activity in nodes_to_copy:
            old_key = activity.key

            new_activity = activity._create_activity_copy(database=target_database)
            new_activity.save(signal=signal)

            new_key = new_activity.key
            old_to_new_key[old_key] = new_key
            new_activities.append(new_activity)

        # Third pass: copy all exchanges
        for activity in nodes_to_copy:
            old_key = activity.key
            new_key = old_to_new_key[old_key]

            for exc in activity.exchanges():
                data = {k: v for k, v in exc._data.items() if k != "id"}
                data["output"] = new_key
                inpt = old_to_new_key.get(data["input"], data["input"])
                data["input"] = inpt
                ExchangeDataset(
                    data=data,
                    input_code=inpt[1],
                    input_database=inpt[0],
                    output_code=new_key[1],
                    output_database=new_key[0],
                    type=data["type"],
                ).save(signal=signal)

        return new_activities

    def filepath_intermediate(self):
        raise NotImplementedError

    def filepath_processed(self):
        if self.metadata.get("dirty"):
            self.process()
        return self.dirpath_processed() / self.filename_processed()

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
            if ds.get("type") in labels.process_node_types
            and exc.get("type") in labels.lci_edge_types
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
            return set.union(seeds, set.union(*[set(databases[obj]["depends"]) for obj in seeds]))

        seed, extended = {self.name}, extend({self.name})
        while extended != seed:
            seed, extended = extended, extend(extended)
        return extended

    def query(self, *queries):
        """Search through the database."""
        return Query(*queries)(self.load())

    def register(self, write_empty=True, **kwargs):
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
        super().register(**kwargs)
        if write_empty:
            self.write({}, searchable=False, signal=False)

    def relabel_data(self, data: dict, old_name: str, new_name: str) -> dict:
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

        def relabel_exchanges(obj: dict, old_name: str, new_name: str) -> dict:
            for e in obj.get("exchanges", []):
                if "input" in e and e["input"][0] == old_name:
                    e["input"] = (new_name, e["input"][1])
                if "output" in e and e["output"][0] == old_name:
                    e["output"] = (new_name, e["output"][1])

            return obj

        return dict(
            [((new_name, k[1]), relabel_exchanges(v, old_name, new_name)) for k, v in data.items()]
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
        new_data = self.relabel_data(self.load(), old_name, name)
        new_db.write(new_data, searchable=databases[name].get("searchable"))
        del databases[old_name]
        self.name = name
        return new_db

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

    @property
    def _searchable(self):
        return databases.get(self.name, {}).get("searchable", False)

    def _get_queryset(self, random=False, filters=True):
        qs = ActivityDataset.select().where(ActivityDataset.database == self.name)
        if filters:
            if isinstance(filters, dict):
                for key, value in filters.items():
                    qs = qs.where(getattr(ActivityDataset, key) == value)
            if self.filters:
                stdout_feedback_logger.info(
                    "Using the following database filters: %s", pprint.pformat(self.filters)
                )
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
            stdout_feedback_logger.info(
                "Filters will effect all database queries" " until unset (`.filters = None`)"
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
                return self.node_class(self._get_queryset(random=True, filters=filters).get())
            else:
                return self.node_class(
                    self._get_queryset(filters=filters).offset(random.randint(0, len(self))).get()
                )
        except DoesNotExist:
            warnings.warn("This database is empty")
            return None

    def get(self, code=None, **kwargs):
        kwargs["database"] = self.name
        if code is not None:
            kwargs["code"] = code
        return get_node(**kwargs)

    ### Data management
    ###################

    # Private methods

    def _drop_indices(self):
        with sqlite3_lci_db.transaction():
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "activitydataset_key"')
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_input"')
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_output"')

    def _add_indices(self):
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

    def _efficient_write_dataset(
        self,
        ds: dict,
        exchanges: list,
        activities: list,
        check_typos: bool = True,
    ) -> (list, list):
        for exchange in ds.get("exchanges", []):
            if "input" not in exchange or "amount" not in exchange:
                raise InvalidExchange
            if "type" not in exchange:
                raise UntypedExchange

            if check_typos:
                check_exchange_type(exchange.get("type"))
                check_exchange_keys(exchange)

            if "output" not in exchange:
                exchange["output"] = (ds["database"], ds["code"])
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

        if check_typos:
            check_activity_type(ds.get("type"))
            check_activity_keys(ds)

        activities.append(dict_as_activitydataset(ds, add_snowflake_id=True))

        if len(activities) > 125:
            ActivityDataset.insert_many(activities).execute()
            activities = []

        return exchanges, activities

    def _efficient_write_many_data(
        self, data: list, indices: bool = True, check_typos: bool = True
    ) -> None:
        be_complicated = len(data) >= 100 and indices
        if be_complicated:
            self._drop_indices()
        sqlite3_lci_db.db.autocommit = False
        try:
            sqlite3_lci_db.db.begin()
            self.delete(keep_params=True, warn=False, vacuum=False)
            exchanges, activities = [], []

            for ds in tqdm_wrapper(data, getattr(config, "is_test", False)):
                exchanges, activities = self._efficient_write_dataset(
                    ds, exchanges, activities, check_typos
                )

            if activities:
                ActivityDataset.insert_many(activities).execute()
            if exchanges:
                ExchangeDataset.insert_many(exchanges).execute()
            sqlite3_lci_db.db.commit()
            sqlite3_lci_db.vacuum()
        except:
            sqlite3_lci_db.db.rollback()
            raise
        finally:
            sqlite3_lci_db.db.autocommit = True
            if be_complicated:
                self._add_indices()

    # Public API

    def write(
        self,
        data: Union[dict, list],
        process: bool = True,
        searchable: bool = True,
        check_typos: bool = True,
        signal: Optional[bool] = None,
    ):
        """Write ``data`` to database.

        ``data`` must be a dictionary of the form::

            {
                ('database name', 'dataset code'): {dataset}
            }

        Writing a database will first deletes all existing data."""
        from bw2data import projects

        if signal is None:
            signal = projects.dataset.is_sourced

        def merger(d1: dict, d2: dict) -> dict:
            """The joys of 3.9 compatibility"""
            d1.update(d2)
            return d1

        if isinstance(data, dict):
            data = [merger(v, {"database": db, "code": code}) for (db, code), v in data.items()]

        data = [set_correct_process_type(dataset) for dataset in data]

        if self.name not in databases:
            self.register(write_empty=False)
        wrong_database = {ds["database"] for ds in data}.difference({self.name})
        if wrong_database:
            raise WrongDatabase(
                "Can't write activities in databases {} to database {}".format(
                    wrong_database, self.name
                )
            )

        databases[self.name]["number"] = len(data)

        databases.set_modified(self.name)
        geocollections = {
            get_geocollection(dataset.get("location"))
            for dataset in data
            if dataset.get("type") in labels.process_node_types
        }
        if None in geocollections:
            stdout_feedback_logger.warning(
                "Not able to determine geocollections for all datasets. This database is not ready for regionalization."
            )
            geocollections.discard(None)
        databases[self.name]["geocollections"] = sorted(geocollections)
        # processing will flush the database metadata

        geomapping.add({x["location"] for x in data if x.get("location")})
        if data:
            try:
                self._efficient_write_many_data(data, check_typos=check_typos)
            except:
                # Purge all data from database, then reraise
                self.delete(warn=False, signal=signal)
                raise

        if searchable:
            self.make_searchable(reset=True, signal=False)

        if process:
            self.process()

        if signal:
            on_database_write.send(name=self.name)

    def load(self, *args, **kwargs):
        # Should not be used, in general; relatively slow
        activities = [obj["data"] for obj in self._get_queryset().dicts()]

        activities = {(o["database"], o["code"]): o for o in activities}
        for o in activities.values():
            o["exchanges"] = []

        exchange_qs = (
            ExchangeDataset.select(ExchangeDataset.data)
            .where(ExchangeDataset.output_database == self.name)
            .dicts()
        )

        for exc in exchange_qs:
            try:
                activities[exc["data"]["output"]]["exchanges"].append(exc["data"])
            except KeyError:
                # This exchange not in the potentially filtered set of activities returned
                # by _get_queryset
                pass
        return activities

    def new_activity(self, code, **kwargs):
        return self.new_node(code, **kwargs)

    def new_node(self, code: str = None, **kwargs):
        """Create a new activity node in this database.

        Creates a new Activity object (node) in the current database. The node is not
        automatically saved to the database; you must call ``save()`` on the returned object.

        Args:
            code: Optional unique identifier for the node. If not provided, a random UUID
                will be generated. The code must be unique within the database.
            **kwargs: Additional attributes to set on the node. Common attributes include:
                - ``name``: Human-readable name for the activity
                - ``type``: Node type (e.g., "process", "product", "emission"). Must be a
                  valid node type, not an edge type (a warning will be issued if an edge
                  type is used).
                - ``unit``: Unit of measurement (e.g., "kg", "m3")
                - ``location``: Geographic location code (e.g., "GLO", "US"). If not
                  provided, defaults to ``config.global_location``.
                - ``categories``: List or tuple of category classifications
                - Any other valid activity attributes

        Returns:
            Activity: A new Activity proxy object with the specified attributes. The object
            is not yet saved to the database.

        Raises:
            ValueError: If ``database`` is provided in kwargs and doesn't match this
                database's name, or if ``id`` is provided (ids are auto-generated).
            DuplicateNode: If a node with the same database/code combination already exists.
            UserWarning: If an edge type (e.g., "technosphere", "biosphere") is used for
                the ``type`` parameter instead of a node type.

        Examples:
            Create a simple process node:

            >>> db = DatabaseChooser("my_db")
            >>> db.register()
            >>> activity = db.new_node(code="process_1", name="Steel production",
            ...                       type="process", unit="kg", location="GLO")
            >>> activity.save()

            Create a node with auto-generated code:

            >>> activity = db.new_node(name="Custom process", type="process")
            >>> print(activity["code"])  # Random UUID
            >>> activity.save()

            Create a product node:

            >>> product = db.new_node(code="steel", name="Steel", type="product",
            ...                      unit="kg", location="GLO")
            >>> product.save()
        """
        obj = self.node_class()
        if "database" in kwargs:
            if kwargs["database"] != self.name:
                raise ValueError(
                    f"Creating a new node in database `{self.name}`, but gave database label `{kwargs['database']}`"
                )
            kwargs.pop("database")
        obj["database"] = self.name

        if "id" in kwargs:
            raise ValueError(f"`id` must be created automatically, but `id={kwargs['id']}` given.")

        if code is None:
            obj["code"] = uuid.uuid4().hex
        else:
            obj["code"] = str(code)

        if kwargs.get("type") in labels.lci_edge_types:
            EDGE_LABELS = """
Edge type label used for node.
You gave the type "{}". This is normally used for *edges*, not for *nodes*.
Here are the type values usually used for nodes:
    {}""".format(
                kwargs["type"], labels.node_types
            )
            warnings.warn(EDGE_LABELS)

        if (
            ActivityDataset.select()
            .where((ActivityDataset.database == self.name) & (ActivityDataset.code == obj["code"]))
            .count()
        ):
            raise DuplicateNode("Node with this database / code combo already exists")
        if (
            "id" in kwargs
            and ActivityDataset.select().where(ActivityDataset.id == int("id" in kwargs)).count()
        ):
            raise DuplicateNode("Node with this id already exists")

        if "location" not in kwargs:
            obj["location"] = config.global_location
        obj.update(kwargs)
        return obj

    def make_searchable(self, reset: bool = False, signal: bool = True):
        if self.name not in databases:
            raise UnknownObject("This database is not yet registered")
        if self._searchable and not reset:
            stdout_feedback_logger.info("This database is already searchable")
            return
        databases[self.name]["searchable"] = True
        databases.flush(signal=signal)
        IndexManager(self.filename).create()
        IndexManager(self.filename).add_datasets(self)

    def make_unsearchable(self, signal: bool = True):
        databases[self.name]["searchable"] = False
        databases.flush(signal=signal)
        IndexManager(self.filename).delete_database()

    def delete(
        self, keep_params: bool = False, warn: bool = True, vacuum: bool = True, signal: bool = True
    ):
        """Delete all data from SQLite database and search index"""
        if warn:
            MESSAGE = """
            Please use `del databases['{}']` instead.
            Otherwise, the metadata and database get out of sync.
            Call `.delete(warn=False)` to skip this message in the future.
            """
            warnings.warn(MESSAGE.format(self.name), UserWarning)

        vacuum_needed = len(self) > 500 and vacuum

        database_ids = {obj.id for obj in self}
        database_keys = {obj.key for obj in self}

        def purge(dct: dict) -> dict:
            return {
                key: value
                for key, value in dct.items()
                if key not in database_ids and key not in database_keys
            }

        for name, setup in calculation_setups.items():
            if any(
                (key in database_ids or key in database_keys)
                for func_unit in setup["inv"]
                for key in func_unit
            ):
                stdout_feedback_logger.warning(
                    "Removing database node(s) from calculation setup %s", name
                )
                setup["inv"] = [purge(dct) for dct in setup["inv"] if purge(dct)]
        calculation_setups.flush()

        ActivityDataset.delete().where(ActivityDataset.database == self.name).execute()
        ExchangeDataset.delete().where(ExchangeDataset.output_database == self.name).execute()
        IndexManager(self.filename).delete_database()

        if not keep_params:
            from bw2data.parameters import (
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
            ParameterizedExchange.delete().where(ParameterizedExchange.group << groups).execute()
            ActivityParameter.delete().where(ActivityParameter.database == self.name).execute()
            DatabaseParameter.delete().where(DatabaseParameter.database == self.name).execute()

        if vacuum_needed:
            sqlite3_lci_db.vacuum()

        if signal:
            on_database_reset.send(name=self.name)

    def exchange_data_iterator(self, qs_func, dependents, flip=False):
        """Iterate over exchanges and format for ``bw_processing`` arrays.

        ``dependents`` is a set of dependent database names.

        ``flip`` means flip the numeric sign; see ``bw_processing`` docs.

        Uses raw sqlite3 to retrieve data for ~2x speed boost."""
        for line in qs_func(self.name):
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
            check_exchange(data)
            if row is None or col is None:
                raise UnknownObject(
                    (
                        "Exchange between {} and {} is invalid "
                        "- one of these objects is unknown (i.e. doesn't exist "
                        "as a process dataset)"
                    ).format((input_database, input_code), (output_database, output_code))
                )
            yield {
                **as_uncertainty_dict(data),
                "row": row,
                "col": col,
                "flip": flip,
            }

    def _add_inventory_geomapping_to_datapackage(self, dp: Datapackage) -> None:
        """Add the inventory geomapping array to an existing datapackage.

        Separated out to allow for easier use in subclasses."""
        # Create geomapping array, from dataset interger ids to locations
        inv_mapping_qs = ActivityDataset.select(ActivityDataset.id, ActivityDataset.location).where(
            ActivityDataset.database == self.name,
            ActivityDataset.type << labels.process_node_types,
        )

        if self.metadata.get("location_normalization"):
            location_mapper = lambda x: self.metadata["location_normalization"].get(x, x)
        else:
            location_mapper = lambda x: x

        dp.add_persistent_vector_from_iterator(
            matrix="inv_geomapping_matrix",
            name=clean_datapackage_name(self.name + " inventory geomapping matrix"),
            dict_iterator=(
                {
                    "row": row[0],
                    "col": geomapping[
                        location_mapper(retupleize_geo_strings(row[1]) or config.global_location)
                    ],
                    "amount": 1,
                }
                for row in inv_mapping_qs.tuples()
            ),
            nrows=inv_mapping_qs.count(),
        )

    def process(self, csv=False):
        """Create structured arrays for the technosphere and biosphere matrices.

        Uses ``bw_processing`` for array creation and metadata serialization.

        Also creates a ``geomapping`` array, linking activities to locations. Used for regionalized calculations.

        Use a raw SQLite3 cursor instead of Peewee for a ~2 times speed advantage.

        """
        # Try to avoid race conditions - but no guarantee
        self.metadata["processed"] = datetime.datetime.now().isoformat()
        # Get number of exchanges and processes to set
        # initial Numpy array size (still have to include
        # implicit production exchanges)
        dependents = set()

        # self.filepath_processed checks if data is dirty,
        # and processes if it is. This causes an infinite loop.
        # So we construct the filepath ourselves.
        fp = str(self.dirpath_processed() / self.filename_processed())

        dp = create_datapackage(
            fs=ZipFileSystem(fp, mode="w"),
            name=clean_datapackage_name(self.name),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )
        self._add_inventory_geomapping_to_datapackage(dp)

        dp.add_persistent_vector_from_iterator(
            matrix="biosphere_matrix",
            name=clean_datapackage_name(self.name + " biosphere matrix"),
            dict_iterator=self.exchange_data_iterator(get_biosphere_qs, dependents),
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
                ActivityDataset.type << labels.implicit_production_allowed_node_types,
                # But exclude activities that already have production exchanges
                ~(
                    ActivityDataset.code
                    << ExchangeDataset.select(
                        # Get codes to exclude
                        ExchangeDataset.output_code
                    ).where(
                        ExchangeDataset.output_database == self.name,
                        ExchangeDataset.type << labels.technosphere_positive_edge_types,
                    )
                ),
            )
            .tuples()
        )

        dp.add_persistent_vector_from_iterator(
            matrix="technosphere_matrix",
            name=clean_datapackage_name(self.name + " technosphere matrix"),
            dict_iterator=itertools.chain(
                self.exchange_data_iterator(get_technosphere_negative_qs, dependents, flip=True),
                self.exchange_data_iterator(get_technosphere_positive_qs, dependents),
                implicit_production,
            ),
        )
        if csv:
            df = pandas.DataFrame([get_csv_data_dict(ds) for ds in self])
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

        self.metadata["depends"] = sorted(dependents)
        self.metadata["dirty"] = False
        self._metadata.flush()

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
        * ``proxy``: Return ``Activity`` proxies instead of dictionary index Models. Default is ``True``.

        Returns a list of ``Activity`` datasets."""
        with Searcher(self.filename) as s:
            results = s.search(string=string, **kwargs)
        return results

    def set_geocollections(self):
        """Set ``geocollections`` attribute for databases which don't currently have it."""
        geocollections = {
            get_geocollection(x.get("location"))
            for x in self
            if x.get("type") in labels.process_node_types
        }
        if None in geocollections:
            stdout_feedback_logger.warning(
                "Not able to determine geocollections for all datasets. Not setting `geocollections`."
            )
            geocollections.discard(None)
        else:
            self.metadata["geocollections"] = sorted(geocollections)
            self._metadata.flush()

    def graph_technosphere(self, filename=None, **kwargs):
        from bw2analyzer.matrix_grapher import SparseMatrixGrapher
        from bw2calc import LCA

        lca = LCA({self.random(): 1})
        lca.lci()

        smg = SparseMatrixGrapher(lca.technosphere_matrix)
        return smg.ordered_graph(filename, **kwargs)

    def delete_duplicate_exchanges(self, fields=["amount", "type"]):
        """Delete exchanges which are exact duplicates. Useful if you accidentally ran your input data notebook twice.

        To determine uniqueness, we look at the exchange input and output nodes, and at the exchanges values for fields ``fields``.
        """

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
                    stdout_feedback_logger.warning("Deleting exchange: %s", exc)
                    exc.delete()

    def nodes_to_dataframe(
        self, columns: Optional[List[str]] = None, return_sorted: bool = True
    ) -> pandas.DataFrame:
        """Return a pandas DataFrame with all database nodes. Uses the provided node attributes by default,  such as name, unit, location.

        By default, returns a DataFrame sorted by name, reference product, location, and unit. Set ``return_sorted`` to ``False`` to skip sorting.

        Specify ``columns`` to get custom columns. You will need to write your own function to get more customization, there are endless possibilities here.

        Returns a pandas ``DataFrame``.

        """
        if columns is None:
            # Feels like magic
            df = pandas.DataFrame(self)
        else:
            df = pandas.DataFrame([{field: obj.get(field) for field in columns} for obj in self])
        if return_sorted:
            sort_columns = ["name", "reference product", "location", "unit"]
            df = df.sort_values(by=[column for column in sort_columns if column in df.columns])
        return df

    def edges_to_dataframe(
        self, categorical: bool = True, formatters: Optional[List[Callable]] = None
    ) -> pandas.DataFrame:
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
        from bw2data.backends.wurst_extraction import extract_brightway_databases

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
                    "target_type": target.get("type", labels.process_node_default),
                    "source_id": edge["id"],
                    "source_database": edge["database"],
                    "source_code": edge["code"],
                    "source_name": edge.get("name"),
                    "source_product": edge.get("product"),
                    "source_location": edge.get("location"),
                    "source_unit": edge.get("unit"),
                    "source_categories": (
                        "::".join(edge["categories"]) if edge.get("categories") else None
                    ),
                    "edge_amount": edge["amount"],
                    "edge_type": edge["type"],
                }
                if formatters is not None:
                    for func in formatters:
                        func(node=target, edge=edge, row=row)
                result.append(row)

        stdout_feedback_logger.info("Creating DataFrame")
        df = pandas.DataFrame(result)

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
            stdout_feedback_logger.info("Compressing DataFrame")
            for column in categorical_columns:
                if column in df.columns:
                    df[column] = df[column].astype("category")

        return df
