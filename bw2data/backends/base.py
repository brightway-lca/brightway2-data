from . import sqlite3_lci_db
from .. import config, databases, geomapping
from ..data_store import ProcessedDataStore
from ..errors import UntypedExchange, InvalidExchange, UnknownObject, WrongDatabase
from ..project import writable_project
from ..query import Query
from ..search import IndexManager, Searcher
from ..utils import as_uncertainty_dict
from .proxies import Activity
from .schema import ActivityDataset, ExchangeDataset, get_id
from .utils import (
    check_exchange,
    get_csv_data_dict,
    dict_as_activitydataset,
    dict_as_exchangedataset,
    retupleize_geo_strings,
)
from bw_processing import clean_datapackage_name, create_datapackage
from fs.zipfs import ZipFS
from peewee import fn, DoesNotExist
import copy
import datetime
import itertools
import pandas
import pickle
import pprint
import pyprind
import random
import sqlite3
import warnings


_VALID_KEYS = {"location", "name", "product", "type"}


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
        data = self.relabel_data(copy.deepcopy(self.load()), name)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            new_database = self.__class__(name)
            new_database.register(format="Brightway2 copy",)

        new_database.write(data)
        return new_database

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

    def query(self, *queries):
        """Search through the database."""
        return Query(*queries)(self.load())

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
        super().register(**kwargs)

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

    ### Iteration, filtering, and ordering
    ######################################

    # Private methods

    def __iter__(self):
        for ds in self._get_queryset():
            yield Activity(ds)

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
                return Activity(self._get_queryset(random=True, filters=filters).get())
            else:
                return Activity(
                    self._get_queryset(filters=filters)
                    .offset(random.randint(0, len(self)))
                    .get()
                )
        except DoesNotExist:
            warnings.warn("This database is empty")
            return None

    def get(self, code):
        if isinstance(code, int):
            return Activity(
                self._get_queryset(filters=False)
                .where(ActivityDataset.id == code)
                .get()
            )
        else:
            return Activity(
                self._get_queryset(filters=False)
                .where(ActivityDataset.code == code)
                .get()
            )

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
        be_complicated = len(data) >= 100 and indices
        if be_complicated:
            self._drop_indices()
        sqlite3_lci_db.db.autocommit = False
        try:
            sqlite3_lci_db.db.begin()
            self.delete(keep_params=True, warn=False)
            exchanges, activities = [], []

            if not getattr(config, "is_test", None):
                self.pbar = pyprind.ProgBar(
                    len(data),
                    title="Writing activities to SQLite3 database:",
                    monitor=True,
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

    @writable_project
    def write(self, data, process=True):
        """Write ``data`` to database.

        ``data`` must be a dictionary of the form::

            {
                ('database name', 'dataset code'): {dataset}
            }

        Writing a database will first deletes all existing data."""
        if self.name not in databases:
            self.register()
        wrong_database = {key[0] for key in data}.difference({self.name})
        if wrong_database:
            raise WrongDatabase(
                "Can't write activities in databases {} to database {}".format(
                    wrong_database, self.name
                )
            )

        databases[self.name]["number"] = len(data)
        databases.set_modified(self.name)

        geomapping.add({x["location"] for x in data.values() if x.get("location")})
        if data:
            try:
                self._efficient_write_many_data(data)
            except:
                # Purge all data from database, then reraise
                self.delete(warn=False)
                raise

        self.make_searchable(reset=True)

        if process:
            self.process()

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
                # This exchange not in the reduced set of activities returned
                # by _get_queryset
                pass
        return activities

    def new_activity(self, code, **kwargs):
        obj = Activity()
        obj["database"] = self.name
        obj["code"] = str(code)
        obj["location"] = config.global_location
        obj.update(kwargs)
        return obj

    @writable_project
    def make_searchable(self, reset=False):
        if self.name not in databases:
            raise UnknownObject("This database is not yet registered")
        if self._searchable and not reset:
            print("This database is already searchable")
            return
        databases[self.name]["searchable"] = True
        databases.flush()
        IndexManager(self.filename).delete_database()
        IndexManager(self.filename).add_datasets(self)

    @writable_project
    def make_unsearchable(self):
        databases[self.name]["searchable"] = False
        databases.flush()
        IndexManager(self.filename).delete_database()

    @writable_project
    def delete(self, keep_params=False, warn=True):
        """Delete all data from SQLite database and Whoosh index"""
        if warn:
            MESSAGE = """
            Please use `del databases['{}']` instead.
            Otherwise, the metadata and database get out of sync.
            Call `.delete(warn=False)` to skip this message in the future.
            """
            warnings.warn(MESSAGE.format(self.name), UserWarning)

        vacuum_needed = len(self) > 500

        ActivityDataset.delete().where(ActivityDataset.database == self.name).execute()
        ExchangeDataset.delete().where(
            ExchangeDataset.output_database == self.name
        ).execute()
        IndexManager(self.filename).delete_database()

        if not keep_params:
            from ..parameters import (
                DatabaseParameter,
                ActivityParameter,
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
        connection = sqlite3.connect(sqlite3_lci_db._filepath)
        cursor = connection.cursor()
        for row in cursor.execute(sql, (self.name,)):
            data, row, col, input_database, input_code, output_database, output_code = row
            # Modify ``dependents`` in place
            if input_database != output_database:
                dependents.add(input_database)
            data = pickle.loads(bytes(data))
            check_exchange(data)
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

    def process(self, csv=False):
        """Create structured arrays for the technosphere and biosphere matrices.

        Uses ``bw_processing`` for array creation and metadata serialization.

        Also creates a ``geomapping`` array, linking activities to locations. Used for regionalized calculations.

        Use a raw SQLite3 cursor instead of Peewee for a ~2 times speed advantage.

        """
        # Try to avoid race conditions - but no guarantee
        self.metadata["processed"] = datetime.datetime.now().isoformat()

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
                        retupleize_geo_strings(row[1])
                        or config.global_location
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
                AND e.type IN ('technosphere', 'generic consumption')"""

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
        * ``proxy``: Return ``Activity`` proxies instead of raw Whoosh documents. Default is ``True``.

        Returns a list of ``Activity`` datasets."""
        with Searcher(self.filename) as s:
            results = s.search(string, **kwargs)
        return results

    def graph_technosphere(self, filename=None, **kwargs):
        from bw2analyzer.matrix_grapher import SparseMatrixGrapher
        from bw2calc import LCA

        lca = LCA({self.random(): 1})
        lca.lci()

        smg = SparseMatrixGrapher(lca.technosphere_matrix)
        return smg.ordered_graph(filename, **kwargs)
