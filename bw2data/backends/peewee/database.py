# -*- coding: utf-8 -*-
from . import sqlite3_lci_db
from ... import mapping, geomapping, config, databases, preferences
from ...errors import UntypedExchange, InvalidExchange, UnknownObject, WrongDatabase
from ...project import writable_project
from ...search import IndexManager, Searcher
from ...utils import as_uncertainty_dict
from ..base import LCIBackend
from ..utils import check_exchange
from .proxies import Activity
from .schema import ActivityDataset, ExchangeDataset
from .utils import (
    dict_as_activitydataset,
    dict_as_exchangedataset,
    retupleize_geo_strings,
)
from bw_processing import create_calculation_package, clean_datapackage_name
from peewee import fn, DoesNotExist
import datetime
import itertools
import pickle
import pprint
import pyprind
import random
import sqlite3
import warnings


# AD = ActivityDataset
# out_a = AD.alias()
# in_a = AD.alias()
# qs = AD.select().where(AD.key == (out_a.select(in_a.key).join(ED, on=(ED.output == out_a.key)).join(in_a, on=(ED.input_ == in_a.key)).where((ED.type_ == 'technosphere') & (in_a.product == out_a.product)))

_VALID_KEYS = {"location", "name", "product", "type"}


class SQLiteBackend(LCIBackend):
    backend = "sqlite"

    def __init__(self, *args, **kwargs):
        super(SQLiteBackend, self).__init__(*args, **kwargs)

        self._filters = {}
        self._order_by = None

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
        return Activity(
            self._get_queryset(filters=False).where(ActivityDataset.code == code).get()
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
        mapping.add(data.keys())

        if preferences.get("allow incomplete imports"):
            mapping.add(
                {
                    exc["input"]
                    for ds in data.values()
                    for exc in ds.get("exchanges", [])
                }
            )
            mapping.add(
                {
                    exc.get("output")
                    for ds in data.values()
                    for exc in ds.get("exchanges", [])
                    if exc.get("output")
                }
            )

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
            from ...parameters import (
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
            data, input_database, input_code, output_database, output_code = row
            # Modify ``dependents`` in place
            if input_database != output_database:
                dependents.add(input_database)
            data = pickle.loads(bytes(data))
            check_exchange(data)
            try:
                yield {
                    **as_uncertainty_dict(data),
                    "row": mapping[(input_database, input_code)],
                    "col": mapping[(output_database, output_code)],
                    "flip": flip,
                }
            except KeyError:
                raise UnknownObject(
                    (
                        "Exchange between {} and {} is invalid "
                        "- one of these objects is unknown (i.e. doesn't exist "
                        "as a process dataset)"
                    ).format(
                        (input_database, input_code), (output_database, output_code)
                    )
                )

    def process(self):
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
        resources = []
        dependents = set()

        # Create geomapping array, from dataset interger ids to locations
        inv_mapping_qs = ActivityDataset.select(
            ActivityDataset.location, ActivityDataset.code
        ).where(
            ActivityDataset.database == self.name, ActivityDataset.type == "process"
        )
        resources.append(
            {
                "name": clean_datapackage_name(
                    self.name + " inventory geomapping matrix"
                ),
                "matrix": "inv_mapping_matrix",
                "path": "inv_geomapping_matrix.npy",
                "data": (
                    {
                        "row": mapping[(self.name, row["code"])],
                        "col": geomapping[
                            retupleize_geo_strings(row["location"])
                            or config.global_location
                        ],
                        "amount": 1,
                    }
                    for row in inv_mapping_qs.dicts()
                ),
                "nrows": inv_mapping_qs.count(),
            }
        )

        BIOSPHERE_SQL = """SELECT data, input_database, input_code, output_database, output_code
                FROM exchangedataset
                WHERE output_database = ?
                AND type = 'biosphere'
        """
        resources.append(
            {
                "name": clean_datapackage_name(self.name + " biosphere matrix"),
                "matrix": "biosphere_matrix",
                "path": "biosphere_matrix.npy",
                "data": self.exchange_data_iterator(BIOSPHERE_SQL, dependents),
            }
        )

        # Figure out when the production exchanges are implicit
        implicit_production = (
            {"row": mapping[(self.name, x[0])], "amount": 1}
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

        TECHNOSPHERE_POSITIVE_SQL = """SELECT data, input_database, input_code, output_database, output_code
                FROM exchangedataset
                WHERE output_database = ?
                AND type IN ('production', 'substitution', 'generic production')
        """
        TECHNOSPHERE_NEGATIVE_SQL = """SELECT data, input_database, input_code, output_database, output_code
                FROM exchangedataset
                WHERE output_database = ?
                AND type IN ('technosphere', 'generic consumption')
        """

        resources.append(
            {
                "name": clean_datapackage_name(self.name + " technosphere matrix"),
                "matrix": "technosphere_matrix",
                "path": "technosphere_matrix.npy",
                "data": itertools.chain(
                    self.exchange_data_iterator(TECHNOSPHERE_NEGATIVE_SQL, dependents),
                    self.exchange_data_iterator(TECHNOSPHERE_POSITIVE_SQL, dependents),
                    implicit_production,
                ),
            }
        )

        create_calculation_package(
            name=self.filename_processed(),
            resources=resources,
            path=self.dirpath_processed(),
            compress=True,
        )

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
