# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import sqlite3_lci_db
from ... import mapping, geomapping, config, databases, preferences
from ...errors import UntypedExchange, InvalidExchange, UnknownObject, WrongDatabase
from ...project import writable_project
from ...search import IndexManager, Searcher
from ...sqlite import Key
from ...utils import MAX_INT_32, TYPE_DICTIONARY
from ..base import LCIBackend
from .proxies import Activity
from .schema import ActivityDataset, ExchangeDataset
from .utils import dict_as_activitydataset
from peewee import fn, DoesNotExist
import itertools
import datetime
import numpy as np
import pyprind
import sqlite3
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle


# AD = ActivityDataset
# out_a = AD.alias()
# in_a = AD.alias()
# qs = AD.select().where(AD.key == (out_a.select(in_a.key).join(ED, on=(ED.output == out_a.key)).join(in_a, on=(ED.input_ == in_a.key)).where((ED.type_ == 'technosphere') & (in_a.product == out_a.product)))

_VALID_KEYS = {'location', 'name', 'product', 'type'}


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

    @property
    def _searchable(self):
        return databases.get(self.name, {}).get('searchable', True)

    def _get_queryset(self, random=False, filters=True):
        qs = ActivityDataset.select().where(
            ActivityDataset.database == self.name)
        if filters:
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
            assert isinstance(filters, dict), "Filter must be a dictionary"
            for key in filters:
                assert key in _VALID_KEYS, \
                    "Filter key {} is invalid".format(key)
                self._filters = filters
        return self

    def _get_order_by(self):
        return self._order_by

    def _set_order_by(self, field):
        if not field:
            self._order_by = None
        else:
            assert field in _VALID_KEYS, \
                "order_by field {} is invalid".format(field)
            self._order_by = field
        return self

    # Public API

    filters = property(_get_filters, _set_filters)
    order_by = property(_get_order_by, _set_order_by)

    def random(self, filters=True):
        try:
            return Activity(self._get_queryset(random=True, filters=filters
                            ).get())
        except DoesNotExist:
            warnings.warn("This database is empty")
            return None

    def get(self, code):
        return Activity(
            self._get_queryset(filters=False).where(
                ActivityDataset.key == Key(self.name, code)
            ).get()
        )

    ### Data management
    ###################

    # Private methods

    def _drop_indices(self):
        with sqlite3_lci_db.transaction():
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "activitydataset_key"')
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_database"')
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_input"')
            sqlite3_lci_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_output"')

    def _add_indices(self):
        with sqlite3_lci_db.transaction():
            sqlite3_lci_db.execute_sql('CREATE UNIQUE INDEX "activitydataset_key" ON "activitydataset" ("key")')
            sqlite3_lci_db.execute_sql('CREATE INDEX "exchangedataset_database" ON "exchangedataset" ("database")')
            sqlite3_lci_db.execute_sql('CREATE INDEX "exchangedataset_input" ON "exchangedataset" ("input")')
            sqlite3_lci_db.execute_sql('CREATE INDEX "exchangedataset_output" ON "exchangedataset" ("output")')

    def _efficient_write_many_data(self, data, indices=True):
        be_complicated = len(data) >= 100 and indices
        if be_complicated:
            self._drop_indices()
        sqlite3_lci_db.autocommit = False
        try:
            sqlite3_lci_db.begin()
            self.delete()
            exchanges, activities = [], []

            if not getattr(config, "is_test", None):
                pbar = pyprind.ProgBar(
                    len(data),
                    title="Writing activities to SQLite3 database:",
                    monitor=True
                )

            for index, (key, ds) in enumerate(data.items()):
                for exchange in ds.get('exchanges', []):
                    if 'input' not in exchange or 'amount' not in exchange:
                        raise InvalidExchange
                    if 'type' not in exchange:
                        raise UntypedExchange
                    exchange['output'] = key
                    exchanges.append({
                        'input': exchange["input"],
                        'database': key[0],
                        "output": key,
                        "type": exchange["type"],
                        "data": exchange
                    })

                    # Query gets passed as INSERT INTO x VALUES ('?', '?'...)
                    # SQLite3 has a limit of 999 variables,
                    # So 5 fields * 150 is under the limit
                    # Otherwise get the following:
                    # peewee.OperationalError: too many SQL variables
                    if len(exchanges) > 150:
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
                    pbar.update()
            if not getattr(config, "is_test", None):
                print(pbar)

            if activities:
                ActivityDataset.insert_many(activities).execute()
            if exchanges:
                ExchangeDataset.insert_many(exchanges).execute()
            sqlite3_lci_db.commit()
        except:
            sqlite3_lci_db.rollback()
            raise
        finally:
            sqlite3_lci_db.autocommit = True
            if be_complicated:
                self._add_indices()

    # Public API

    @writable_project
    def write(self, data, process=True):
        """Write ``data`` to database.

        This deletes all existing data for this database."""
        self.register()
        wrong_database = {key[0] for key in data}.difference({self.name})
        if wrong_database:
            raise WrongDatabase("Can't write activities in databases {} to database {}".format(
                                wrong_database, self.name))


        databases[self.name]['number'] = len(data)
        databases.set_modified(self.name)
        mapping.add(data.keys())

        if preferences.get('allow incomplete imports'):
            mapping.add({exc['input'] for ds in data.values() for exc in ds.get('exchanges', [])})
            mapping.add({exc.get('output') for ds in data.values()
                                           for exc in ds.get('exchanges', [])
                                           if exc.get('output')})

        geomapping.add({x["location"] for x in data.values() if
                       x.get("location")})
        if data:
            try:
                self._efficient_write_many_data(data)
            except:
                # Purge all data from database, then reraise
                self.delete()
                raise

        if self._searchable:
            IndexManager().delete_database(self.name)
            IndexManager().add_datasets(self)

        if process:
            self.process()

    def load(self, *args, **kwargs):
        # Should not be used, in general; relatively slow
        activities = [obj['data'] for obj in
            self._get_queryset().dicts()
        ]

        activities = {(o['database'], o['code']): o for o in activities}
        for o in activities.values():
            o['exchanges'] = []

        exchange_qs = (ExchangeDataset.select(ExchangeDataset.data)
            .where(ExchangeDataset.database == self.name).dicts())

        for exc in exchange_qs:
            try:
                activities[exc['data']['output']]['exchanges'].append(exc['data'])
            except KeyError:
                # This exchange not in the reduced set of activities returned
                # by _get_queryset
                pass
        return activities

    def new_activity(self, code, **kwargs):
        obj = Activity()
        obj['database'] = self.name
        obj['code'] = str(code)
        obj['location'] = config.global_location
        obj.update(kwargs)
        return obj

    @writable_project
    def make_searchable(self):
        if self._searchable:
            print("This database is already searchable")
            return
        databases[self.name]['searchable'] = True
        databases.flush()
        IndexManager().add_datasets(self)

    @writable_project
    def make_unsearchable(self):
        databases[self.name]['searchable'] = False
        databases.flush()
        IndexManager().delete_database(self.name)

    @writable_project
    def delete(self):
        """Delete all data from SQLite database and Whoosh index"""
        ActivityDataset.delete().where(ActivityDataset.database== self.name).execute()
        ExchangeDataset.delete().where(ExchangeDataset.database== self.name).execute()
        IndexManager().delete_database(self.name)

    def process(self):
        """
Process inventory documents to NumPy structured arrays.

Use a raw SQLite3 cursor instead of Peewee for a ~2 times speed advantage.

        """
        try:
            from bw2regional import faces
        except ImportError:
            faces = None

        num_exchanges = ExchangeDataset.select().where(ExchangeDataset.database == self.name).count()
        num_processes = ActivityDataset.select().where(
            ActivityDataset.database == self.name,
            ActivityDataset.type == "process"
        ).count()

        # Create geomapping array
        arr = np.zeros((num_processes, ), dtype=self.dtype_fields_geomapping + self.base_uncertainty_fields)

        topo_mapping = []

        for index, row in enumerate(ActivityDataset.select(
                ActivityDataset.location, ActivityDataset.key
                ).where(
                ActivityDataset.database == self.name,
                ActivityDataset.type == "process"
                ).order_by(ActivityDataset.key).dicts()):

            if faces is not None:
                for face_id in faces.get(row['location'], []):
                    topo_mapping.append((row['key'], face_id))

            arr[index] = (
                mapping[row['key']],
                geomapping[row['location'] or config.global_location],
                MAX_INT_32, MAX_INT_32,
                0, 1, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, False
            )

        with open(self.filepath_geomapping(), "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

        if faces is not None:
            arr = np.zeros((len(topo_mapping), ), dtype=self.dtype_fields_geomapping + self.base_uncertainty_fields)

            for index, obj in enumerate(topo_mapping):
                key, face = obj
                arr[index] = (
                    mapping[key],
                    geomapping[face],
                    MAX_INT_32, MAX_INT_32,
                    0, 1, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, False
                )

            with open(self.filepath_geomapping().replace("geomapping", "topomapping"), "wb") as f:
                pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

        missing_production_keys = [x[0] for x in ActivityDataset.select(
            ActivityDataset.key).where(
                ActivityDataset.database == self.name,
                ActivityDataset.type == "process",
                ~(ActivityDataset.key << ExchangeDataset.select(
                    ExchangeDataset.output).where(
                    ExchangeDataset.database == self.name,
                    ExchangeDataset.type == 'production'))
            ).tuples()]

        arr = np.zeros((num_exchanges + len(missing_production_keys), ), dtype=self.dtype)

        # Using raw sqlite3 for ~2x speed boost
        connection = sqlite3.connect(sqlite3_lci_db.database)
        cursor = connection.cursor()
        SQL = "SELECT data FROM exchangedataset WHERE database = ? ORDER BY input, output"

        dependents = set()
        found_exchanges = False

        for index, row in enumerate(cursor.execute(SQL, (self.name,))):
            data = pickle.loads(bytes(row[0]))

            if "type" not in data:
                raise UntypedExchange
            if "amount" not in data or "input" not in data:
                raise InvalidExchange
            if np.isnan(data['amount']) or np.isinf(data['amount']):
                raise ValueError("Invalid amount in exchange {}".format(data))

            found_exchanges = True

            dependents.add(data["input"][0])

            try:
                arr[index] = (
                    mapping[data["input"]],
                    mapping[data["output"]],
                    MAX_INT_32,
                    MAX_INT_32,
                    TYPE_DICTIONARY[data["type"]],
                    data.get("uncertainty type", 0),
                    data["amount"],
                    data["amount"] \
                        if data.get("uncertainty type", 0) in (0,1) \
                        else data.get("loc", np.NaN),
                    data.get("scale", np.NaN),
                    data.get("shape", np.NaN),
                    data.get("minimum", np.NaN),
                    data.get("maximum", np.NaN),
                    data["amount"] < 0
                )
            except KeyError:
                raise UnknownObject(("Exchange between {} and {} is invalid "
                    "- one of these objects is unknown (i.e. doesn't exist "
                    "as a process dataset)"
                    ).format(data["input"], data["output"])
                )

        # If exchanges were found, start inserting rows at len(exchanges) + 1
        index = index + 1 if found_exchanges else 0

        for index, key in zip(itertools.count(index), missing_production_keys):
            arr[index] = (
                mapping[key], mapping[key],
                MAX_INT_32, MAX_INT_32, TYPE_DICTIONARY["production"],
                0, 1, 1, np.NaN, np.NaN, np.NaN, np.NaN, False
            )

        databases[self.name]['depends'] = sorted(dependents.difference({self.name}))
        databases[self.name]['processed'] = datetime.datetime.now().isoformat()
        databases.flush()

        with open(self.filepath_processed(), "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

    def search(self, string, *args, **kwargs):
        """Search this database for ``string``.

        The searcher include the following fields:

        * name
        * comment
        * categories
        * location
        * reference product

        ``string`` can include wild cards, e.g. ``trans*``.

        Optional keyword arguments:

        * ``limit``: Number of results to return.
        * ``facet``: Field to facet results. Must be one of ``name``, ``product``, ``categories``, ``location``, or ``database``.
        * ``proxy``: Return ``Activity`` proxies instead of raw Whoosh documents. Default is ``True``.

        Returns a list of ``Activity`` datasets."""
        kwargs['database'] = self.name
        with Searcher() as s:
            results = s.search(string, **kwargs)
        return results

    def graph_technosphere(self, filename=None, **kwargs):
        from bw2analyzer.matrix_grapher import SparseMatrixGrapher
        from bw2calc import LCA
        lca = LCA({self.random(): 1})
        lca.lci()

        smg = SparseMatrixGrapher(lca.technosphere_matrix)
        return smg.ordered_graph(filename, **kwargs)
