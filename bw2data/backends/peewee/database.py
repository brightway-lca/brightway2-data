from __future__ import print_function
from . import sqlite3_db
from ... import mapping, geomapping, config, databases
from ...errors import UntypedExchange, InvalidExchange, UnknownObject
from ...search import IndexManager, Searcher
from ...utils import MAX_INT_32, TYPE_DICTIONARY
from ..base import LCIBackend
from .proxies import Activity
from .schema import ActivityDataset, ExchangeDataset
from .utils import dict_as_activity, keyjoin, keysplit
from peewee import fn
import cPickle as pickle
import datetime
import itertools
import numpy as np
import progressbar
import sqlite3

# AD = ActivityDataset
# out_a = AD.alias()
# in_a = AD.alias()
# qs = AD.select().where(AD.key == (out_a.select(in_a.key).join(ED, on=(ED.output == out_a.key)).join(in_a, on=(ED.input_ == in_a.key)).where((ED.type_ == 'technosphere') & (in_a.product == out_a.product)))

_VALID_KEYS = {'location', 'name', 'product', 'type'}


class SQLiteBackend(LCIBackend):
    backend = u"sqlite"

    def __init__(self, *args, **kwargs):
        super(SQLiteBackend, self).__init__(*args, **kwargs)
        if self.name in databases:
            self._searchable = databases[self.name].get('searchable', False)
        else:
            self._searchable = False
        self._filters = {}
        self._order_by = None

    ### Iteration, filtering, and ordering

    def __iter__(self):
        for ds in self._get_queryset():
            yield Activity(ds)

    def _get_queryset(self):
        qs = ActivityDataset.select().where(
            ActivityDataset.database == self.name)
        for key, value in self.filters.items():
            qs = qs.where(getattr(ActivityDataset, key) == value)
        if self.order_by:
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
            assert isinstance(filters, dict), u"Filter must be a dictionary"
            for key in filters:
                assert key in _VALID_KEYS, \
                    u"Filter key {} is invalid".format(key)
                self._filters = filters
        return self

    def _get_order_by(self):
        return self._order_by

    def _set_order_by(self, field):
        if not field:
            self._order_by = None
        else:
            assert field in _VALID_KEYS, \
                u"order_by field {} is invalid".format(field)
            self._order_by = field
        return self

    filters = property(_get_filters, _set_filters)
    order_by = property(_get_order_by, _set_order_by)

    ### Data management

    def write(self, data, process=True):
        """Write ``data`` to database.

        This deletes all exiting data for this database."""
        self.assert_registered()
        self.metadata[self.name]['modified'] = datetime.datetime.now().isoformat()
        self.metadata[self.name]['number'] = len(data)
        self.metadata.flush()
        mapping.add(data.keys())
        geomapping.add({x[u"location"] for x in data.values() if
                       x.get(u"location", False)})
        if data:
            self._efficient_write_many_data(data)

        if self._searchable:
            IndexManager().delete_database(self.name)
            IndexManager().add_datasets(self)

        if process:
            self.process()

    def load(self, *args, **kwargs):
        # Should not be used, in general; relatively slow
        activities = [obj[u'data'] for obj in
            ActivityDataset.select(ActivityDataset.data)
            .where(ActivityDataset.database == self.name).dicts()
        ]

        activities = {(o[u'database'], o[u'code']): o for o in activities}
        for o in activities.values():
            o[u'exchanges'] = []

        exchange_qs = (ExchangeDataset.select(ExchangeDataset.data)
            .where(ExchangeDataset.database == self.name).dicts())

        for exc in exchange_qs:
            activities[exc[u'data'][u'output']][u'exchanges'].append(exc['data'])
        return activities

    def random(self):
        return Activity(ActivityDataset.select().where(ActivityDataset.database == self.name).order_by(fn.Random()).get())

    def get(self, code):
        return Activity(ActivityDataset.select().where(ActivityDataset.key == self._make_key(code)).get())

    def new_activity(self, code, **kwargs):
        obj = Activity()
        obj[u'database'] = self.name
        obj[u'code'] = unicode(code)
        obj[u'location'] = config.global_location
        obj.update(**kwargs)
        return obj

    def make_searchable(self):
        if self._searchable:
            print(u"This database is already searchable")
            return
        databases[self.name][u'searchable'] = self._searchable = True
        databases.flush()
        IndexManager().add_datasets(self)

    def make_unsearchable(self):
        databases[self.name][u'searchable'] = self._searchable = False
        databases.flush()
        IndexManager().delete_database(self.name)

    def __len__(self):
        return self._get_queryset().count()

    def _make_key(self, obj):
        if isinstance(obj, basestring):
            obj = (self.name, obj)
        return keyjoin(obj)

    def _drop_indices(self):
        with sqlite3_db().transaction():
            sqlite3_db().execute_sql('DROP INDEX IF EXISTS "activitydataset_key"')
            sqlite3_db().execute_sql('DROP INDEX IF EXISTS "exchangedataset_database"')
            sqlite3_db().execute_sql('DROP INDEX IF EXISTS "exchangedataset_input"')
            sqlite3_db().execute_sql('DROP INDEX IF EXISTS "exchangedataset_output"')

    def _add_indices(self):
        with sqlite3_db().transaction():
            sqlite3_db().execute_sql('CREATE UNIQUE INDEX "activitydataset_key" ON "activitydataset" ("key")')
            sqlite3_db().execute_sql('CREATE INDEX "exchangedataset_database" ON "exchangedataset" ("database")')
            sqlite3_db().execute_sql('CREATE INDEX "exchangedataset_input" ON "exchangedataset" ("input")')
            sqlite3_db().execute_sql('CREATE INDEX "exchangedataset_output" ON "exchangedataset" ("output")')

    def _efficient_write_many_data(self, data, indices=True):
        be_complicated = len(data) >= 100 and indices
        if be_complicated:
            self._drop_indices()
        sqlite3_db().autocommit = False
        try:
            sqlite3_db().begin()
            self.delete()
            exchanges, activities = [], []

            widgets = [
                progressbar.SimpleProgress(sep="/"), " (",
                progressbar.Percentage(), ') ',
                progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
                progressbar.ETA()
            ]
            pbar = progressbar.ProgressBar(
                widgets=widgets,
                maxval=len(data)
            ).start()

            for index, (key, ds) in enumerate(data.items()):
                for exchange in ds.get(u'exchanges', []):
                    if 'input' not in exchange or 'amount' not in exchange:
                        raise InvalidExchange
                    if 'type' not in exchange:
                        raise UntypedExchange
                    exchange[u'output'] = key
                    # TODO: Raise error if 'input' missing?
                    exchanges.append({
                        'input': self._make_key(exchange[u"input"]),
                        'database': key[0],
                        "output": self._make_key(key),
                        "type": exchange.get(u"type"),
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

                ds = {k: v for k, v in ds.items() if k != u"exchanges"}
                ds[u"database"] = key[0]
                ds[u"code"] = key[1]

                activities.append(dict_as_activity(ds))

                if len(activities) > 125:
                    ActivityDataset.insert_many(activities).execute()
                    activities = []

                pbar.update(index)

            pbar.finish()

            if activities:
                ActivityDataset.insert_many(activities).execute()
            if exchanges:
                ExchangeDataset.insert_many(exchanges).execute()
            sqlite3_db().commit()
        except:
            sqlite3_db().rollback()
            raise
        finally:
            sqlite3_db().autocommit = True
            if be_complicated:
                self._add_indices()

    def delete(self):
        """Delete all data from SQLite database and Whoosh index"""
        ActivityDataset.delete().where(ActivityDataset.database==self.name).execute()
        ExchangeDataset.delete().where(ExchangeDataset.database==self.name).execute()
        IndexManager().delete_database(self.name)

    def process(self):
        """
Process inventory documents to NumPy structured arrays.

Use a raw SQLite3 cursor instead of Peewee for a ~2 times speed advantage.

        """
        num_exchanges = ExchangeDataset.select().where(ExchangeDataset.database == self.name).count()
        num_processes = ActivityDataset.select().where(
            ActivityDataset.database == self.name,
            ActivityDataset.type == u"process"
        ).count()

        # Create geomapping array
        arr = np.zeros((num_processes, ), dtype=self.dtype_fields_geomapping + self.base_uncertainty_fields)

        for index, row in enumerate(ActivityDataset.select(
                ActivityDataset.location, ActivityDataset.key
                ).where(
                ActivityDataset.database == self.name,
                ActivityDataset.type == u"process"
                ).order_by(ActivityDataset.key).dicts()):
            arr[index] = (
                mapping[keysplit(row['key'])],
                geomapping[row['location'] or config.global_location],
                MAX_INT_32, MAX_INT_32,
                0, 1, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, False
            )

        with open(self.filepath_geomapping(), "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

        missing_production_keys = [x[0] for x in ActivityDataset.select(
            ActivityDataset.key).where(
                ActivityDataset.database == self.name,
                ActivityDataset.type == u"process",
                ~(ActivityDataset.key << ExchangeDataset.select(
                    ExchangeDataset.output).where(
                    ExchangeDataset.database == self.name,
                    ExchangeDataset.type == u'production'))
            ).tuples()]

        arr = np.zeros((num_exchanges + len(missing_production_keys), ), dtype=self.dtype)

        # Using raw sqlite3 for ~2x speed boost
        connection = sqlite3.connect(sqlite3_db.path)
        cursor = connection.cursor()
        SQL = "SELECT data FROM exchangedataset WHERE database = ? ORDER BY input, output"

        dependents = set()
        found_exchanges = False

        for index, row in enumerate(cursor.execute(SQL, (self.name,))):
            data = pickle.loads(str(row[0]))

            if u"type" not in data:
                raise UntypedExchange
            if u"amount" not in data or u"input" not in data:
                raise InvalidExchange

            found_exchanges = True

            dependents.add(data[u"input"][0])

            try:
                arr[index] = (
                    mapping[data[u"input"]],
                    mapping[data[u"output"]],
                    MAX_INT_32,
                    MAX_INT_32,
                    TYPE_DICTIONARY[data[u"type"]],
                    data.get(u"uncertainty type", 0),
                    data[u"amount"],
                    data[u"amount"] \
                        if data.get(u"uncertainty type", 0) in (0,1) \
                        else data.get(u"loc", np.NaN),
                    data.get(u"scale", np.NaN),
                    data.get(u"shape", np.NaN),
                    data.get(u"minimum", np.NaN),
                    data.get(u"maximum", np.NaN),
                    data[u"amount"] < 0
                )
            except KeyError:
                raise UnknownObject((u"Exchange between {} and {} is invalid "
                    "- one of these objects is unknown (i.e. doesn't exist "
                    "as a process dataset)"
                    ).format(data[u"input"], data[u"output"])
                )

        # If exchanges were found, start inserting rows at len(exchanges) + 1
        index = index + 1 if found_exchanges else 0

        for index, key in zip(itertools.count(index), missing_production_keys):
            arr[index] = (
                mapping[keysplit(key)], mapping[keysplit(key)],
                MAX_INT_32, MAX_INT_32, TYPE_DICTIONARY[u"production"],
                0, 1, 1, np.NaN, np.NaN, np.NaN, np.NaN, False
            )

        self.metadata[self.name]['depends'] = list(dependents.difference({self.name}))
        self.metadata[self.name]['processed'] = datetime.datetime.now().isoformat()
        self.metadata.flush()

        with open(self.filepath_processed(), "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

    def search(self, string, *args, **kwargs):
        kwargs['database'] = self.name
        return Searcher().search(string, **kwargs)


    def graph_technosphere(self, filename=None, **kwargs):
        from bw2analyzer.matrix_grapher import SparseMatrixGrapher
        from bw2calc import LCA
        lca = LCA({self.random(): 1})
        lca.lci()

        smg = SparseMatrixGrapher(lca.technosphere_matrix)
        return smg.ordered_graph(filename, **kwargs)
