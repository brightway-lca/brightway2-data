from . import sqlite3_db
from ..base import LCIBackend
from .proxies import Activity
from .schema import ActivityDataset, ExchangeDataset
from .utils import dict_as_activity, keyjoin, keysplit
from peewee import fn
import cPickle as pickle
import progressbar


# qs = AD.select().where(AD.key == (out_a.select(in_a.key).join(ED, on=(ED.output == out_a.key)).join(in_a, on=(ED.input_ == in_a.key)).where((ED.type_ == 'technosphere') & (in_a.product == out_a.product)))


class SQLiteBackend(LCIBackend):
    backend = u"sqlite"

    def load(self, *args, **kwargs):
        # Should not be used, in general; relatively slow
        activities = [pickle.loads(str(obj[u'data'])) for obj in
            ActivityDataset.select(ActivityDataset.data)
            .where(ActivityDataset.database == self.name).dicts()
        ]

        activities = {(o[u'database'], o[u'code']): o for o in activities}
        for o in activities.values():
            o[u'exchanges'] = []

        exchange_qs = (ExchangeDataset.select(ExchangeDataset.data)
            .where(ExchangeDataset.database == self.name).dicts())

        for exc in exchange_qs:
            exc = pickle.loads(str(exc[u'data']))
            activities[exc[u'output']][u'exchanges'].append(exc)
        return activities

    def random(self):
        return Activity(ActivityDataset.select().where(ActivityDataset.database == self.name).order_by(fn.Random()).get())

    def get(self, code):
        return Activity(ActivityDataset.select().where(ActivityDataset.key == self._make_key(code)).get())

    def __len__(self):
        return ActivityDataset.select().where(ActivityDataset.database == self.name).count()

    def _make_key(self, obj):
        if isinstance(obj, basestring):
            obj = (self.name, obj)
        return keyjoin(obj)

    def _drop_indices(self):
        with sqlite3_db.transaction():
            sqlite3_db.execute_sql('DROP INDEX IF EXISTS "activitydataset_key"')
            sqlite3_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_database"')
            sqlite3_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_input"')
            sqlite3_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_output"')

    def _add_indices(self):
        with sqlite3_db.transaction():
            sqlite3_db.execute_sql('CREATE UNIQUE INDEX "activitydataset_key" ON "activitydataset" ("key")')
            sqlite3_db.execute_sql('CREATE INDEX "exchangedataset_database" ON "exchangedataset" ("database")')
            sqlite3_db.execute_sql('CREATE INDEX "exchangedataset_input" ON "exchangedataset" ("input")')
            sqlite3_db.execute_sql('CREATE INDEX "exchangedataset_output" ON "exchangedataset" ("output")')

    def _efficient_write_many_data(self, data):
        self._drop_indices()
        sqlite3_db.autocommit = False
        try:
            sqlite3_db.begin()
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
                    exchange[u'output'] = key
                    # TODO: Raise error if 'input' missing?
                    exchanges.append({
                        'input': self._make_key(exchange[u"input"]),
                        'database': key[0],
                        "output": self._make_key(key),
                        "type": exchange.get(u"type"),
                        "data": pickle.dumps(exchange, protocol=pickle.HIGHEST_PROTOCOL)
                    })

                    # Query gets passed as INSERT INTO x VALUES ('?', '?'...)
                    # SQLite3 has a limit of 999 variables,
                    # So 5 fields * 150 is under the limit
                    # Otherwise get the following:
                    # eewee.OperationalError: too many SQL variables
                    if len(exchanges) > 150:
                        ExchangeDataset.insert_many(exchanges).execute()
                        exchanges = []

                ds = {k: v for k, v in ds.items() if k != u"exchanges"}
                ds[u"database"] = key[0]
                ds[u"code"] = key[1]

                activities.append(dict_as_activity(ds))

                if len(activities) > 150:
                    ActivityDataset.insert_many(activities).execute()
                    activities = []

                pbar.update(index)

            pbar.finish()

            if activities:
                ActivityDataset.insert_many(activities).execute()
            if exchanges:
                ExchangeDataset.insert_many(exchanges).execute()
            sqlite3_db.commit()
        except:
            sqlite3_db.rollback()
            raise
        finally:
            sqlite3_db.autocommit = True
            self._add_indices()
