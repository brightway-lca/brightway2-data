from . import sqlite3_db
from ..base import LCIBackend
from .proxies import Activity
from .schema import ActivityDataset, ExchangeDataset
from peewee import fn
import cPickle as pickle
import progressbar


class SQLiteBackend(LCIBackend):
    backend = u"sqlite"

    def random(self):
        return Activity(ActivityDataset.select().where(ActivityDataset.database == self.name).order_by(fn.Random()).get())

    def get(self, code):
        return Activity(ActivityDataset.select().where(ActivityDataset.key == self._make_key(code)).get())

    def __len__(self):
        return ActivityDataset.select().where(ActivityDataset.database == self.name).count()

    def _make_key(self, obj):
        if isinstance(obj, basestring):
            obj = (self.name, obj)
        return u":".join(obj)

    def _drop_indices(self):
        with sqlite3_db.transaction():
            sqlite3_db.execute_sql('DROP INDEX IF EXISTS "activitydataset_key"')
            sqlite3_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_database"')
            sqlite3_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_input_"')
            sqlite3_db.execute_sql('DROP INDEX IF EXISTS "exchangedataset_output"')

    def _add_indices(self):
        with sqlite3_db.transaction():
            sqlite3_db.execute_sql('CREATE UNIQUE INDEX "activitydataset_key" ON "activitydataset" ("key")')
            sqlite3_db.execute_sql('CREATE INDEX "exchangedataset_database" ON "exchangedataset" ("database")')
            sqlite3_db.execute_sql('CREATE INDEX "exchangedataset_input_" ON "exchangedataset" ("input_")')
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
                        'input_': self._make_key(exchange[u"input"]),
                        'database': key[0],
                        "output": self._make_key(key),
                        "type_": exchange.get(u"type"),
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

                activities.append({
                    "key": self._make_key(key),
                    "database": key[0],
                    "location": ds.get(u"location"),
                    "name": ds.get(u"name"),
                    "product": ds.get(u"reference product"),
                    "data": pickle.dumps(ds, protocol=pickle.HIGHEST_PROTOCOL)
                })

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
