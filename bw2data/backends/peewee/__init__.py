from ... import config
from peewee import SqliteDatabase
import os


class DB(object):
    def __init__(self):
        self.reset(initial=True)

    def reset(self, initial=False):
        self.path = os.path.join(
            config.request_dir(u"peewee"),
            u"lci.db"
        )
        self.db = SqliteDatabase(self.path)
        if not initial:
            ActivityDataset._meta.database = self.db
            ExchangeDataset._meta.database = self.db
            self.create()

    def create(self):
        self.db.create_tables(
            [ActivityDataset, ExchangeDataset],
            safe=True
        )

    def __call__(self):
        return self.db


sqlite3_db = DB()

from .fields import PickleField
from .schema import ActivityDataset, ExchangeDataset
from .proxies import Activity
from .database import SQLiteBackend

sqlite3_db.create()
