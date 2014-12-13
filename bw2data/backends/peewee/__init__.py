from peewee import SqliteDatabase
from ... import config
import os

sqlite3_db = SqliteDatabase(os.path.join(config.request_dir(u"peewee"), u"lci.db"))

from .schema import ActivityDataset, ExchangeDataset
from .database import SQLiteBackend

try:
    sqlite3_db.create_tables([ActivityDataset, ExchangeDataset])
except:
    pass
