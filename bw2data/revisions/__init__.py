from .. import config
from peewee import SqliteDatabase
import os

sqlite3_revisions_db = SqliteDatabase(os.path.join(config.request_dir(u"peewee"), u"revisions.db"))

from .schema import Revision
from .interface import RevisionsInterface

sqlite3_revisions_db.create_tables([Revision], safe=True)
