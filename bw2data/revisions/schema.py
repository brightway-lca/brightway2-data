from . import sqlite3_revisions_db
from ..backends.peewee import PickleField
import datetime
import peewee


class Revision(peewee.Model):
    data = PickleField()
    description = peewee.TextField()
    number = peewee.IntegerField()
    key_frame = peewee.BooleanField()
    modified = peewee.DateTimeField(default=datetime.datetime.now)
    key = peewee.TextField(index=True)

    class Meta(object):
        database = sqlite3_revisions_db
