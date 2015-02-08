from . import sqlite3_db, PickleField
from peewee import Model, TextField, BlobField


class ActivityDataset(Model):
    data = PickleField()
    key = TextField(index=True, unique=True)
    database = TextField()
    location = TextField(null=True)
    name = TextField(null=True)
    product = TextField(null=True)
    type = TextField(null=True)

    class Meta(object):
        database = sqlite3_db()


class ExchangeDataset(Model):
    data = PickleField()
    input = TextField(index=True)
    output = TextField(index=True)
    database = TextField(index=True)
    type = TextField()

    class Meta(object):
        database = sqlite3_db()
