from peewee import Model, TextField, BlobField
from . import sqlite3_db


# class ActivityDataset(Model):
#     data = BlobField()
#     key = TextField(null=False, unique=True, index=True)
#     database = TextField(index=True)
#     location = TextField(index=True, null=True)
#     name = TextField(index=True, null=True)
#     product = TextField(index=True, null=True)
#     type_ = TextField(index=True, null=True)
#     unit = TextField(index=True, null=True)

#     class Meta(object):
#         database = sqlite3_db


# class ExchangeDataset(Model):
#     data = BlobField()
#     input_ = TextField(null=False, index=True)
#     output = TextField(null=False, index=True)
#     database = TextField(null=False, index=True)
#     type_ = TextField(index=True)

#     class Meta(object):
#         database = sqlite3_db


class ActivityDataset(Model):
    data = BlobField()
    key = TextField(index=True, null=False, unique=True)
    database = TextField()
    location = TextField(null=True)
    name = TextField(null=True)
    product = TextField(null=True)

    class Meta(object):
        database = sqlite3_db


class ExchangeDataset(Model):
    data = BlobField()
    input_ = TextField(null=False)
    output = TextField(null=False)
    type_ = TextField()

    class Meta(object):
        database = sqlite3_db
