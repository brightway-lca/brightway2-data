# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from peewee import SqliteDatabase, BlobField, Model, TextField
from playhouse.shortcuts import RetryOperationalError
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle


class PickleField(BlobField):
    def db_value(self, value):
        return super(PickleField, self).db_value(
            pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        )

    def python_value(self, value):
        return pickle.loads(bytes(value))


class RetryDatabase(RetryOperationalError, SqliteDatabase):
    pass


def create_database(filepath, tables):
    db = RetryDatabase(filepath)
    for table in tables:
        table._meta.database = db
    db.create_tables(
        list(tables),
        safe=True
    )
    db._tables = tables
    return db
