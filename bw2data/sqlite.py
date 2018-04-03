# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from peewee import SqliteDatabase, BlobField, Model, TextField
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


def create_database(filepath, tables):
    print("Calling create_database:", filepath)
    db = SqliteDatabase(filepath)
    for model in tables:
        model.bind(db, bind_refs=False, bind_backrefs=False)
    # for table in tables:
    #     table._meta.database = db
    with db.connection_context():
        db.create_tables(tables)
    return db
