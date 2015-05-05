# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from peewee import SqliteDatabase, BlobField, Model, TextField
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

MAGIC_JOIN_CHARACTER = "⊡"

def keyjoin(tpl):
    return MAGIC_JOIN_CHARACTER.join(tpl)


def keysplit(key):
    return tuple(key.split(MAGIC_JOIN_CHARACTER))


class TupleField(TextField):
    def db_value(self, value):
        return keyjoin(value)

    def python_value(self, vlaue):
        return keysplit(value)


class PickleField(BlobField):
    def db_value(self, value):
        return super(PickleField, self).db_value(
            pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        )

    def python_value(self, value):
        return pickle.loads(str(value))


def create_database(filepath, tables):
    db = SqliteDatabase(filepath)
    for table in tables:
        table._meta.database = db
    db.create_tables(
        list(tables),
        safe=True
    )
    return db


# def change_database_path(db, filepath, tables):
#     db.database = filepath
