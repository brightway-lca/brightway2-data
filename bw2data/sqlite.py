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

MAGIC_JOIN_CHARACTER = "⊡"

def keyjoin(tpl):
    return MAGIC_JOIN_CHARACTER.join(tpl)


def keysplit(key):
    return tuple(key.split(MAGIC_JOIN_CHARACTER))


class Key(object):
    """Can't use lists in peewee expressions.

    See https://github.com/coleifer/peewee/issues/150"""
    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], tuple):
            self.data = args[0]
        else:
            self.data = args

    def __iter__(self):
        return iter(self.data)


class TupleField(TextField):
    def db_value(self, value):
        return keyjoin(value)

    def python_value(self, value):
        return keysplit(value)


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
