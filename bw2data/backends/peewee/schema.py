# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ...sqlite import PickleField, TupleField
from peewee import Model, TextField, BlobField


class ActivityDataset(Model):
    data = PickleField()
    key = TupleField(index=True, unique=True)
    database = TextField()
    location = TextField(null=True)
    name = TextField(null=True)
    product = TextField(null=True)
    type = TextField(null=True)


class ExchangeDataset(Model):
    data = PickleField()
    input = TupleField(index=True)
    output = TupleField(index=True)
    database = TextField(index=True)
    type = TextField()
