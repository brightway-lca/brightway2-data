# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ...sqlite import PickleField
from peewee import Model, TextField, BlobField


class ActivityDataset(Model):
    data = PickleField()             # Canonical, except for other C fields
    code = TextField()               # Canonical
    database = TextField()           # Canonical
    location = TextField(null=True)  # Reset from `data`
    name = TextField(null=True)      # Reset from `data`
    product = TextField(null=True)   # Reset from `data`
    type = TextField(null=True)      # Reset from `data`


class ExchangeDataset(Model):
    data = PickleField()           # Canonical, except for other C fields
    input_code = TextField()       # Canonical
    input_database = TextField()   # Canonical
    output_code = TextField()      # Canonical
    output_database = TextField()  # Canonical
    type = TextField()             # Reset from `data`
