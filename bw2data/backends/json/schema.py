# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from peewee import Model, TextField


class MappingDataset(Model):
    code = TextField()
    filepath = TextField()
