# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ... import config
from ...sqlite import create_database, PickleField
from ...project import projects
from peewee import SqliteDatabase
import os

from .schema import ActivityDataset, ExchangeDataset

sqlite3_lci_db = create_database(
    os.path.join(projects.dir, "lci", "databases.db"),
    (ActivityDataset, ExchangeDataset)
)

from .proxies import Activity, Exchange
from .database import SQLiteBackend

config.sqlite3_databases.append((
    os.path.join("lci", "databases.db"),
    sqlite3_lci_db,
    True
))
