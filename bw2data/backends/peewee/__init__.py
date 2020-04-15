# -*- coding: utf-8 -*-
from ... import config
from ...sqlite import PickleField, SubstitutableDatabase
from ...project import projects
from peewee import SqliteDatabase
import os

from .schema import ActivityDataset, ExchangeDataset

sqlite3_lci_db = SubstitutableDatabase(
    os.path.join(projects.dir, "lci", "databases.db"),
    [ActivityDataset, ExchangeDataset]
)

from .proxies import Activity, Exchange
from .database import SQLiteBackend

config.sqlite3_databases.append((
    os.path.join("lci", "databases.db"),
    sqlite3_lci_db,
))
