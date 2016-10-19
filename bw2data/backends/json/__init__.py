# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ... import config
from ...project import projects
from ...sqlite import create_database
from .database import JSONDatabase
from .proxies import Activity, Exchange
from .schema import MappingDataset
from peewee import SqliteDatabase
import os

sqlite3_mapping_db = create_database(
    os.path.join(projects.dir, "lci", "json-mapping.db"),
    [MappingDataset]
)
config.sqlite3_databases.append((
    os.path.join("lci", "json-mapping.db"),
    sqlite3_mapping_db,
    [MappingDataset]
))
