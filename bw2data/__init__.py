# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from eight import *

__version__ = (2, 0, "dev")

from .project import projects
from .configuration import config

# Add projects database to global list of sqlite3 databases
config.sqlite3_databases.append((
    "projects.db",
    projects.db,
    False
))

from .utils import set_data_dir
from .meta import (
    databases,
    geomapping,
    mapping,
    methods,
    normalizations,
    preferences,
    weightings,
)

# Add metadata class instances to global list of serialized metadata
config.metadata.extend([
    databases,
    geomapping,
    mapping,
    methods,
    normalizations,
    preferences,
    weightings,
])

# Backwards compatibility - preferable to access ``preferences`` directly
config.p = preferences

from .serialization import JsonWrapper
from .database import DatabaseChooser as Database, get_activity
from .data_store import DataStore, ProcessedDataStore
from .method import Method
from .proxies.activity import Activity
from .proxies.exchange import Exchange
from .search import Searcher, IndexManager
from .weighting_normalization import Weighting, Normalization
from .query import Query, Filter, Result
from .backends import convert_backend
# Don't confuse nose tests
from .updates import Updates

Updates.check_status()

import warnings


def warning_message(message, *args, **kwargs):
    return "Warning: " + str(message).encode("utf8", "ignore") + "\n"

warnings.formatwarning = warning_message
