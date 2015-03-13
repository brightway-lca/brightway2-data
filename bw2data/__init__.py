# -*- coding: utf-8 -*
__version__ = (2, 0, "dev")

from ._config import config
from .utils import set_data_dir
from .meta import (
    databases,
    geomapping,
    mapping,
    methods,
    normalizations,
    weightings,
)
from .serialization import JsonWrapper
from .database import DatabaseChooser as Database, get_activity
from .data_store import DataStore
from .method import Method
from .proxies.activity import Activity
from .proxies.exchange import Exchange
from .search import Searcher, IndexManager
from .weighting_normalization import Weighting, Normalization
from .query import Query, Filter, Result
# Don't confuse nose tests
from .updates import Updates

Updates.check_status()

import warnings


def warning_message(message, *args, **kwargs):
    return "Warning: " + unicode(message).encode("utf8", "ignore") + "\n"

warnings.formatwarning = warning_message
