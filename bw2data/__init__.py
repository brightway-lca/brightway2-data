# -*- coding: utf-8 -*
__version__ = (1, 1)

from ._config import config
from .meta import databases, methods, mapping, reset_meta, geomapping, \
    weightings, normalizations
from .serialization import JsonWrapper
from .database import DatabaseChooser as Database
from .data_store import DataStore
from .method import Method
from .weighting_normalization import Weighting, Normalization
from .query import Query, Filter, Result
# Don't confuse nose tests
from .utils import set_data_dir, setup as bw2setup
from .updates import Updates

Updates.check_status()
