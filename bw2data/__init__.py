# -*- coding: utf-8 -*
__version__ = (0, 10, 5)

from _config import config
from meta import databases, methods, mapping, reset_meta, geomapping, \
    weightings, normalizations
from serialization import JsonWrapper
from database import Database
from method import Method
from weighting_normalization import Weighting, Normalization
from query import Query, Filter, Result
from utils import set_data_dir
# Don't confuse nose tests
from utils import setup as bw2setup
import proxies
import utils
import validate
import io

from upgrades import check_status
check_status()
