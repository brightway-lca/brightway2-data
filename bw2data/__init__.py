# -*- coding: utf-8 -*
from _config import config
from meta import databases, methods, mapping, reset_meta, geomapping
from serialization import JsonWrapper
from database import Database
from method import Method
from query import Query, Filter, Result
from utils import set_data_dir
# Don't confuse nose tests
from utils import setup as bw2setup
import proxies
import utils
import validate
import io

__version__ = (0, 9, 2)
