# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .base import BW2DataTest
from .config import ConfigTest
from .data_store import DataStoreTestCase
from .database import (
    DatabaseQuerysetTest,
    DatabaseTest,
    ExchangeTest,
    PeeweeProxyTest,
    SingleFileDatabaseTest,
)
from .database_parameters import DatabaseParameterTest
from .geo import GeoTest
from .ia import (
    IADSTest,
    MethodTest,
    NormalizationTest,
    WeightingTest,
)
# from .json_database import JSONDatabaseTest, SynchronousJSONDictTest
# from .query import QueryTest, FilterTest, ResultTest
from .search import SearchTest, IndexTest
from .serialization import JsonSantizierTestCase
from .updates import UpdatesTest
from .utils import UtilsTest, UncertainifyTestCase
from .validation import ValidationTestCase
