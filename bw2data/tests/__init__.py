# -*- coding: utf-8 -*-
from .array import ArrayProxyTest, ListArrayProxyTest
from .base import BW2DataTest
from .config import ConfigTest
from .data_store import DataStoreTestCase
from .database import DatabaseTest, SingleFileDatabaseTest
from .geo import GeoTest
from .ia import IADSTest, MethodTest, WeightingTest, NormalizationTest
from .json_database import JSONDatabaseTest, SynchronousJSONDictTest
from .packaging import BW2PackageTest
from .query import QueryTest, FilterTest, ResultTest
from .serialization import JsonSantizierTestCase
from .simapro import SimaProImportTest
from .sparse import SparseMatrixProxyTest
from .updates import UpdatesTest
from .utils import UtilsTest, UncertainifyTestCase
from .validation import ValidationTestCase
