# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import config
from ..backends.json import JSONDatabase, SynchronousJSONDict
from ..errors import UnknownObject
from ..meta import mapping, geomapping, databases
from ..validate import db_validator
from .fixtures import food, biosphere
import copy
import os
import unittest


class JSONDatabaseTest(BW2DataTest):
    # Intermediate filepath
    # Write/Load
    # Register creates directory

    pass


class SynchronousJSONDictTest(unittest.TestCase):
    # JSONDict writes changes on modification
    # JSONDict setitem, getitem, delitem
    # JSONDict, iter, contains, len
    # JSONDict turns inputs into tuples
    # JSONDict keys, values, iteritems
    # Indirectly test mapping with funny key characters

    pass
