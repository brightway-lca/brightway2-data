# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import config
from ..database import DatabaseChooser as Database
from ..errors import UnknownObject
from ..meta import mapping, geomapping, databases
from ..validate import db_validator
from .fixtures import food, biosphere
import copy
import os


class JSONDatabaseTest(BW2DataTest):
    pass
