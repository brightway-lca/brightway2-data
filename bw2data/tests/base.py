# -*- coding: utf-8 -*-
from .. import config, reset_meta
from ..backends.peewee import sqlite3_db
import tempfile
import unittest


class BW2DataTest(unittest.TestCase):
    def setUp(self):
        config.dont_warn = True
        config.dir = tempfile.mkdtemp()
        config.create_basic_directories()
        reset_meta()
        sqlite3_db.reset()
        self.extra_setup()

    def extra_setup(self):
        pass
