# -*- coding: utf-8 -*-
from .. import config, reset_meta
import tempfile
import unittest


class BW2DataTest(unittest.TestCase):
    def setUp(self):
        config.dont_warn = True
        config.cache = {}
        config.dir = tempfile.mkdtemp()
        config.create_basic_directories()
        reset_meta()
        self.extra_setup()

    def extra_setup(self):
        pass
