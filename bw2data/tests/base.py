# -*- coding: utf-8 -*-
from .. import config
import tempfile
import unittest


class BW2DataTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        config.dont_warn = True
        config.reset(tempfile.mkdtemp())
        self.extra_setup()

    def extra_setup(self):
        pass
