# -*- coding: utf-8 -*-
from .. import config
from ..project import projects
import unittest


class BW2DataTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        config.dont_warn = True
        projects.use_temp_directory()
        self.extra_setup()

    def extra_setup(self):
        pass
