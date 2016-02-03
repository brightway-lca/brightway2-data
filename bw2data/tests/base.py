# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .. import config, databases, methods, mapping, geomapping
from ..project import projects
import unittest


class BW2DataTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        config.dont_warn = True
        config.is_test = True
        config.cache = {}
        projects.use_temp_directory()
        self.extra_setup()

    def extra_setup(self):
        pass

    def test_setup_clean(self):
        self.assertEqual(list(databases), [])
        self.assertEqual(list(methods), [])
        self.assertEqual(len(mapping), 0)
        self.assertEqual(len(geomapping), 1)  # GLO
        self.assertTrue("GLO" in geomapping)
        self.assertEqual(len(projects), 1)  # Default project
        self.assertTrue("default" in projects)
