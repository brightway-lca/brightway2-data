# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

import os
from . import BW2DataTest
from .. import config
import json


class ConfigTest(BW2DataTest):
    def test_default_biosphere(self):
        self.assertEqual(config.biosphere, "biosphere3")

    def test_default_geo(self):
        self.assertEqual(config.global_location, "GLO")

    # def test_set_retrieve_biosphere(self):
    #     config.p['biosphere_database'] = "foo"
    #     config.save_preferences()
    #     config.load_preferences()
    #     self.assertEqual(config.biosphere, "foo")
    #     del config.p['biosphere_database']
    #     config.save_preferences()
