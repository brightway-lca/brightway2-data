# -*- coding: utf-8 -*-
import os
from . import BW2DataTest
from .. import config
import json


class ConfigTest(BW2DataTest):
    def test_request_directory_not_writable(self):
        dirpath = config.request_dir("untouchable")
        os.chmod(dirpath, 000)
        self.assertFalse(config.request_dir("untouchable"))
        os.chmod(dirpath, 776)

    def test_request_directory(self):
        self.assertTrue(config.request_dir("wow"))
        self.assertTrue(config.request_dir(u"привет"))

    def test_basic_preferences(self):
        preferences = {
            "use_cache": True,
        }
        config.load_preferences()
        self.assertEqual(preferences, config.p)

    def test_save_preferences(self):
        config.load_preferences()
        config.p['saved'] = "yep"
        config.save_preferences()
        self.assertEqual(config.p['saved'], "yep")
        config.load_preferences()
        self.assertEqual(config.p['saved'], "yep")
        data = json.load(open(os.path.join(config.dir, "preferences.json")))
        self.assertEqual(data['saved'], "yep")
