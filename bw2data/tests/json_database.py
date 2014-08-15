# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import config, databases
from ..backends.json import JSONDatabase, SynchronousJSONDict
from ..backends.json.mapping import KeyMapping, cache as mapping_cache
import os
import unittest
import shutil
import json


class JSONDatabaseTest(BW2DataTest):
    def test_load_write(self):
        jd = JSONDatabase("foo")
        jd.register()
        key = ("foobar", "spaghetti")
        data = {
            key: {
                'categories': ['stuff', 'meals'],
                'code': 1,
                'exchanges': [],
                'location': 'CA',
                'name': 'early lunch',
                'type': 'process',
                'unit': 'kg'
            }
        }
        jd.write(data)
        loaded = jd.load()
        data[key]["key"] = key
        self.assertTrue(isinstance(loaded, SynchronousJSONDict))
        self.assertEqual(loaded[key], data[key])

    def test_register_creates_directory(self):
        self.assertFalse(os.path.exists(os.path.join(
            config.dir, u"intermediate", u"foo"
        )))
        JSONDatabase("foo").register()
        print os.listdir(os.path.join(config.dir, u"intermediate"))
        self.assertTrue(os.path.exists(os.path.join(
            config.dir,
            u"intermediate",
            u"foo.acbd18db4cc2f85cedef654fccc4a4d8"
        )))
        self.assertTrue(os.path.isdir(os.path.join(
            config.dir,
            u"intermediate",
            u"foo.acbd18db4cc2f85cedef654fccc4a4d8"
        )))

    def test_write_sets_number_metadata(self):
        db = JSONDatabase("foo")
        db.register()
        db.write({("foo", str(x)): {} for x in range(10)})
        self.assertEqual(databases["foo"]["number"], 10)


class SynchronousJSONDictTest(unittest.TestCase):
    def setUp(self):
        global mapping_cache
        mapping_cache = {}
        self.fp = config.request_dir("futball")
        self.js = SynchronousJSONDict(self.fp, "futball")
        self.js.mapping = KeyMapping(self.fp)

    def tearDown(self):
        shutil.rmtree(self.fp)

    def test_write_on_modification(self):
        self.js[(u"futball", u"brazil")] = {u"foot": u"ball"}
        self.assertIn(
            u"brazil.6e5fa4d9c48ca921c0a2ce1e64c9ae6f.json",
            os.listdir(self.fp)
        )
        with open(os.path.join(
                self.fp,
                u"brazil.6e5fa4d9c48ca921c0a2ce1e64c9ae6f.json"
                )) as f:
            data = json.load(f)
            print data
        self.assertEqual(
            data,
            {u'foot': u'ball', u'key': [u'futball', u'brazil']}
        )

    def test_non_dict_raises_error(self):
        with self.assertRaises(AssertionError):
            self.js[("futball", "brazil")] = "foot"

    def test_load_makes_tuples(self):
        ds = {u'exchanges': [{u'input': [u'foo', u'bar']}]}
        self.js[(u'futball', u'brazil')] = ds
        self.js.cache = {}
        self.assertEqual(
            self.js[(u'futball', u'brazil')],
            {u'key': (u'futball', u'brazil'), u'exchanges': [{u'input': (u'foo', u'bar')}]}
        )
        self.assertNotEqual([1], tuple([1]))

    def test_set_get(self):
        ds = {u'foo': u'bar'}
        self.js[(u'futball', u'spain')] = ds
        self.js.cache = {}
        self.assertEqual(self.js[(u'futball', u'spain')][u'foo'], u'bar')

    def test_delete(self):
        key = (u'futball', u'germany')
        self.js[key] = {}
        del self.js[key]
        with self.assertRaises(KeyError):
            self.js[key]
        with self.assertRaises(KeyError):
            del self.js[key]

    def test_iter_keys_values_iteritems(self):
        keys = {(u'x', str(x)) for x in [1,2,3,4]}
        for key in keys:
            self.js[key] = {}
        for key in self.js:
            self.assertIn(key, keys)

    def test_keys(self):
        keys = {(u'x', str(x)) for x in [1,2,3,4]}
        for key in keys:
            self.js[key] = {}
        self.assertTrue(isinstance(self.js.keys(), list))
        self.assertEqual(
            sorted(list(keys)),
            sorted(self.js.keys())
        )

    def test_values(self):
        keys = {(u'x', str(x)) for x in [1,2,3,4]}
        for key in keys:
            self.js[key] = {}
        self.assertTrue(isinstance(self.js.values(), list))
        self.assertEqual(
            sorted(self.js.values()),
            sorted([{u'key': key} for key in keys])
        )

    def test_len(self):
        keys = {(u'x', str(x)) for x in [1,2,3,4]}
        for key in keys:
            self.js[key] = {}
        self.assertEqual(len(self.js), 4)

    def test_iteritems(self):
        keys = {(u'x', str(x)) for x in [1,2,3,4]}
        for key in keys:
            self.js[key] = {}
        for k, v in self.js.iteritems():
            self.assertIn(k, keys)
            self.assertEqual(v.keys(), [u'key'])

    def test_contains(self):
        keys = {(u'x', str(x)) for x in [1,2,3,4]}
        for key in keys:
            self.js[key] = {}
        for key in keys:
            self.assertIn(key, self.js)

    def test_keys_must_be_strings(self):
        with self.assertRaises(TypeError):
            self.js[(u'x', 1)] = {}

    def test_filename_illegal_characters(self):
        self.js[(u'futball', u'!')] = {}
        self.js.cache = {}
        self.assertEqual(
            self.js[(u'futball', u'!')],
            {u'key': (u'futball', u'!')}
        )
