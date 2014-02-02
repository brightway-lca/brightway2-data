# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import Database, config, databases
from ..data_store import DataStore
from ..errors import UnsafeData, InvalidPackage
from ..io import BW2Package
from ..serialization import SerializedDict
from fixtures import food, biosphere
import copy
import fractions
import json


class MockMetadata(SerializedDict):
    filename = "mock-meta.json"


class MockDS(DataStore):
    """Mock DataStore for testing"""
    metadata = MockMetadata()
    validator = lambda x: True
    dtype_fields = []

    def process_data(self, row):
        return (), 0


class BW2PackageTest(BW2DataTest):
    def test_class_metadata(self):
        class_metadata = {
            'module': 'bw2data.tests.packaging',
            'name': 'MockDS',
        }
        self.assertEqual(
            BW2Package._get_class_metadata(MockDS('foo')),
            class_metadata
        )

    def test_validation(self):
        good_dict = {
            'metadata': {'foo': 'bar'},
            'name': 'Johnny',
            'class': {
                'module': 'some',
                'name': 'thing'
            },
            'unrolled_dict': False,
            'data': {}
        }
        self.assertTrue(BW2Package._is_valid_package(good_dict))
        d = copy.deepcopy(good_dict)
        del d['unrolled_dict']
        self.assertTrue(BW2Package._is_valid_package(d))
        d = copy.deepcopy(good_dict)
        d['name'] = ()
        self.assertTrue(BW2Package._is_valid_package(d))
        for key in ['metadata', 'name', 'data']:
            d = copy.deepcopy(good_dict)
            del d[key]
            self.assertFalse(BW2Package._is_valid_package(d))

    def test_whitelist(self):
        good_class_metadata = {
            'module': 'bw2data.tests.packaging',
            'name': 'MockDS',
        }
        bad_class_metadata = {
            'module': 'some.package',
            'name': 'Foo',
        }
        self.assertTrue(BW2Package._is_whitelisted(good_class_metadata))
        self.assertFalse(BW2Package._is_whitelisted(bad_class_metadata))

    def test_unroll_dict(self):
        good_dict = {"a": "b", 1: 2}
        bad_dict = {("a", "composite", "key"): "some value"}
        bad_dict_result = [(("a", "composite", "key"), "some value")]
        self.assertEqual(
            BW2Package._unroll_dict(good_dict),
            (good_dict, False)
        )
        self.assertEqual(
            BW2Package._unroll_dict(bad_dict),
            (bad_dict_result, True)
        )

    def test_reroll_dict(self):
        bad_dict = {("a", "composite", "key"): "some value"}
        bad_dict_result = [(("a", "composite", "key"), "some value")]
        self.assertEqual(
            BW2Package._reroll_dict(bad_dict_result),
            bad_dict
        )

    def test_create_class_whitelist(self):
        bad_class_metadata = {
            'module': 'some.package',
            'name': 'Foo',
        }
        with self.assertRaises(UnsafeData):
            BW2Package._create_class(bad_class_metadata)
        with self.assertRaises(ImportError):
            BW2Package._create_class(bad_class_metadata, False)

    def test_create_class(self):
        class_metadata = {
            'module': 'collections',
            'name': 'Counter'
        }
        cls = BW2Package._create_class(class_metadata, False)
        import collections
        self.assertEqual(cls, collections.Counter)
        class_metadata = {
            'module': 'bw2data.database',
            'name': 'Database'
        }
        cls = BW2Package._create_class(class_metadata, False)
        self.assertEqual(cls, Database)

    def test_load_object(self):
        test_data = {
            'metadata': {'foo': 'bar'},
            'name': ['Johnny', 'B', 'Good'],
            'class': {
                'module': 'fractions',
                'name': 'Fraction'
            },
            'unrolled_dict': False,
            'data': {}
        }
        after = BW2Package._load_object(copy.deepcopy(test_data), False)
        for key in test_data:
            self.assertTrue(key in after)
        with self.assertRaises(InvalidPackage):
            BW2Package._load_object({})
        self.assertEqual(after['class'], fractions.Fraction)
        self.assertEqual(after['name'], ('Johnny', 'B', 'Good'))
        self.assertTrue(isinstance(after, dict))

    def test_tupleize(self):
        pass

    def test_create_obj(self):
        pass

    def test_export_filenames(self):
        pass

    def test_load_file(self):
        pass

    def test_roundtrip(self):
        pass

    def test_import_file(self):
        pass
