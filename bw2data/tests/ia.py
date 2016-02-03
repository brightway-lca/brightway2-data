# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest
from .. import config, projects
from ..database import DatabaseChooser as Database
from ..ia_data_store import abbreviate, ImpactAssessmentDataStore as IADS
from ..meta import mapping, geomapping, weightings, normalizations, methods
from ..method import Method
from ..serialization import CompoundJSONDict
from ..utils import numpy_string
from ..validate import weighting_validator, normalization_validator, ia_validator
from ..weighting_normalization import Normalization, Weighting
import hashlib
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle


class Metadata(CompoundJSONDict):
    filename = "mock-meta.json"

metadata = Metadata()


class MockIADS(IADS):
    """Mock IADS for testing"""
    _metadata = metadata
    validator = lambda x: True
    dtype_fields = []

    def process_data(self, row):
        return (), 0


class IADSTest(BW2DataTest):
    def setUp(self):
        super(IADSTest, self).setUp()
        metadata.__init__()

    def test_unicode(self):
        iads = MockIADS(("foo", "bar"))
        self.assertEqual(
            str(iads),
            u"Brightway2 MockIADS: foo: bar"
        )

    def test_abbreviate(self):
        self.assertEqual(
            abbreviate(("foo", "bar")),
            u"foob.%s" % hashlib.md5(b"foo-bar").hexdigest()
        )
        self.assertNotEqual(
            abbreviate(("foo", "bar")),
            abbreviate(("foo", "baz"))
        )

    def test_copy_no_name(self):
        iads = MockIADS(("foo", "bar"))
        iads.register(paris="France")
        iads.write({1:2})
        new_one = iads.copy()
        new_name = ("foo", "Copy of bar")
        self.assertEqual(new_one.name, new_name)
        self.assertTrue(new_name in metadata)
        self.assertEqual(new_one.load(), {1:2})
        self.assertEqual(
            metadata[("foo", "bar")]["paris"],
            metadata[new_name]["paris"]
        )
        self.assertFalse(metadata[("foo", "bar")] == metadata[new_name])

    def test_copy_with_name(self):
        iads = MockIADS(("foo", "bar"))
        iads.register(paris="France")
        iads.write({1:2})
        new_name = ("bar", "foo")
        new_one = iads.copy(new_name)
        self.assertEqual(new_one.name, new_name)
        self.assertTrue(new_name in metadata)
        self.assertEqual(new_one.load(), {1:2})
        self.assertEqual(
            metadata[("foo", "bar")]["paris"],
            metadata[new_name]["paris"]
        )
        self.assertFalse(metadata[("foo", "bar")] == metadata[new_name])

    def test_register_adds_abbreviation(self):
        name = ("foo", "bar")
        self.assertFalse(name in metadata)
        iads = MockIADS(name)
        iads.register()
        self.assertEqual(
            list(metadata[name].keys()),
            ['abbreviation']
        )


class MethodTest(BW2DataTest):
    def test_write_adds_to_mapping(self):
        Database("testy").register()
        method_data = [
            [("testy", "A"), 1],
            [("testy", "B"), 1],
        ]
        method = Method(("a", "method"))
        method.register()
        method.write(method_data)
        self.assertTrue(("testy", "A") in mapping)
        self.assertTrue(("testy", "B") in mapping)
        method_data = [
            [("testy", "A"), 1, "CH"],
            [("testy", "B"), 1, "DE"],
        ]
        method.write(method_data)
        self.assertTrue("CH" in geomapping)
        self.assertTrue("DE" in geomapping)

    def test_write_adds_num_cfs_to_metadata(self):
        method_data = [
            [("testy", "A"), 1],
            [("testy", "B"), 1],
        ]
        name = ("a", "method")
        method = Method(name)
        method.register()
        method.write(method_data)
        self.assertTrue(methods[name]["num_cfs"], 2)

    def test_processed_array(self):
        method = Method(("a", "method"))
        method.register()
        method.write([[("foo", "bar"), 42]])
        fp = os.path.join(projects.dir, u"processed", method.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))

        fieldnames = {x[0] for x in method.base_uncertainty_fields}.union({'flow', 'geo', 'row', 'col'})
        self.assertEqual(fieldnames, set(array.dtype.names))
        self.assertEqual(array[0]['amount'], 42)

    def test_base_class(self):
        method = Method(("a", "method"))
        self.assertEqual(method.validator, ia_validator)
        self.assertEqual(method._metadata, methods)
        method.register()
        self.assertTrue(isinstance(method.metadata, dict))
        self.assertEqual(
            [x[0] for x in method.dtype_fields],
            [numpy_string(x) for x in ('flow', 'geo', 'row', 'col')])

    def test_validator(self):
        method = Method(("a", "method"))
        self.assertTrue(method.validate([]))


class WeightingTest(BW2DataTest):
    def test_write_good_data(self):
        w = Weighting(("foo",))
        w.register()
        w.write([2])
        w.write([{'amount': 2}])
        w.write([{'amount': 2, 'uncertainty type': 0}])

    def test_write_invalid_data(self):
        w = Weighting(("foo",))
        w.register()
        with self.assertRaises(ValueError):
            w.write(2)
        with self.assertRaises(ValueError):
            w.write([2, 4])

    def test_process(self):
        weighting = Weighting(("foo",))
        weighting.register()
        weighting.write([42])

        fp = os.path.join(projects.dir, u"processed", weighting.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))

        fieldnames = {x[0] for x in weighting.base_uncertainty_fields}
        self.assertEqual(fieldnames, set(array.dtype.names))
        self.assertEqual(array[0]['amount'], 42)

    def test_base_class(self):
        weighting = Weighting(("foo",))
        self.assertEqual(weighting.validator, weighting_validator)
        self.assertEqual(weighting._metadata, weightings)
        weighting.register()
        self.assertTrue(isinstance(weighting.metadata, dict))
        self.assertEqual(weighting.dtype_fields, [])

    def test_validator(self):
        weighting = Weighting(("foo",))
        self.assertTrue(weighting.validate([{'amount': 1}]))


class NormalizationTest(BW2DataTest):
    def test_base_class(self):
        norm = Normalization(("foo",))
        self.assertEqual(norm.validator, normalization_validator)
        self.assertEqual(norm._metadata, normalizations)
        self.assertEqual(
            [x[0] for x in norm.dtype_fields],
            [numpy_string(x) for x in ('flow', 'index')])

    def test_add_mappings(self):
        norm = Normalization(("foo",))
        norm.register()
        norm.write([[("foo", "bar"), 42]])
        self.assertTrue(("foo", "bar") in mapping)

    def test_process_data(self):
        norm = Normalization(("foo",))
        norm.register()
        norm.write([[("foo", "bar"), 42]])

        fp = os.path.join(projects.dir, u"processed", norm.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))

        fieldnames = {x[0] for x in norm.base_uncertainty_fields}.union({'flow', 'index'})
        self.assertEqual(fieldnames, set(array.dtype.names))
        self.assertEqual(array[0]['amount'], 42)
        self.assertEqual(array[0]['flow'], mapping[("foo", "bar")])
