# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import Updates, config, Database, Method, mapping, geomapping
from ..ia_data_store import abbreviate, ImpactAssessmentDataStore as IADS
from ..serialization import CompoundJSONDict
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
    metadata = metadata
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
            iads.__unicode__(),
            u"Brightway2 MockIADS: foo: bar"
        )

    def test_abbreviate(self):
        self.assertEqual(
            abbreviate(("foo", "bar")),
            u"foob-%s" % hashlib.md5("foo-bar").hexdigest()
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
        self.assertEqual(metadata[name].keys(), ['abbreviation'])


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

    def test_processed_array(self):
        method = Method(("a", "method"))
        method.register()
        method.write([])
        method.process()
        fp = os.path.join(config.dir, u"processed", method.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))

        fieldnames = {'flow', 'geo', 'row', 'col'}
        self.assertFalse(fieldnames.difference(set(array.dtype.names)))


