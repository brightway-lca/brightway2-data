# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import config, Database, mapping
from ..data_store import DataStore
from ..serialization import SerializedDict
from ..errors import UnknownObject
import hashlib
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle


class Metadata(SerializedDict):
    filename = "mock-meta.json"

metadata = Metadata()


class MockDS(DataStore):
    """Mock DataStore for testing"""
    metadata = metadata
    validator = lambda x: True
    dtype_fields = []

    def process_data(self, row):
        return (), 0


class DataStoreTestCase(BW2DataTest):
    def setUp(self):
        super(DataStoreTestCase, self).setUp()
        metadata.__init__()

    def test_repr(self):
        d = MockDS("food")
        self.assertTrue(isinstance(str(d), str))
        self.assertTrue(isinstance(unicode(d), unicode))

    def test_unicode(self):
        d = MockDS("food")
        self.assertEqual(
            unicode(d),
            u"Brightway2 MockDS: food"
        )

    def test_register_twice(self):
        d = MockDS("morning")
        d.register()
        with self.assertRaises(AssertionError):
            d.register()

    def test_deregister(self):
        d = MockDS("evening")
        d.register()
        self.assertTrue("evening" in metadata)
        d.deregister()
        self.assertFalse("evening" in metadata)

    def test_assert_registered(self):
        d = MockDS("evening")
        with self.assertRaises(UnknownObject):
            d.assert_registered()

    def test_write_load(self):
        d = MockDS("full moon")
        d.register()
        d.write(range(10))
        data = pickle.load(open(os.path.join(
            config.dir,
            u"intermediate",
            d.filename + ".pickle"
        )))
        self.assertEqual(data, range(10))

    def test_copy(self):
        d = MockDS("full moon")
        d.register(foo='bar')
        d.write(range(10))
        gibbous = d.copy("waning gibbous")
        self.assertEqual(gibbous.load(), range(10))
        self.assertEqual(metadata['waning gibbous'], {'foo': 'bar'})

    def test_as_uncertainty_dict(self):
        d = MockDS("sad")
        self.assertEqual(d.as_uncertainty_dict({}), {})
        self.assertEqual(d.as_uncertainty_dict(1), {'amount': 1.})
        with self.assertRaises(TypeError):
            d.as_uncertainty_dict("foo")

    def test_validation(self):
        d = MockDS("cat")
        self.assertTrue(d.validate("dog"))

    def test_processed_array(self):
        d = MockDS("happy")
        d.register()
        d.write([])
        d.process()
        fp = os.path.join(config.dir, u"processed", d.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))

        fieldnames = {x[0] for x in d.base_uncertainty_fields}
        self.assertFalse(fieldnames.difference(set(array.dtype.names)))
