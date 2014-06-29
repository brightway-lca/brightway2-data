# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import config
from ..data_store import DataStore
from ..errors import UnknownObject
from ..serialization import SerializedDict
from voluptuous import Schema
import os
import numpy as np
import pickle


class Metadata(SerializedDict):
    filename = "mock-meta.json"

metadata = Metadata()


class MockDS(DataStore):
    """Mock DataStore for testing"""
    metadata = metadata
    validator = Schema(int)
    dtype_fields = []

    def process_data(self, row):
        return (), row


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
        self.assertTrue(d.validate(4))

    def test_processed_array(self):
        d = MockDS("happy")
        d.register()
        d.write([{'amount': 42, 'uncertainty type': 7}])
        d.process()
        fp = os.path.join(config.dir, u"processed", d.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))

        fieldnames = {x[0] for x in d.base_uncertainty_fields}
        self.assertEqual(fieldnames, set(array.dtype.names))
        self.assertEqual(array.shape, (1,))
        self.assertEqual(array[0]['uncertainty_type'], 7)
        self.assertEqual(array[0]['amount'], 42)

    def test_loc_value_if_no_uncertainty(self):
        d = MockDS("happy meal")
        d.register()
        d.write(range(10))
        d.process()
        fp = os.path.join(config.dir, u"processed", d.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))
        self.assertTrue(np.allclose(np.arange(10), array['loc']))
