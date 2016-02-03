# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest
from .. import config, projects
from ..data_store import DataStore, ProcessedDataStore
from ..errors import UnknownObject
from ..serialization import SerializedDict
from numbers import Number
from voluptuous import Schema
import numpy as np
import os
import pickle


class Metadata(SerializedDict):
    filename = "mock-meta.json"

metadata = Metadata()


class MockDS(DataStore):
    """Mock DataStore for testing"""
    _metadata = metadata
    validator = Schema(Number)


class MockPDS(ProcessedDataStore):
    """Mock DataStore for testing"""
    _metadata = metadata
    validator = Schema(Number)
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

    def test_unicode(self):
        d = MockDS("food")
        self.assertEqual(
            str(d),
            "Brightway2 MockDS: food"
        )

    def test_deregister(self):
        d = MockDS("evening")
        d.register()
        self.assertTrue("evening" in metadata)
        d.deregister()
        self.assertFalse("evening" in metadata)

    def test_metadata_keyerror(self):
        d = MockDS("evening")
        with self.assertRaises(UnknownObject):
            d.metadata

    def test_metadata_readable_writable(self):
        d = MockDS("twilight")
        d.register()
        d.metadata = {'foo': 'bar'}
        self.assertEqual(d.metadata, {'foo': 'bar'})

    def test_write_load(self):
        d = MockDS("full moon")
        d.write(range(10))
        data = pickle.load(open(os.path.join(
            projects.dir,
            "intermediate",
            d.filename + ".pickle"
        ), 'rb'))
        self.assertEqual(list(data), list(range(10)))

    def test_copy(self):
        d = MockDS("full moon")
        d.register(foo='bar')
        d.write(range(10))
        gibbous = d.copy("waning gibbous")
        self.assertEqual(list(gibbous.load()), list(range(10)))
        self.assertEqual(metadata['waning gibbous'], {'foo': 'bar'})

    def test_validation(self):
        d = MockDS("cat")
        self.assertTrue(d.validate(4))


class ProcessedDataStoreTestCase(BW2DataTest):
    def setUp(self):
        super(ProcessedDataStoreTestCase, self).setUp()
        metadata.__init__()

    def test_as_uncertainty_dict(self):
        d = MockPDS("sad")
        self.assertEqual(d.as_uncertainty_dict({}), {})
        self.assertEqual(d.as_uncertainty_dict(1), {'amount': 1.})
        with self.assertRaises(TypeError):
            d.as_uncertainty_dict("foo")

    def test_processed_array(self):
        d = MockPDS("happy")
        d.write([{'amount': 42, 'uncertainty type': 7}])
        fp = os.path.join(projects.dir, u"processed", d.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))

        fieldnames = {x[0] for x in d.base_uncertainty_fields}
        self.assertEqual(fieldnames, set(array.dtype.names))
        self.assertEqual(array.shape, (1,))
        self.assertEqual(array[0]['uncertainty_type'], 7)
        self.assertEqual(array[0]['amount'], 42)

    def test_loc_value_if_no_uncertainty(self):
        d = MockPDS("happy meal")
        d.write(range(10))
        fp = os.path.join(projects.dir, u"processed", d.filename + u".pickle")
        array = pickle.load(open(fp, "rb"))
        self.assertTrue(np.allclose(np.arange(10), array['loc']))
