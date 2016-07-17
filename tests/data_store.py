# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import bw2test
from .fixtures import food as food_data, biosphere
from bw2data import config, projects
from bw2data.data_store import DataStore, ProcessedDataStore
from bw2data.errors import UnknownObject
from bw2data.serialization import SerializedDict
from numbers import Number
from voluptuous import Schema
import numpy as np
import os
import pickle
import pytest


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


@pytest.fixture
@bw2test
def reset():
    metadata.__init__()


### DataStore

def test_data_store_repr(reset):
    d = MockDS("food")
    assert isinstance(str(d), str)

def test_data_store_unicode(reset):
    d = MockDS("food")
    assert str(d) == "Brightway2 MockDS: food"

def test_data_store_deregister(reset):
    d = MockDS("evening")
    d.register()
    assert "evening" in metadata
    d.deregister()
    assert "evening" not in metadata

def test_data_store_metadata_keyerror(reset):
    d = MockDS("evening")
    with pytest.raises(UnknownObject):
        d.metadata

def test_data_store_metadata_readable_writable(reset):
    d = MockDS("twilight")
    d.register()
    d.metadata = {'foo': 'bar'}
    assert d.metadata == {'foo': 'bar'}

def test_data_store_write_load(reset):
    d = MockDS("full moon")
    d.write(range(10))
    data = pickle.load(open(os.path.join(
        projects.dir,
        "intermediate",
        d.filename + ".pickle"
    ), 'rb'))
    assert list(data) == list(range(10))

def test_data_store_copy(reset):
    d = MockDS("full moon")
    d.register(foo='bar')
    d.write(range(10))
    gibbous = d.copy("waning gibbous")
    assert list(gibbous.load()) == list(range(10))
    assert metadata['waning gibbous'] == {'foo': 'bar'}

def test_data_store_validation(reset):
    d = MockDS("cat")
    assert d.validate(4)

### ProcessedDataStore

def test_processed_data_store_as_uncertainty_dict(reset):
    d = MockPDS("sad")
    assert d.as_uncertainty_dict({}) == {}
    assert d.as_uncertainty_dict(1) == {'amount': 1.}
    with pytest.raises(TypeError):
        d.as_uncertainty_dict("foo")

def test_processed_array(reset):
    d = MockPDS("happy")
    d.write([{'amount': 42, 'uncertainty type': 7}])
    fp = os.path.join(projects.dir, "processed", d.filename + ".npy")
    array = np.load(fp)

    fieldnames = {x[0] for x in d.base_uncertainty_fields}
    assert fieldnames == set(array.dtype.names)
    assert array.shape == (1,)
    assert array[0]['uncertainty_type'] == 7
    assert array[0]['amount'] == 42

def test_loc_value_if_no_uncertainty(reset):
    d = MockPDS("happy meal")
    d.write(range(10))
    fp = os.path.join(projects.dir, "processed", d.filename + ".npy")
    array = np.load(fp)
    assert np.allclose(np.arange(10), array['loc'])

def test_order(reset):
    d = MockPDS("happy meal")
    d.write(range(10))
    fp = os.path.join(projects.dir, "processed", d.filename + ".npy")
    array = np.load(fp)
    assert np.allclose(np.arange(10), array['loc'])

def test_order_custom_dtype(reset):
    pass
