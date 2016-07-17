# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import bw2test
from .fixtures import food as food_data, biosphere
from bw2data import config, projects
from bw2data.data_store import DataStore, ProcessedDataStore
from bw2data.errors import UnknownObject
from bw2data.serialization import SerializedDict
from bw2data.utils import numpy_string
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
    array = np.load(d.filepath_processed())

    fieldnames = {x[0] for x in d.base_uncertainty_fields}
    assert fieldnames == set(array.dtype.names)
    assert array.shape == (1,)
    assert array[0]['uncertainty_type'] == 7
    assert array[0]['amount'] == 42

def test_filepath_processed(reset):
    d = MockPDS("happy meal")
    d.write(range(10))
    fp = os.path.join(projects.dir, "processed", d.filename + ".npy")
    assert d.filepath_processed() == fp

def test_loc_value_if_no_uncertainty(reset):
    d = MockPDS("happy meal")
    d.write(range(10))
    array = np.load(d.filepath_processed())
    assert np.allclose(np.arange(10), array['loc'])

def test_order(reset):
    d = MockPDS("happy meal")
    d.write([
        {'amount': 1, 'uncertainty type': 1},
        {'amount': 2, 'uncertainty type': 0},
        {'amount': 1, 'uncertainty type': 0},
        {'amount': 0, 'uncertainty type': 1},
    ])
    array = np.load(d.filepath_processed())
    values = [(array['amount'][x], array['uncertainty_type'][x]) for x in range(4)]
    assert values == [(0.0, 1), (1.0, 0), (1.0, 1), (2.0, 0)]

def test_order_custom_dtype(reset):
    class PDS(ProcessedDataStore):
        _metadata = metadata
        dtype_fields = [
            (numpy_string('input'), np.uint32),
        ]

        def process_data(self, row):
            return (row['input'],), row

    d = PDS("happy meal")
    d.write([
        {'input': 4, 'amount': 3},
        {'input': 1, 'amount': 1},
        {'input': 3, 'amount': 2},
        {'input': 2, 'amount': 4},
    ])
    array = np.load(d.filepath_processed())
    values = [array['amount'][x] for x in range(4)]
    assert values == [1, 4, 2, 3]
