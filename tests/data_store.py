from bw2data.tests import bw2test
from bw2data import projects
from bw2data.data_store import DataStore, ProcessedDataStore
from bw2data.errors import UnknownObject
from bw2data.serialization import SerializedDict

# from bw_processing import load_package, COMMON_DTYPE
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

    def process_row(self, row):
        return row


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
    d.metadata = {"foo": "bar"}
    assert d.metadata == {"foo": "bar"}


def test_data_store_write_load(reset):
    d = MockDS("full moon")
    d.write(range(10))
    data = pickle.load(
        open(os.path.join(projects.dir, "intermediate", d.filename + ".pickle"), "rb")
    )
    assert list(data) == list(range(10))


def test_data_store_copy(reset):
    d = MockDS("full moon")
    d.register(foo="bar")
    d.write(range(10))
    gibbous = d.copy("waning gibbous")
    assert list(gibbous.load()) == list(range(10))
    assert metadata["waning gibbous"] == {"foo": "bar"}


def test_data_store_validation(reset):
    d = MockDS("cat")
    assert d.validate(4)
