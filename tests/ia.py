import hashlib
import unittest.mock as mock

import numpy as np
import pytest
from bw_processing import load_datapackage
from fsspec.implementations.zip import ZipFileSystem

from bw2data import config, get_id, get_node
from bw2data.backends import Activity
from bw2data.backends.schema import ActivityDataset as AD
from bw2data.database import DatabaseChooser
from bw2data.errors import UnknownObject
from bw2data.ia_data_store import ImpactAssessmentDataStore as IADS
from bw2data.ia_data_store import abbreviate
from bw2data.meta import databases, geomapping, methods, normalizations, weightings
from bw2data.method import Method
from bw2data.serialization import CompoundJSONDict
from bw2data.tests import bw2test
from bw2data.validate import ia_validator, normalization_validator, weighting_validator
from bw2data.weighting_normalization import Normalization, Weighting


class Metadata(CompoundJSONDict):
    filename = "mock-meta.json"


metadata = Metadata()


class MockIADS(IADS):
    """Mock IADS for testing"""

    _metadata = metadata
    validator = lambda x: True

    def process_row(self, row):
        return {"row": 1, "amount": 2}


@pytest.fixture
@bw2test
def reset():
    metadata.__init__()


def test_unicode(reset):
    iads = MockIADS(("foo", "bar"))
    assert str(iads) in {
        "Brightway2 MockIADS: foo: bar",
        "Brightway2 MockIADS: (u'foo', u'bar')",
    }


def test_abbreviate(reset):
    assert abbreviate(("foo", "bar")) == "foob.%s" % hashlib.md5(b"foo-bar").hexdigest()
    assert abbreviate(("foo", "bar")) != abbreviate(("foo", "baz"))


def test_copy_no_name(reset):
    iads = MockIADS(("foo", "bar"))
    iads.register(paris="France")
    iads.write({1: 2})
    new_one = iads.copy()
    new_name = ("foo", "Copy of bar")
    assert new_one.name == new_name
    assert new_name in metadata
    assert new_one.load() == {1: 2}
    assert metadata[("foo", "bar")]["paris"] == metadata[new_name]["paris"]
    assert metadata[("foo", "bar")] != metadata[new_name]


def test_copy_with_name(reset):
    iads = MockIADS(("foo", "bar"))
    iads.register(paris="France")
    iads.write({1: 2})
    new_name = ("bar", "foo")
    new_one = iads.copy(new_name)
    assert new_one.name == new_name
    assert new_name in metadata
    assert new_one.load() == {1: 2}
    assert metadata[("foo", "bar")]["paris"] == metadata[new_name]["paris"]
    assert metadata[("foo", "bar")] != metadata[new_name]


def test_register_adds_abbreviation(reset):
    name = ("foo", "bar")
    assert name not in metadata
    iads = MockIADS(name)
    iads.register()
    assert list(metadata[name].keys()) == ["abbreviation"]


def test_method_write_adds_num_cfs_to_metadata(reset):
    assert not len(databases)
    assert not len(methods)
    assert not AD.select().count()

    database = DatabaseChooser("testy")
    data = {
        ("testy", "A"): {},
        ("testy", "B"): {
            "exchanges": [
                {"input": ("testy", "A"), "amount": 1, "type": "technosphere"},
            ]
        },
    }
    database.write(data)

    method_data = [
        [("testy", "A"), 1],
        [("testy", "B"), 1],
    ]
    name = ("a", "method")
    method = Method(name)
    method.register()
    method.write(method_data)
    assert methods[name]["num_cfs"] == 2


def test_method_processed_array(reset):
    database = DatabaseChooser("foo")
    database.write({("foo", "bar"): {}})

    method = Method(("a", "method"))
    method.write([[("foo", "bar"), 42]])
    package = load_datapackage(ZipFileSystem(method.filepath_processed()))
    data = package.get_resource("a_method_matrix_data.data")[0]
    assert np.allclose(data, [42])

    indices = package.get_resource("a_method_matrix_data.indices")[0]
    assert np.allclose(indices["row"], get_id(("foo", "bar")))
    assert np.allclose(indices["col"], geomapping[config.global_location])


def test_method_processed_array_add_identifier(reset):
    database = DatabaseChooser("foo")
    database.write({("foo", "bar"): {}})

    method = Method(("a", "method"))
    method.write([[("foo", "bar"), 42]])
    package = load_datapackage(ZipFileSystem(method.filepath_processed()))
    assert package.metadata["resources"][0]["identifier"] == ["a", "method"]


@bw2test
def test_iads_process_without_name():
    iads = MockIADS(None)
    with pytest.raises(TypeError):
        iads.process()


@bw2test
def test_method_missing_reference():
    database = DatabaseChooser("foo")
    database.write({("foo", "bar"): {}, ("foo", "baz"): {}})

    method = Method(("a", "method"))
    method.write([[("foo", "bar"), 42], [("foo", "baz"), 1]])

    database.get(code="baz").delete()
    with pytest.raises(UnknownObject):
        method.process()


@bw2test
def test_method_missing_location():
    database = DatabaseChooser("foo")
    database.write({("foo", "bar"): {}, ("foo", "baz"): {}})

    method = Method(("a", "method"))
    method.write([[("foo", "bar"), 42, "somewhere"]])

    del geomapping["somewhere"]
    with pytest.raises(UnknownObject):
        method.process()


@bw2test
def test_method_missing_global_location():
    database = DatabaseChooser("foo")
    database.write({("foo", "bar"): {}, ("foo", "baz"): {}})

    method = Method(("a", "method"))
    method.write([[("foo", "bar"), 42]])

    del geomapping[config.global_location]
    with pytest.raises(KeyError):
        method.process()


def test_method_base_class(reset):
    method = Method(("a", "method"))
    assert method.validator == ia_validator
    assert method._metadata == methods
    method.register()
    assert isinstance(method.metadata, dict)


def test_method_validator(reset):
    method = Method(("a", "method"))
    assert method.validate([])


def test_weighting_write_good_data(reset):
    w = Weighting(("foo",))
    w.register()
    w.write([2])
    w.write([{"amount": 2}])
    w.write([{"amount": 2, "uncertainty type": 0}])


def test_weighting_write_invalid_data(reset):
    w = Weighting(("foo",))
    w.register()
    with pytest.raises(ValueError):
        w.write(2)
    with pytest.raises(ValueError):
        w.write([2, 4])


def test_weighting_process(reset):
    weighting = Weighting(("foo",))
    weighting.write([42])
    package = load_datapackage(ZipFileSystem(weighting.filepath_processed()))

    data = package.get_resource("foo_matrix_data.data")[0]
    assert np.allclose(data, [42])

    indices = package.get_resource("foo_matrix_data.indices")[0]
    assert np.allclose(indices["row"], 0)
    assert np.allclose(indices["col"], 0)


def test_weighting_base_class(reset):
    weighting = Weighting(("foo",))
    assert weighting.validator == weighting_validator
    assert weighting._metadata == weightings
    weighting.register()
    assert isinstance(weighting.metadata, dict)


def test_weighting_validator(reset):
    weighting = Weighting(("foo",))
    assert weighting.validate([{"amount": 1}])


def test_base_normalization_class(reset):
    norm = Normalization(("foo",))
    assert norm.validator == normalization_validator
    assert norm._metadata == normalizations


def test_normalization_process_row(reset):
    database = DatabaseChooser("foo")
    database.write({("foo", "bar"): {}})

    norm = Normalization(("foo",))
    norm.write([[("foo", "bar"), 42]])
    package = load_datapackage(ZipFileSystem(norm.filepath_processed()))

    data = package.get_resource("foo_matrix_data.data")[0]
    assert np.allclose(data, [42])

    indices = package.get_resource("foo_matrix_data.indices")[0]
    assert np.allclose(indices["row"], get_id(("foo", "bar")))
    assert np.allclose(indices["col"], get_id(("foo", "bar")))


@bw2test
def test_method_geocollection():
    database = DatabaseChooser("foo")
    database.write(
        {
            ("foo", "1"): {},
            ("foo", "2"): {},
            ("foo", "3"): {},
        }
    )

    f1 = get_node(code="1").id
    f2 = get_node(code="2").id

    m = Method(("foo",))
    m.write([(f1, 2, "RU"), (f2, 4, ("foo", "bar"))])
    assert m.metadata["geocollections"] == ["foo", "world"]


@bw2test
def test_method_geocollection_missing_ok():
    database = DatabaseChooser("foo")
    database.write(
        {
            ("foo", "1"): {},
            ("foo", "2"): {},
            ("foo", "3"): {},
        }
    )

    f1 = get_node(code="1").id
    f3 = get_node(code="3").id

    m = Method(("foo",))
    m.write(
        [
            (f1, 2, None),
            (f3, 4),
        ]
    )
    assert m.metadata["geocollections"] == ["world"]


@bw2test
def test_method_geocollection_warning():
    database = DatabaseChooser("foo")
    database.write(
        {
            ("foo", "1"): {},
        }
    )

    f1 = get_node(code="1").id

    m = Method(("foo",))
    m.write(
        [
            (f1, 2, "Russia"),
        ]
    )
    assert m.metadata["geocollections"] == []


def test_method_pass_id_processed_array(reset):
    database = DatabaseChooser("foo")
    database.write({("foo", "bar"): {}})
    node = get_node(code="bar")

    method = Method(("a", "method"))
    method.write([[node.id, 42]])
    package = load_datapackage(ZipFileSystem(method.filepath_processed()))
    data = package.get_resource("a_method_matrix_data.data")[0]
    assert np.allclose(data, [42])

    indices = package.get_resource("a_method_matrix_data.indices")[0]
    assert np.allclose(indices["row"], node.id)
    assert np.allclose(indices["col"], geomapping[config.global_location])


@pytest.fixture
@bw2test
def testy():
    database = DatabaseChooser("testy")
    data = {
        ("testy", "A"): {},
        ("testy", "B"): {
            "exchanges": [
                {"input": ("testy", "A"), "amount": 1, "type": "technosphere"},
            ]
        },
    }
    database.write(data)

    method_data = [
        [("testy", "A"), 1],
        [("testy", "B"), 1],
    ]
    name = ("a", "method")
    method = Method(name)
    method.write(method_data)
    assert methods[name]["num_cfs"] == 2
    return method


def test_method_write_tuple_get_int(testy):
    for line in testy.load():
        assert isinstance(line[0], int)


def test_method_iteration(testy):
    for line in testy:
        assert isinstance(line[0], Activity)
        assert isinstance(line[1], int)

    assert list(testy) == [(get_node(code="A"), 1), (get_node(code="B"), 1)]


def test_method_write_with_nodes():
    database = DatabaseChooser("testy")
    data = {
        ("testy", "A"): {},
        ("testy", "B"): {
            "exchanges": [
                {"input": ("testy", "A"), "amount": 1, "type": "technosphere"},
            ]
        },
    }
    database.write(data)

    method_data = [(get_node(code="A"), 1), (get_node(code="B"), 1)]
    name = ("a", "method")
    method = Method(name)
    method.write(method_data)
    assert methods[name]["num_cfs"] == 2

    assert list(method) == [(get_node(code="A"), 1), (get_node(code="B"), 1)]
