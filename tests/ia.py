# -*- coding: utf-8 -*-
from bw2data.tests import bw2test
from bw2data.database import DatabaseChooser as Database
from bw2data.ia_data_store import abbreviate, ImpactAssessmentDataStore as IADS
from bw2data.meta import mapping, geomapping, weightings, normalizations, methods
from bw2data.method import Method
from bw2data.serialization import CompoundJSONDict
from bw2data.validate import weighting_validator, normalization_validator, ia_validator
from bw2data.weighting_normalization import Normalization, Weighting
from bw_processing import load_package
import hashlib
import pytest


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


def test_method_write_adds_to_mapping(reset):
    Database("testy").register()
    method_data = [
        [("testy", "A"), 1],
        [("testy", "B"), 1],
    ]
    method = Method(("a", "method"))
    method.register()
    method.write(method_data)
    assert ("testy", "A") in mapping
    assert ("testy", "B") in mapping
    method_data = [
        [("testy", "A"), 1, "CH"],
        [("testy", "B"), 1, "DE"],
    ]
    method.write(method_data)
    assert "CH" in geomapping
    assert "DE" in geomapping


def test_method_write_adds_num_cfs_to_metadata(reset):
    method_data = [
        [("testy", "A"), 1],
        [("testy", "B"), 1],
    ]
    name = ("a", "method")
    method = Method(name)
    method.register()
    method.write(method_data)
    methods[name]["num_cfs"] == 2


def test_method_processed_array(reset):
    method = Method(("a", "method"))
    method.write([[("foo", "bar"), 42]])
    package = load_package(method.filepath_processed())
    array = package["characterization_matrix.npy"]
    assert array[0]["amount"] == 42


def test_method_processed_array_global_location(reset):
    method = Method(("a", "method"))
    method.write([[("foo", "bar"), 42]])
    package = load_package(method.filepath_processed())
    assert isinstance(package['datapackage']['resources'][0]['global_index'], int)


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
    package = load_package(weighting.filepath_processed())
    array = package["weighting_matrix.npy"]
    assert array[0]["amount"] == 42


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


def test_add_normalization_mappings(reset):
    norm = Normalization(("foo",))
    norm.write([[("foo", "bar"), 42]])
    assert ("foo", "bar") in mapping


def test_normalization_process_row(reset):
    norm = Normalization(("foo",))
    norm.write([[("foo", "bar"), 42]])
    package = load_package(norm.filepath_processed())
    array = package["normalization_matrix.npy"]
    assert array[0]["amount"] == 42
    assert array[0]["row_value"] == mapping[("foo", "bar")]
