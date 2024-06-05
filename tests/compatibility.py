import pytest

from bw2data import (
    Database,
    DataStore,
    Method,
    Normalization,
    Weighting,
    databases,
    geomapping,
    get_activity,
    get_multilca_data_objs,
    methods,
    normalizations,
    prepare_lca_inputs,
    projects,
    weightings,
)
from bw2data.errors import UnknownObject
from bw2data.tests import bw2test

from .fixtures import biosphere, food, food2, lcia


@bw2test
def test_repr_str_unicode():
    objects = (
        geomapping,
        databases,
        methods,
        normalizations,
        weightings,
        Database("foo"),
        DataStore("foo"),
        projects,
    )
    for obj in objects:
        assert repr(obj)
        assert str(obj)
        print(obj)


@bw2test
def test_registered_database_repr():
    d = Database("biosphere")
    d.write(biosphere)
    assert repr(d)
    assert str(d)
    # Make sure can be printed - not for debugging
    print(d)


def setup():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Method(("foo",)).write(lcia)

    return Database("biosphere"), Database("food"), Method(("foo",))


@bw2test
def test_prepare_lca_inputs_basic():
    pla = setup()
    d, objs, r = prepare_lca_inputs(demand={("food", "1"): 1}, method=("foo",))
    # ID is 3; two biosphere flows, then '1' is next written
    assert d == {3: 1}
    assert [str(o.fs) for o in objs] == [str(o.datapackage().fs) for o in pla]

    remapping_expected = {
        "activity": {
            1: ("biosphere", "1"),
            2: ("biosphere", "2"),
            3: ("food", "1"),
            4: ("food", "2"),
        },
        "product": {
            1: ("biosphere", "1"),
            2: ("biosphere", "2"),
            3: ("food", "1"),
            4: ("food", "2"),
        },
        "biosphere": {
            1: ("biosphere", "1"),
            2: ("biosphere", "2"),
            3: ("food", "1"),
            4: ("food", "2"),
        },
    }
    assert r == remapping_expected


@bw2test
def test_prepare_lca_inputs_only_method():
    setup()
    d, objs, r = prepare_lca_inputs(method=("foo",))
    # ID is 3; two biosphere flows, then '1' is next written
    assert d is None
    assert [str(o.fs) for o in objs] == [
        str(o.datapackage().fs) for o in [Method(("foo",))]
    ]


@bw2test
def test_prepare_lca_inputs_multiple_demands():
    pla = setup()
    d, objs, r = prepare_lca_inputs(
        demands=[{("food", "1"): 1}, {("food", "2"): 10}], method=("foo",)
    )
    # ID is 3; two biosphere flows, then '1' is next written
    assert d == [{3: 1}, {4: 10}]
    assert {str(o.fs) for o in objs} == {str(o.datapackage().fs) for o in pla}


@bw2test
def test_prepare_lca_inputs_database_ordering():
    pla = setup()
    d, objs, r = prepare_lca_inputs(
        demands=[{("food", "1"): 1}, {("food", "2"): 10}],
        method=("foo",),
        demand_database_last=False,
    )
    assert {str(o.fs) for o in objs} == {str(o.datapackage().fs) for o in pla}


@bw2test
def test_prepare_lca_inputs_remapping():
    setup()
    d, objs, r = prepare_lca_inputs(
        demand={("food", "1"): 1}, method=("foo",), remapping=False
    )
    assert r is None


@bw2test
def test_get_multilca_data_objs_complete():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Database("food2").write(food2)
    Method(("foo",)).write(lcia)
    Normalization(("normal",)).write(
        [
            (("biosphere", "1"), 10),
            (("biosphere", "2"), 20),
        ]
    )
    Weighting(("weight",)).write([42])

    objs = get_multilca_data_objs(
        functional_units={
            "a": {get_activity(("food", "1")).id: 1},
            "b": {get_activity(("food2", "2")).id: 2},
        },
        method_config={
            "impact_categories": [("foo",)],
            "normalizations": {("normal",): [("foo",)]},
            "weightings": {("weight",): [("normal",)]},
        },
    )
    object_names = [obj.metadata["name"] for obj in objs]

    assert Database("biosphere").datapackage().metadata["name"] in object_names
    assert Database("food").datapackage().metadata["name"] in object_names
    assert Database("food2").datapackage().metadata["name"] in object_names
    assert Normalization(("normal",)).datapackage().metadata["name"] in object_names
    assert Weighting(("weight",)).datapackage().metadata["name"] in object_names
    assert Method(("foo",)).datapackage().metadata["name"] in object_names


@bw2test
def test_get_multilca_data_objs_partial():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Database("food2").write(food2)
    Method(("foo",)).write(lcia)
    Normalization(("normal",)).write(
        [
            (("biosphere", "1"), 10),
            (("biosphere", "2"), 20),
        ]
    )
    Weighting(("weight",)).write([42])

    objs = get_multilca_data_objs(
        functional_units={
            "a": {get_activity(("food", "1")).id: 1},
            "b": {get_activity(("food2", "2")).id: 2},
        },
        method_config={
            "impact_categories": [("foo",)],
            "weightings": {("weight",): [("foo",)]},
        },
    )
    object_names = [obj.metadata["name"] for obj in objs]

    assert Database("biosphere").datapackage().metadata["name"] in object_names
    assert Database("food").datapackage().metadata["name"] in object_names
    assert Database("food2").datapackage().metadata["name"] in object_names
    assert Normalization(("normal",)).datapackage().metadata["name"] not in object_names
    assert Weighting(("weight",)).datapackage().metadata["name"] in object_names
    assert Method(("foo",)).datapackage().metadata["name"] in object_names


@bw2test
def test_get_multilca_data_objs_errors_fu_unknown_object():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Database("food2").write(food2)
    Method(("foo",)).write(lcia)
    Normalization(("normal",)).write(
        [
            (("biosphere", "1"), 10),
            (("biosphere", "2"), 20),
        ]
    )
    Weighting(("weight",)).write([42])

    with pytest.raises(UnknownObject):
        get_multilca_data_objs(functional_units={"a": {12345: 1}}, method_config={})


@bw2test
def test_get_multilca_data_objs_errors_fu_wrong_type():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Database("food2").write(food2)
    Method(("foo",)).write(lcia)
    Normalization(("normal",)).write(
        [
            (("biosphere", "1"), 10),
            (("biosphere", "2"), 20),
        ]
    )
    Weighting(("weight",)).write([42])

    with pytest.raises(ValueError):
        get_multilca_data_objs(
            functional_units={"a": {("food2", "2"): 1}}, method_config={}
        )
    with pytest.raises(ValueError):
        get_multilca_data_objs(
            functional_units={"a": {get_activity(("food", "1")): 1}}, method_config={}
        )


@bw2test
def test_get_multilca_data_objs_errors_missing_ic():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Database("food2").write(food2)
    Normalization(("normal",)).write(
        [
            (("biosphere", "1"), 10),
            (("biosphere", "2"), 20),
        ]
    )
    Weighting(("weight",)).write([42])

    with pytest.raises(ValueError):
        get_multilca_data_objs(
            functional_units={
                "a": {get_activity(("food", "1")).id: 1},
                "b": {get_activity(("food2", "2")).id: 2},
            },
            method_config={
                "impact_categories": [("foo",)],
                "normalizations": {("normal",): [("foo",)]},
                "weightings": {("weight",): [("normal",)]},
            },
        )


@bw2test
def test_get_multilca_data_objs_errors_missing_n():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Database("food2").write(food2)
    Method(("foo",)).write(lcia)
    Weighting(("weight",)).write([42])

    with pytest.raises(ValueError):
        get_multilca_data_objs(
            functional_units={
                "a": {get_activity(("food", "1")).id: 1},
                "b": {get_activity(("food2", "2")).id: 2},
            },
            method_config={
                "impact_categories": [("foo",)],
                "normalizations": {("normal",): [("foo",)]},
                "weightings": {("weight",): [("normal",)]},
            },
        )


@bw2test
def test_get_multilca_data_objs_errors_missing_w():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Database("food2").write(food2)
    Method(("foo",)).write(lcia)
    Normalization(("normal",)).write(
        [
            (("biosphere", "1"), 10),
            (("biosphere", "2"), 20),
        ]
    )

    with pytest.raises(ValueError):
        get_multilca_data_objs(
            functional_units={
                "a": {get_activity(("food", "1")).id: 1},
                "b": {get_activity(("food2", "2")).id: 2},
            },
            method_config={
                "impact_categories": [("foo",)],
                "normalizations": {("normal",): [("foo",)]},
                "weightings": {("weight",): [("normal",)]},
            },
        )
