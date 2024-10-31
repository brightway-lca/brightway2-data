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
    get_node,
    get_multilca_data_objs,
    get_node,
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
        # Make sure can be printed - not for debugging
        print(obj)


@bw2test
def test_registered_database_repr():
    d = Database("biosphere")
    d.write(biosphere)
    assert repr(d)
    assert str(d)
    # Make sure can be printed - not for debugging
    print(d)


@pytest.fixture
@bw2test
def setup():
    Database("biosphere").write(biosphere)
    Database("food").write(food)
    Method(("foo",)).write(lcia)

    return Database("biosphere"), Database("food"), Method(("foo",))


def test_prepare_lca_inputs_basic(setup):
    d, objs, r = prepare_lca_inputs(demand={("food", "1"): 1}, method=("foo",))
    # ID is 3; two biosphere flows, then '1' is next written
    assert list(d.values()) == [1]
    assert {o.metadata["id"] for o in objs} == {o.datapackage().metadata["id"] for o in setup}

    b1 = get_node(database="biosphere", code='1').id
    b2 = get_node(database="biosphere", code='2').id
    f1 = get_node(database="food", code='1').id
    f2 = get_node(database="food", code='2').id

    remapping_expected = {
        "activity": {
            b1: ("biosphere", "1"),
            b2: ("biosphere", "2"),
            f1: ("food", "1"),
            f2: ("food", "2"),
        },
        "product": {
            b1: ("biosphere", "1"),
            b2: ("biosphere", "2"),
            f1: ("food", "1"),
            f2: ("food", "2"),
        },
        "biosphere": {
            b1: ("biosphere", "1"),
            b2: ("biosphere", "2"),
            f1: ("food", "1"),
            f2: ("food", "2"),
        },
    }
    assert r == remapping_expected


def test_prepare_lca_inputs_only_method(setup):
    d, objs, r = prepare_lca_inputs(method=("foo",))
    # ID is 3; two biosphere flows, then '1' is next written
    assert d is None
    assert [o.metadata["id"] for o in objs] == [
        o.datapackage().metadata["id"] for o in [Method(("foo",))]
    ]


def test_prepare_lca_inputs_multiple_demands_data_types(setup):
    first = get_node(database="food", code="1")
    second = get_node(database="food", code="2")
    d, objs, r = prepare_lca_inputs(demands=[{first: 1}, {second.id: 10}], method=("foo",))
    assert d == [{first.id: 1}, {second.id: 10}]
    assert {o.metadata["id"] for o in objs} == {o.datapackage().metadata["id"] for o in setup}


def test_prepare_lca_inputs_multiple_demands(setup):
    d, objs, r = prepare_lca_inputs(
        demands=[{("food", "1"): 1}, {("food", "2"): 10}], method=("foo",)
    )
    f1 = get_node(database="food", code='1').id
    f2 = get_node(database="food", code='2').id
    assert d == [{f1: 1}, {f2: 10}]
    assert {o.metadata["id"] for o in objs} == {o.datapackage().metadata["id"] for o in setup}


def test_prepare_lca_inputs_database_ordering(setup):
    d, objs, r = prepare_lca_inputs(
        demands=[{("food", "1"): 1}, {("food", "2"): 10}],
        method=("foo",),
        demand_database_last=False,
    )
    assert {o.metadata["id"] for o in objs} == {o.datapackage().metadata["id"] for o in setup}


def test_prepare_lca_inputs_remapping(setup):
    d, objs, r = prepare_lca_inputs(demand={("food", "1"): 1}, method=("foo",), remapping=False)
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
        get_multilca_data_objs(functional_units={"a": {("food2", "2"): 1}}, method_config={})
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
