from bw2data.tests import BW2DataTest, bw2test
from bw2data import *
from .fixtures import food, biosphere


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
