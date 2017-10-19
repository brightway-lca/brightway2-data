from __future__ import print_function

from future.utils import PY2
from bw2data.tests import BW2DataTest, bw2test
from bw2data import *
from .fixtures import food, biosphere


@bw2test
def test_repr_str_unicode():
    objects = (mapping, geomapping, databases, methods,
               normalizations, weightings, Database("foo"), DataStore("foo"),
               projects)
    for obj in objects:
        assert repr(obj)
        assert str(obj)
        print(obj)
        if PY2:
            assert unicode(obj)


@bw2test
def test_registered_database_repr():
    d = Database("biosphere")
    d.write(biosphere)
    assert repr(d)
    assert str(d)
    print(d)
