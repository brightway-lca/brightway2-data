from bw2data import databases, methods, mapping, geomapping, projects
from bw2data.tests import bw2test


@bw2test
def test_bw2test_decorator():
    assert list(databases) == []
    assert list(methods) == []
    assert not len(mapping)
    assert len(geomapping) == 1
    assert "GLO" in geomapping
    assert len(projects) == 1
    assert [x.name for x in projects] == ["default"]
