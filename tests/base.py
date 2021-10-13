from bw2data import databases, geomapping, methods, projects
from bw2data.tests import bw2test


@bw2test
def test_bw2test_decorator():
    assert list(databases) == []
    assert list(methods) == []
    assert len(geomapping) == 1
    assert "GLO" in geomapping
    assert len(projects) == 2
    assert "default" in projects
