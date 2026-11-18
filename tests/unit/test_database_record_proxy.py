"""Writes through a ``DatabaseRecordProxy`` must not clobber changes made via other handles."""

import pytest

from bw2data import databases
from bw2data.tests import bw2test


@bw2test
def test_stale_proxy_write_keeps_known_field_changes():
    databases["a"] = {"backend": "sqlite"}
    stale = databases["a"]

    databases.set_dirty("a")
    databases["a"]["searchable"] = True

    stale["format"] = "foo"

    fresh = databases["a"]
    assert fresh["dirty"] is True
    assert "modified" in fresh
    assert fresh["searchable"] is True
    assert fresh["format"] == "foo"
    assert fresh["backend"] == "sqlite"


@bw2test
def test_stale_proxy_write_merges_extra_fields():
    databases["a"] = {"backend": "sqlite"}
    stale = databases["a"]

    databases["a"]["first"] = 1
    stale["second"] = 2

    assert dict(databases["a"]) == {"backend": "sqlite", "first": 1, "second": 2}


@bw2test
def test_stale_proxy_delete_keeps_other_changes():
    databases["a"] = {"backend": "sqlite", "first": 1, "second": 2}
    stale = databases["a"]

    databases["a"]["third"] = 3
    databases.set_dirty("a")
    del stale["first"]

    fresh = databases["a"]
    assert "first" not in fresh
    assert fresh["second"] == 2
    assert fresh["third"] == 3
    assert fresh["dirty"] is True

    del stale["backend"]
    assert "backend" not in databases["a"]
    assert databases["a"]["third"] == 3


@bw2test
def test_proxy_delete_missing_extra_key_raises():
    databases["a"] = {"backend": "sqlite"}
    with pytest.raises(KeyError):
        del databases["a"]["missing"]


@bw2test
def test_proxy_reflects_own_writes():
    databases["a"] = {"backend": "sqlite"}
    proxy = databases["a"]
    proxy["searchable"] = False
    proxy["custom"] = [1, 2]
    assert proxy["searchable"] is False
    assert proxy["custom"] == [1, 2]
    del proxy["custom"]
    assert "custom" not in proxy
