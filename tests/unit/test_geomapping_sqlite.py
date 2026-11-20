"""``geomapping`` stored in the ``Geocollection`` and ``GeoMappingRecord`` SQLite tables."""

import multiprocessing
import pickle

import pytest
from peewee import IntegrityError

from bw2data import config, geomapping, preferences
from bw2data.backends.schema import GeoMappingRecord, Geocollection
from bw2data.project import projects
from bw2data.tests import bw2test


def _write_legacy_pickle(data):
    with open(projects.dir / "geomapping.pickle", "wb") as f:
        pickle.dump(data, f, protocol=4)
    # Same as a project switch: forget that this project was already checked.
    geomapping.__init__()


# ----------------------------------------------------------------------
# Defaults and keys
# ----------------------------------------------------------------------


@bw2test
def test_glo_present_by_default():
    assert "GLO" in geomapping
    assert len(geomapping) == 1
    assert geomapping.collections() == []


@bw2test
def test_custom_global_location_present():
    preferences["global_location"] = "World"
    geomapping.__init__()
    assert config.global_location == "World"
    assert "GLO" in geomapping
    assert "World" in geomapping


@bw2test
def test_add_bare_and_collection_keys():
    geomapping.add(["CH", ("world", "CH"), ("places", "L")])

    assert geomapping["CH"] != geomapping[("world", "CH")]
    assert ("places", "L") in geomapping
    assert ["places", "L"] in geomapping
    assert geomapping.collections() == ["places", "world"]
    assert set(geomapping) == {"GLO", "CH", ("world", "CH"), ("places", "L")}
    assert geomapping.locations("places") == {("places", "L"): geomapping[("places", "L")]}
    assert set(geomapping.locations()) == {"GLO", "CH"}


@bw2test
def test_add_is_idempotent():
    geomapping.add(["CH", ("world", "DE")])
    ids = dict(geomapping.items())
    geomapping.add(["CH", ("world", "DE")])
    geomapping.__init__()  # drop the cache, so SQLite is checked too
    geomapping.add(["CH", ("world", "DE")])

    assert dict(geomapping.items()) == ids
    assert GeoMappingRecord.select().count() == 3
    assert Geocollection.select().count() == 1


@bw2test
def test_bare_locations_are_unique_in_sqlite():
    # NULL geocollections must not slip past the unique index
    GeoMappingRecord.create(location="CH")
    with pytest.raises(IntegrityError):
        GeoMappingRecord.create(location="CH")


@bw2test
def test_add_accepts_dict_keys():
    geomapping.add({("topo", "a"): [1, 2], ("topo", "b"): [3]})
    assert ("topo", "a") in geomapping
    assert ("topo", "b") in geomapping


@bw2test
def test_add_assigns_ids_in_order():
    geomapping.add(["a", "b", "c"])
    assert geomapping["a"] < geomapping["b"] < geomapping["c"]


@pytest.mark.parametrize(
    "key",
    [("world", 5), ("world", ""), ("", "CH"), "", None, 5, ("a", "b", "c"), (("a",), "b")],
)
@bw2test
def test_invalid_keys_rejected(key):
    with pytest.raises(ValueError):
        geomapping.add([key])
    with pytest.raises(ValueError):
        geomapping[key]
    with pytest.raises(ValueError):
        key in geomapping


@bw2test
def test_activity_with_invalid_location_not_saved():
    from bw2data import Database
    from bw2data.backends import ActivityDataset

    Database("db").register()
    node = Database("db").new_node(code="a", name="a", location=("world", 5), type="process")
    with pytest.raises(ValueError):
        node.save()
    assert not ActivityDataset.select().where(ActivityDataset.code == "a").exists()

    node["location"] = ("world", "5")
    node.save()
    assert ("world", "5") in geomapping


@bw2test
def test_setitem_not_allowed():
    with pytest.raises(NotImplementedError):
        geomapping["foo"] = 7


@bw2test
def test_flush_is_deprecated():
    with pytest.warns(DeprecationWarning):
        geomapping.flush()


@bw2test
def test_data_property():
    geomapping.add([("world", "CH")])
    assert geomapping.data == {"GLO": geomapping["GLO"], ("world", "CH"): geomapping[("world", "CH")]}


# ----------------------------------------------------------------------
# Deleting
# ----------------------------------------------------------------------


@bw2test
def test_delete():
    geomapping.add(["foo", ("world", "CH")])
    geomapping.delete(["foo"])
    del geomapping[("world", "CH")]

    assert "foo" not in geomapping
    assert ("world", "CH") not in geomapping
    with pytest.raises(KeyError):
        del geomapping["foo"]


@bw2test
def test_ids_not_reused_after_delete():
    geomapping.add(["a", "b"])
    highest = geomapping["b"]
    geomapping.delete(["b"])
    geomapping.add(["c"])
    assert geomapping["c"] > highest


@bw2test
def test_geocollection_with_locations_cant_be_deleted():
    geomapping.add([("world", "CH")])
    with pytest.raises(IntegrityError):
        Geocollection.delete().where(Geocollection.name == "world").execute()


@bw2test
def test_geocollection_extra():
    geomapping.add([("world", "CH")])
    Geocollection.update(extra={"field": "iso2"}).where(Geocollection.name == "world").execute()
    assert Geocollection.get(Geocollection.name == "world").extra == {"field": "iso2"}


# ----------------------------------------------------------------------
# Cache
# ----------------------------------------------------------------------


@bw2test
def test_finds_keys_added_by_other_process():
    assert "elsewhere" not in geomapping
    # Simulate another process writing directly to SQLite
    GeoMappingRecord.create(location="elsewhere")
    assert "elsewhere" in geomapping


@bw2test
def test_project_switch():
    geomapping.add(["only-here"])
    projects.set_current("other")
    assert "only-here" not in geomapping
    assert "GLO" in geomapping


# ----------------------------------------------------------------------
# Migration from ``geomapping.pickle``
# ----------------------------------------------------------------------


@bw2test
def test_migrates_pickle_keeping_ids():
    GeoMappingRecord.delete().execute()
    legacy = {"GLO": 1, "CH": 5, ("world", "DE"): 9, ("places", "L"): 12}
    _write_legacy_pickle(legacy)

    with pytest.warns(UserWarning, match="Migrating"):
        assert dict(geomapping.items()) == legacy

    assert geomapping.collections() == ["places", "world"]
    assert (projects.dir / "geomapping.pickle").exists()
    geomapping.add(["new"])
    assert geomapping["new"] > 12


@bw2test
def test_migration_converts_integer_feature_ids():
    GeoMappingRecord.delete().execute()
    _write_legacy_pickle({"GLO": 1, ("world", 5): 7})

    with pytest.warns(UserWarning, match="Converted 1 integer feature ids"):
        assert geomapping[("world", "5")] == 7


@bw2test
def test_migration_fails_on_conflicting_keys():
    GeoMappingRecord.delete().execute()
    _write_legacy_pickle({"GLO": 1, ("world", 5): 7, ("world", "5"): 8})

    with pytest.raises(ValueError, match="more than once"):
        "GLO" in geomapping
    assert GeoMappingRecord.select().count() == 0
    assert Geocollection.select().count() == 0


@bw2test
def test_migration_fails_on_invalid_keys():
    GeoMappingRecord.delete().execute()
    _write_legacy_pickle({"GLO": 1, None: 2, ("a", "b", "c"): 3})

    with pytest.raises(ValueError, match="invalid keys"):
        len(geomapping)
    assert GeoMappingRecord.select().count() == 0


@bw2test
def test_migration_skipped_when_table_already_populated():
    geomapping.add(["existing"])
    _write_legacy_pickle({"GLO": 1, "CH": 5})

    assert "existing" in geomapping
    assert "CH" not in geomapping


@bw2test
def test_unreadable_pickle_warns_and_starts_fresh():
    GeoMappingRecord.delete().execute()
    (projects.dir / "geomapping.pickle").write_bytes(b"not a pickle")
    geomapping.__init__()

    with pytest.warns(UserWarning, match="Can't read"):
        assert "GLO" in geomapping


# ----------------------------------------------------------------------
# Concurrency
# ----------------------------------------------------------------------


def _add_in_other_process(base_dir, project_name, keys, barrier):
    from pathlib import Path

    from bw2data import geomapping as gm
    from bw2data.project import projects as p

    base_dir = Path(base_dir)
    p.change_base_directories(
        base_dir=base_dir, base_logs_dir=base_dir, project_name=project_name, update=False
    )
    barrier.wait()
    # Small batches, like real callers which add the locations of a dataset or method at once
    for i in range(0, len(keys), 10):
        gm.add(keys[i : i + 10])


@bw2test
def test_concurrent_adds_get_unique_ids():
    base_dir = str(projects._base_data_dir)
    keys = [f"loc-{i}" for i in range(100)] + [("world", f"f-{i}") for i in range(100)]
    "GLO" in geomapping  # create the tables before the workers start

    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(4)
    workers = [
        ctx.Process(
            target=_add_in_other_process,
            # Overlapping key sets, in different orders
            args=(base_dir, projects.current, keys[i::2] + keys[::-1], barrier),
        )
        for i in range(4)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=120)
        assert worker.exitcode == 0

    mapping = dict(geomapping.items())
    assert set(keys) <= set(mapping)
    assert len(mapping) == len(keys) + 1
    assert len(set(mapping.values())) == len(mapping)
