"""Location ids derived from location names (#331): the same in every project and every file.
Schema migration 10 gives existing locations these ids and makes processed data be processed
again."""

import pytest

from bw2data import Database, Method, geomapping, get_node, prepare_lca_inputs, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import migrate, schema_version
from bw2data.backends.schema import GeoMappingRecord, Geocollection
from bw2data.meta import location_id
from bw2data.tests import bw2test

from .test_extension_export import built  # noqa: F401


def write_and_process():
    Database("bio").write({("bio", "co2"): {"name": "CO2", "type": "emission"}})
    Database("inv").write(
        {
            ("inv", "a"): {
                "name": "a",
                "location": ("world", "CH"),
                "exchanges": [
                    {"input": ("inv", "a"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 2, "type": "biosphere"},
                ],
            }
        }
    )
    Method(("m",)).write([(("bio", "co2"), 3.0), (("bio", "co2"), 100.0, "CH")])
    Database("inv").process()


def score():
    from bw2calc import LCA

    demand, data_objs, remapping = prepare_lca_inputs(
        {get_node(database="inv", code="a"): 1}, method=("m",)
    )
    lca = LCA(demand, data_objs=data_objs, remapping_dicts=remapping)
    lca.lci()
    lca.lcia()
    return lca.score


def as_version_9(rows, collections=()):
    """``geomappingrecord`` with the ``rows`` (``(id, collection id, location)``), as written
    before schema migration 10."""
    GeoMappingRecord.delete().execute()
    for id_, name in collections:
        Geocollection.insert(id=id_, name=name).on_conflict_ignore().execute()
    for row in rows:
        sqlite3_lci_db.execute_sql(
            'INSERT INTO main."geomappingrecord" ("id", "geocollection_id", "location") '
            "VALUES (?, ?, ?)",
            row,
        )
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 9")
    geomapping.__init__()


@bw2test
def test_migration_gives_ids_derived_from_names_and_merges_locations():
    as_version_9(
        [(1, None, "GLO"), (2, None, "Global"), (3, 50, "CH"), (4, None, "CH"), (5, 51, "X")],
        [(50, "world"), (51, "places")],
    )
    assert migrate(sqlite3_lci_db.db, projects.dir) == [10]
    assert schema_version(sqlite3_lci_db.db) == 10
    rows = set(
        sqlite3_lci_db.execute_sql(
            'SELECT "id", "geocollection_id", "location" FROM main."geomappingrecord"'
        ).fetchall()
    )
    assert rows == {
        (location_id("GLO"), None, "GLO"),
        (location_id("CH"), None, "CH"),
        (location_id(("places", "X")), 51, "X"),
    }
    assert dict(geomapping.items()) == {
        "GLO": location_id("GLO"),
        "CH": location_id("CH"),
        ("places", "X"): location_id(("places", "X")),
    }


@bw2test
def test_migration_is_postponed_if_two_locations_get_the_same_id(monkeypatch):
    from bw2data.backends import migrations

    as_version_9([(1, None, "GLO"), (2, None, "CH")])
    monkeypatch.setattr(migrations, "_m10_id", lambda collection, location: 7)
    with pytest.warns(UserWarning, match="would get the same id"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == []
    assert schema_version(sqlite3_lci_db.db) == 9
    assert GeoMappingRecord.select().count() == 2


@bw2test
def test_processed_data_is_processed_again():
    write_and_process()
    before = score()
    database_file = Database("inv").filepath_processed()
    method_file = Method(("m",)).filepath_processed()
    assert database_file.is_file() and method_file.is_file()
    # Ids as before schema migration 10: the processed data uses them
    rows = [(id_ + 1, None, key) for id_, key in enumerate(["GLO", "CH"])]
    as_version_9(rows)
    assert migrate(sqlite3_lci_db.db, projects.dir) == [10]
    # Removed after the commit; processed again when they are needed
    assert not database_file.exists() and not method_file.exists()
    assert score() == pytest.approx(before) == pytest.approx(2 * 3)
    assert database_file.is_file() and method_file.is_file()
    dp = Database("inv").datapackage()
    (geo,) = [
        r for r in dp.resources if r["matrix"] == "inv_geomapping_matrix" and r["kind"] == "indices"
    ]
    assert dp.get_resource(geo["name"])[0]["col"].tolist() == [location_id("CH")]


@bw2test
def test_processed_data_of_io_tables_is_kept():
    Database("io", backend="iotable").write(
        {("io", "a"): {"name": "a", "location": "CH", "type": "process"}}
    )
    a = get_node(database="io", code="a")
    Database("io").write_exchanges(
        technosphere=[{"row": a.id, "col": a.id, "amount": 1, "flip": False}],
        biosphere=[],
        dependents=[],
    )
    path = Database("io").filepath_processed()
    assert path.is_file()
    as_version_9([(1, None, "GLO"), (2, None, "CH")])
    with pytest.warns(UserWarning, match="IO table databases keeps the location ids"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == [10]
    assert path.is_file()


@bw2test
def test_ids_are_the_same_in_every_project():
    geomapping.add(["CH", ("places", "X")])
    first = dict(geomapping.items())
    name = projects.current
    projects.set_current("other")
    geomapping.add([("places", "X"), ("world", "CH")])
    assert dict(geomapping.items()) == first
    projects.set_current(name)


def test_stored_datapackages_use_the_ids_of_the_attaching_project(built):  # noqa: F811
    bio, inv = built
    projects.attach_extension([bio, inv])
    dp = Database("inv").datapackage()
    (geo,) = [
        r for r in dp.resources if r["matrix"] == "inv_geomapping_matrix" and r["kind"] == "indices"
    ]
    columns = dp.get_resource(geo["name"])[0]["col"].tolist()
    # Built in another project, without any translation
    assert sorted(columns) == sorted([geomapping["CH"], geomapping[("world", "Mars")]])
