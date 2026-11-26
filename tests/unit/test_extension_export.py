"""Building read-only LCI extensions (extensions PR 6): ``projects.export_extension``, and what
the project does with location names and processed data when extensions are attached and
detached."""

import json
import sqlite3

import pytest

from bw2data import Database, Method, databases, geomapping, get_node, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.extensions import (
    INFO_BW2DATA_VERSION,
    INFO_CREATED,
    INFO_FINGERPRINTS,
    INFO_ID,
    INFO_VERSION,
    database_fingerprint,
)
from bw2data.backends.migrations import SCHEMA_VERSION
from bw2data.errors import ExtensionDependencyError, ExtensionError, ReadOnlyExtensionError
from bw2data.search import IndexManager
from bw2data.search.indices import index_table
from bw2data.signals import on_extension_attached, on_extension_detached

from .test_extension_attach import _temporary_directory, lci_path

BIO = {
    ("bio", str(i)): {"name": f"flow {i}", "type": "emission", "unit": "kg"} for i in range(3)
}
INV = {
    ("inv", "a"): {
        "name": "steel",
        "unit": "kg",
        "location": "CH",
        "exchanges": [
            {"input": ("inv", "a"), "amount": 1, "type": "production"},
            {"input": ("bio", "0"), "amount": 2, "type": "biosphere"},
        ],
    },
    ("inv", "b"): {
        "name": "bicycle",
        "unit": "unit",
        "location": ("world", "Mars"),
        "exchanges": [
            {"input": ("inv", "b"), "amount": 1, "type": "production"},
            {"input": ("inv", "a"), "amount": 0.5, "type": "technosphere"},
        ],
    },
}


def file_rows(path, sql, params=()):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def info(path):
    return dict(file_rows(path, "SELECT key, value FROM extension_info"))


@pytest.fixture
def built(tmp_path):
    """``bio.db``, exported from project ``bio-builder``, and ``inv.db`` (requires ``bio``),
    exported from project ``inv-builder``, which has ``bio.db`` attached. Then a new, empty
    project ``user`` is current."""
    _temporary_directory()
    bio, inv = tmp_path / "bio.db", tmp_path / "inv.db"
    projects.set_current("bio-builder")
    Database("bio").write(BIO)
    projects.export_extension(bio, ["bio"], version="1.0")
    projects.set_current("inv-builder")
    projects.attach_extension(bio)
    Database("inv").write(INV)
    projects.export_extension(inv, "inv", version="2.0")
    projects.set_current("user")
    yield bio, inv
    sqlite3_lci_db.change_path(lci_path())


# The file


def test_file_contents(built):
    bio, inv = built
    data = info(inv)
    assert set(data) == {
        INFO_ID,
        INFO_VERSION,
        INFO_FINGERPRINTS,
        INFO_BW2DATA_VERSION,
        INFO_CREATED,
    }
    assert data[INFO_VERSION] == "2.0"
    assert file_rows(inv, "PRAGMA user_version") == [(SCHEMA_VERSION,)]
    assert file_rows(inv, "PRAGMA page_size") == [(16384,)]
    conn = sqlite3.connect(inv)
    fingerprints = {"inv": database_fingerprint(conn, "main", "inv")}
    conn.close()
    assert json.loads(data[INFO_FINGERPRINTS]) == fingerprints
    # Requires ``bio`` in exactly the version which was attached
    bio_info = info(bio)
    assert file_rows(inv, "SELECT * FROM extension_requires") == [
        ("bio", bio_info[INFO_ID], json.loads(bio_info[INFO_FINGERPRINTS])["bio"])
    ]
    assert file_rows(bio, "SELECT * FROM extension_requires") == []
    assert file_rows(inv, "SELECT DISTINCT database FROM activitydataset") == [("inv",)]
    assert file_rows(inv, "SELECT DISTINCT output_database FROM exchangedataset") == [("inv",)]
    ((name, depends, dirty, searchable, extra),) = file_rows(
        inv, "SELECT name, depends, dirty, searchable, extra FROM databaserecord"
    )
    assert (name, json.loads(depends), dirty, searchable) == ("inv", ["bio"], 0, 1)
    assert "processed" not in json.loads(extra or "{}")
    # Project-local tables are empty
    for table in ("preference", "geomappingrecord", "extensiondatabasestate", "method"):
        assert file_rows(inv, f'SELECT count(*) FROM "{table}"') == [(0,)], table
    # The schema of the project's file, with its indexes
    project = {
        row
        for row in file_rows(
            lci_path(),
            "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'searchindex%' "
            "AND name NOT LIKE 'sqlite_%'",
        )
    }
    exported = set(
        file_rows(
            inv,
            "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'searchindex%' "
            "AND name NOT LIKE 'sqlite_%'",
        )
    )
    assert exported == project | {
        ("table", "extension_info"),
        ("table", "extension_requires"),
        # Which a project's file only has after a bulk write
        ("index", "activitydataset_key"),
        ("index", "exchangedataset_input"),
        ("index", "exchangedataset_output"),
    }


def test_ids(built, tmp_path):
    projects.set_current("inv-builder")
    activities = file_rows(lci_path(), "SELECT id, code FROM activitydataset WHERE database = 'inv'")
    exchanges = file_rows(lci_path(), "SELECT id FROM exchangedataset")
    path = tmp_path / "again.db"
    projects.export_extension(path, ["inv"])
    # Activities keep their ids, exchanges get new ones
    assert sorted(file_rows(path, "SELECT id, code FROM activitydataset")) == sorted(activities)
    new = file_rows(path, "SELECT id FROM exchangedataset")
    assert len(new) == len(exchanges) == 4
    assert not set(new) & set(exchanges)
    # A new id for every file
    assert info(path)[INFO_ID] != info(built[1])[INFO_ID]


def test_several_databases_in_one_file(built, tmp_path):
    projects.set_current("both")
    Database("bio").write(BIO)
    Database("inv").write(INV)
    path = tmp_path / "both.db"
    projects.export_extension(path, ["inv", "bio"])
    assert file_rows(path, "SELECT * FROM extension_requires") == []
    assert set(json.loads(info(path)[INFO_FINGERPRINTS])) == {"bio", "inv"}
    projects.set_current("user")
    projects.attach_extension(path)
    assert sorted(databases) == ["bio", "inv"]
    assert databases["inv"]["depends"] == ["bio"]
    assert databases["bio"]["depends"] == []


def test_compressed_values_can_be_read(tmp_path):
    # At least 500 datasets: a zstd dictionary is trained and stored
    _temporary_directory()
    projects.set_current("source")
    Database("big").write(
        {("big", str(i)): {"name": f"activity {i}", "comment": "same " * 20} for i in range(500)}
    )
    # Written in a transaction, so compressed with the dictionary trained on ``big``
    Database("small").write({("small", "x"): {"name": "little", "comment": "same " * 20}})
    ((project_dictionary,),) = file_rows(lci_path(), "SELECT id FROM zstddictionary")
    used = file_rows(
        lci_path(), "SELECT hex(substr(data, 1, 4)) FROM activitydataset WHERE database = 'small'"
    )
    assert used == [("28B52FFD",)]
    big, small = tmp_path / "big.db", tmp_path / "small.db"
    projects.export_extension(big, ["big"])
    projects.export_extension(small, ["small"])
    # The project's dictionary holds pieces of all its data, and is in neither file. ``big``
    # gets a new one, trained on its values alone; ``small`` needs none
    ((new_dictionary, active),) = file_rows(big, "SELECT id, active FROM zstddictionary")
    assert new_dictionary != project_dictionary and active == 0
    assert file_rows(small, "SELECT count(*) FROM zstddictionary") == [(0,)]
    projects.set_current("target")
    try:
        projects.attach_extension([big, small])
        assert get_node(database="big", code="7")["comment"] == "same " * 20
        assert get_node(database="small", code="x")["comment"] == "same " * 20
        assert [node["code"] for node in Database("small").search("little")] == ["x"]
    finally:
        sqlite3_lci_db.change_path(lci_path())


# Search


def test_search_index_is_copied(built):
    bio, inv = built
    tables = file_rows(inv, "SELECT name FROM sqlite_master WHERE name LIKE 'searchindex%'")
    assert {table for (table,) in tables if "_" not in table[len("searchindex_") :]} == {
        index_table("inv")
    }
    projects.attach_extension([bio, inv])
    index = IndexManager("inv")
    assert index.read_only
    assert [node["code"] for node in Database("inv").search("bicycle")] == ["b"]
    # Nothing was built in the project
    assert file_rows(
        lci_path(), "SELECT count(*) FROM sqlite_master WHERE name LIKE 'searchindex%'"
    ) == [(0,)]


def test_search_index_is_built_if_the_project_has_none(built, tmp_path):
    projects.set_current("bio-builder")
    sqlite3_lci_db.execute_sql(f'DROP TABLE main."{index_table("bio")}"')
    path = tmp_path / "bio-again.db"
    projects.export_extension(path, ["bio"])
    projects.set_current("user")
    projects.attach_extension(path)
    assert IndexManager("bio").read_only
    assert [node["code"] for node in Database("bio").search("flow 2")][0] == "2"


def test_unsearchable_database_has_no_index(built, tmp_path):
    projects.set_current("bio-builder")
    Database("bio").make_unsearchable()
    path = tmp_path / "bio-hidden.db"
    projects.export_extension(path, ["bio"])
    assert file_rows(path, "SELECT name FROM sqlite_master WHERE name LIKE 'searchindex%'") == []
    assert file_rows(path, "SELECT searchable FROM databaserecord") == [(0,)]


# Refusals


def test_links_to_editable_databases_are_refused(built, tmp_path):
    projects.set_current("bio-builder")
    Database("mine").write(
        {
            ("mine", "x"): {
                "exchanges": [{"input": ("bio", "1"), "amount": 1, "type": "biosphere"}]
            }
        }
    )
    path = tmp_path / "mine.db"
    with pytest.raises(ExtensionError, match="'mine' links to database 'bio', which is neither"):
        projects.export_extension(path, ["mine"])
    assert not path.exists()
    assert list(tmp_path.glob(".mine.db*")) == []
    # Exported together, it works
    projects.export_extension(path, ["mine", "bio"])


def test_databases_of_extensions_are_refused(built, tmp_path):
    projects.set_current("inv-builder")
    with pytest.raises(ExtensionError, match="stored in the read-only extension 'bio'"):
        projects.export_extension(tmp_path / "x.db", ["bio"])


def test_unknown_and_iotable_databases_are_refused(built, tmp_path):
    with pytest.raises(ExtensionError, match="no database 'nope'"):
        projects.export_extension(tmp_path / "x.db", ["nope"])
    databases["io"] = {"backend": "iotable"}
    databases.flush()
    with pytest.raises(ExtensionError, match="'io' is an IO table database"):
        projects.export_extension(tmp_path / "x.db", ["io"])
    with pytest.raises(ValueError):
        projects.export_extension(tmp_path / "x.db", [])
    with pytest.raises(ValueError):
        projects.export_extension(tmp_path / "x.db", ["io", "io"])


def test_existing_file(built):
    bio, inv = built
    projects.set_current("bio-builder")
    old_id = info(bio)[INFO_ID]
    with pytest.raises(FileExistsError):
        projects.export_extension(bio, ["bio"])
    assert info(bio)[INFO_ID] == old_id
    new_id = projects.export_extension(bio, ["bio"], overwrite=True)
    assert info(bio)[INFO_ID] == new_id != old_id


def test_files_of_the_project_are_never_replaced(built):
    bio, inv = built
    projects.set_current("inv-builder")
    with pytest.raises(ExtensionError, match="the project's own file"):
        projects.export_extension(lci_path(), ["inv"], overwrite=True)
    with pytest.raises(ExtensionError, match="attached extension 'bio'"):
        projects.export_extension(bio, ["inv"], overwrite=True)
    assert sorted(databases) == ["bio", "inv"]
    assert len(Database("bio")) == 3


def test_failed_attach_steps_change_nothing(built, monkeypatch):
    bio, inv = built
    sent = []
    on_extension_attached.connect(lambda sender, **kwargs: sent.append(kwargs), weak=False)

    def fail(keys):
        raise RuntimeError("no geomapping today")

    monkeypatch.setattr(geomapping, "add", fail)
    with pytest.raises(RuntimeError, match="no geomapping today"):
        projects.attach_extension(bio)
    assert projects.extensions == ()
    assert sqlite3_lci_db.extensions == ()
    assert sorted(databases) == []
    assert sent == []
    monkeypatch.undo()
    projects.attach_extension(bio)
    assert sorted(databases) == ["bio"]
    assert len(sent) == 1


def test_detach_with_processed_data_which_cannot_be_removed(built):
    bio, inv = built
    projects.attach_extension(bio)
    processed = Database("bio").filepath_processed()
    # Stands for a file which is still open, on Windows
    processed.unlink()
    processed.mkdir()
    sent = []
    on_extension_detached.connect(lambda sender, **kwargs: sent.append(kwargs), weak=False)
    with pytest.warns(UserWarning, match="Can't remove the processed data of database 'bio'"):
        projects.detach_extension("bio")
    assert projects.extensions == ()
    assert len(sent) == 1


def test_not_inside_a_transaction(built, tmp_path):
    projects.set_current("bio-builder")
    with sqlite3_lci_db.atomic():
        with pytest.raises(RuntimeError, match="transaction"):
            projects.export_extension(tmp_path / "x.db", ["bio"])


# Using the files


def test_round_trip(built):
    from bw2calc import LCA

    from bw2data import prepare_lca_inputs

    bio, inv = built
    # ``inv`` needs ``bio``
    with pytest.raises(ExtensionDependencyError):
        projects.attach_extension(inv)
    projects.attach_extension([inv, bio])
    assert [e.name for e in projects.extensions] == ["bio", "inv"]
    # Location names get geomapping ids in the project
    assert "CH" in geomapping and ("world", "Mars") in geomapping

    with pytest.raises(ReadOnlyExtensionError):
        get_node(database="inv", code="a").delete()
    Database("mine").write(
        {
            ("mine", "x"): {
                "name": "ride",
                "exchanges": [
                    {"input": ("inv", "b"), "amount": 1, "type": "technosphere"},
                    {"input": ("bio", "1"), "amount": 3, "type": "biosphere"},
                ],
            }
        }
    )
    Method(("m",)).write([(("bio", "0"), 10), (("bio", "1"), 1)])
    demand, data_objs, remapping = prepare_lca_inputs(
        {get_node(database="mine", code="x"): 1}, method=("m",)
    )
    lca = LCA(demand, data_objs=data_objs, remapping_dicts=remapping)
    lca.lci()
    lca.lcia()
    # 0.5 kg steel with 2 kg of flow 0 each (10), and 3 kg of flow 1 (1)
    assert lca.score == pytest.approx(13)

    with pytest.raises(ExtensionError):
        projects.detach_extension("bio")
    processed = Database("inv").filepath_processed()
    assert processed.exists()
    del databases["mine"]
    Method(("m",)).deregister()
    projects.detach_extension("inv")
    # The processed data of the extension's databases is removed with it
    assert not processed.exists()
    projects.detach_extension("bio")
    assert projects.extensions == ()
    assert sorted(databases) == []


def test_processing_adds_missing_locations(built):
    bio, inv = built
    projects.attach_extension([bio, inv])
    # E.g. a project copied before the locations were added
    from bw2data.backends.schema import GeoMappingRecord

    GeoMappingRecord.delete().where(GeoMappingRecord.location == "CH").execute()
    geomapping._cache.clear()
    assert "CH" not in geomapping
    Database("inv").process()
    assert "CH" in geomapping
