"""Processed datapackages stored in read-only LCI extension files (#352): written by
``projects.export_extension``, and read by ``Database.datapackage()`` instead of processing the
database in the project."""

import threading

import numpy as np
import pytest

from bw2data import Database, Method, databases, get_node, prepare_lca_inputs, projects
from bw2data.backends import SQLiteBackend, sqlite3_lci_db
from bw2data.backends.extensions import (
    DATAPACKAGE_TABLE,
    build_extension,
    has_stored_datapackage,
    stored_datapackage,
)
from bw2data.errors import ExtensionError

from .test_extension_attach import edit, lci_path
from .test_extension_export import BIO, built, file_rows  # noqa: F401


def arrays(dp, matrix):
    """``{kind: array}`` of the resources of ``matrix`` in the datapackage ``dp``."""
    return {
        resource["kind"]: dp.get_resource(resource["name"])[0]
        for resource in dp.resources
        if resource["matrix"] == matrix
    }


def no_processing(monkeypatch):
    def refuse(self, *args, **kwargs):
        raise AssertionError(f"database {self.name!r} was processed")

    monkeypatch.setattr(SQLiteBackend, "process", refuse)


def lca_score():
    from bw2calc import LCA

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
    return lca.score


# Writing


def test_export_stores_the_processed_datapackages(built):
    bio, inv = built
    assert file_rows(inv, f"SELECT database FROM {DATAPACKAGE_TABLE}") == [("inv",)]
    ((content,),) = file_rows(inv, f"SELECT content FROM {DATAPACKAGE_TABLE}")
    # As processed in the project which exported it
    projects.set_current("inv-builder")
    assert content == Database("inv").filepath_processed().read_bytes()


def test_export_processes_dirty_databases_first(built, tmp_path):
    projects.set_current("bio-builder")
    Database("bio").process()
    Database("bio").new_node(code="3", name="flow 3", type="emission", unit="kg").save()
    assert databases["bio"]["dirty"]
    target = tmp_path / "bio-2.db"
    projects.export_extension(target, "bio")
    assert not databases["bio"].get("dirty")
    projects.set_current("user")
    projects.attach_extension(target)
    dp = Database("bio").datapackage()
    assert len(arrays(dp, "inv_geomapping_matrix")["indices"]) == 0
    assert "bio" in dp.metadata["name"]


def test_export_processes_databases_without_processed_data(built, tmp_path):
    projects.set_current("bio-builder")
    Database("bio").filepath_processed().unlink()
    target = tmp_path / "bio-2.db"
    projects.export_extension(target, "bio")
    assert file_rows(target, f"SELECT database FROM {DATAPACKAGE_TABLE}") == [("bio",)]


def test_build_extension_checks_the_datapackages(built, tmp_path):
    projects.set_current("bio-builder")
    with pytest.raises(ValueError, match="not exported: \\['other'\\]"):
        build_extension(lci_path(), tmp_path / "x.db", ["bio"], datapackages={"other": "x"})
    not_a_zip = tmp_path / "not-a.zip"
    not_a_zip.write_bytes(b"nope")
    with pytest.raises(ExtensionError, match="'bio'.*is not a zip file"):
        build_extension(lci_path(), tmp_path / "x.db", ["bio"], datapackages={"bio": not_a_zip})
    assert not (tmp_path / "x.db").exists()
    # Without datapackages, as files built before #352
    build_extension(lci_path(), tmp_path / "x.db", ["bio"])
    assert (DATAPACKAGE_TABLE,) not in file_rows(
        tmp_path / "x.db", "SELECT name FROM sqlite_master WHERE type = 'table'"
    )


# Reading


def test_attached_databases_are_not_processed(built, monkeypatch):
    bio, inv = built
    projects.attach_extension([bio, inv])
    assert has_stored_datapackage("bio") and has_stored_datapackage("inv")
    assert not databases["bio"].get("dirty")
    assert not databases["inv"].get("dirty")
    process = SQLiteBackend.process

    def only_mine(self, *args, **kwargs):
        if self.name != "mine":
            raise AssertionError(f"database {self.name!r} was processed")
        return process(self, *args, **kwargs)

    monkeypatch.setattr(SQLiteBackend, "process", only_mine)
    databases.clean()
    # 0.5 kg steel with 2 kg of flow 0 each (10), and 3 kg of flow 1 (1)
    assert lca_score() == pytest.approx(13)
    # Nothing written to the project's processed folder for the extension databases
    for name in ("bio", "inv"):
        database = Database(name)
        assert not (database.dirpath_processed() / database.filename_processed()).exists()


def test_stored_datapackage_is_what_processing_gives(built):
    bio, inv = built
    projects.attach_extension([bio, inv])
    stored = Database("inv").datapackage()
    Database("inv").process()
    processed = Database("inv").filepath_processed()
    from bw_processing import load_datapackage
    from fsspec.implementations.zip import ZipFileSystem

    fresh = load_datapackage(ZipFileSystem(processed))
    for matrix in ("technosphere_matrix", "biosphere_matrix"):
        a, b = arrays(stored, matrix), arrays(fresh, matrix)
        assert a.keys() == b.keys()
        for kind in a:
            assert a[kind].tobytes() == b[kind].tobytes(), (matrix, kind)
    # The same activities; the location ids are those of the project which built the file
    assert np.array_equal(
        arrays(stored, "inv_geomapping_matrix")["indices"]["row"],
        arrays(fresh, "inv_geomapping_matrix")["indices"]["row"],
    )
    assert stored.metadata["database_dependencies"] == ["bio"]


def test_datapackage_in_another_thread(built):
    bio, inv = built
    projects.attach_extension(bio)
    result = {}

    def run():
        try:
            dp = Database("bio").datapackage()
            result["value"] = len(arrays(dp, "inv_geomapping_matrix")["indices"])
        except Exception as err:
            result["value"] = err

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    # ``bio`` has only biosphere flows, which are not processes
    assert result["value"] == 0


def test_filepath_processed_writes_the_stored_datapackage(built, monkeypatch):
    bio, inv = built
    projects.attach_extension(bio)
    no_processing(monkeypatch)
    path = Database("bio").filepath_processed()
    assert path.read_bytes() == stored_datapackage("bio")
    # Detaching removes it, as other processed data
    monkeypatch.undo()
    projects.detach_extension("bio")
    assert not path.exists()


# Processed in the project instead


def test_files_without_datapackages_are_processed(built):
    bio, inv = built
    edit(bio, f"DROP TABLE {DATAPACKAGE_TABLE}")
    projects.attach_extension(bio)
    assert not has_stored_datapackage("bio")
    assert stored_datapackage("bio") is None
    assert databases["bio"]["dirty"]
    databases.clean()
    assert not databases["bio"].get("dirty")
    assert Database("bio").filepath_processed().is_file()


def test_requirements_met_by_the_project_are_processed(built):
    # The project's copy of ``bio`` has the same codes, but other activity ids than the
    # arrays in ``inv.db`` were built with
    bio, inv = built
    Database("bio").write(BIO)
    projects.attach_extension(inv, allow_main_requirements=True)
    assert not has_stored_datapackage("inv")
    assert databases["inv"]["dirty"]
    assert lca_score() == pytest.approx(13)


def test_update_to_a_version_with_datapackages(built, tmp_path):
    bio, inv = built
    old = tmp_path / "old-bio.db"
    old.write_bytes(bio.read_bytes())
    edit(
        old,
        f"DROP TABLE {DATAPACKAGE_TABLE}",
        "UPDATE extension_info SET value = 'old' WHERE key = 'id'",
    )
    projects.attach_extension(old, name="bio")
    assert databases["bio"]["dirty"]
    projects.update_extension("bio", path=bio)
    assert has_stored_datapackage("bio")
    assert not databases["bio"].get("dirty")


def test_reading_needs_the_extension_attached(built):
    bio, inv = built
    assert stored_datapackage("bio") is None
    projects.attach_extension(bio)
    assert stored_datapackage("bio")[:4] == b"PK\x03\x04"
    sqlite3_lci_db.change_path(lci_path())
    assert stored_datapackage("bio") is None
