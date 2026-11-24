"""Read-only LCI extensions of a project (extensions PR 4): ``projects.attach_extension``,
``projects.detach_extension``, ``projects.extensions``, and what happens to them when projects
are switched, copied and deleted."""

import json
import os
import sqlite3
import subprocess
import sys
import warnings

import pytest

from bw2data import Database, databases, get_node, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.schema import ActivityDataset
from bw2data.errors import ExtensionDependencyError, ExtensionError
from bw2data.signals import on_extension_attached, on_extension_detached
from bw2data.tests import bw2test

from .test_extension_attach import BIO, edit, files, lci_path, main_objects  # noqa: F401


def statuses():
    return {extension.name: extension.status for extension in projects.extensions}


def reopen():
    """Switch to another project and back, as a new session would open the project."""
    name = projects.current
    projects.set_current("somewhere else")
    projects.set_current(name)


# Attaching


def test_attach_a_list_in_any_order(files):
    bio, inv = files
    new = projects.attach_extension([inv, bio])
    assert {extension.name for extension in new} == {"bio", "inv"}
    assert all(extension.available for extension in new)
    # Stored such that each one comes after the ones it requires
    assert [extension.name for extension in projects.extensions] == ["bio", "inv"]
    first = projects.extensions[0]
    assert first.path == bio.resolve()
    assert first.id == "bio-id"
    assert first.databases == ("bio",)
    assert first.status == "available"
    assert first.problem is None
    assert sorted(databases) == ["bio", "inv"]
    assert len(Database("bio")) == 3


def test_attach_one_after_the_other(files):
    bio, inv = files
    projects.attach_extension(bio)
    projects.attach_extension(str(inv), name="inventory", immutable=True)
    assert statuses() == {"bio": "available", "inventory": "available"}
    assert projects.extensions[1].immutable is True
    assert sorted(databases) == ["bio", "inv"]


def test_extensions_are_stored_with_the_project(files):
    bio, inv = files
    projects.attach_extension([bio, inv])
    Database("mine").write(
        {
            ("mine", "x"): {
                "exchanges": [{"input": ("inv", "a"), "amount": 1, "type": "technosphere"}]
            }
        }
    )
    projects.set_current("other")
    assert projects.extensions == ()
    assert sorted(databases) == []
    assert ActivityDataset._meta.table_name == "activitydataset"
    projects.set_current("target")
    assert statuses() == {"bio": "available", "inv": "available"}
    assert sorted(databases) == ["bio", "inv", "mine"]
    assert [e.input["name"] for e in get_node(database="mine", code="x").technosphere()] == ["a"]
    assert not any(name.startswith("lci_") for name in main_objects())


def test_failed_attach_changes_nothing(files):
    bio, inv = files
    with pytest.raises(ExtensionDependencyError):
        projects.attach_extension(inv)
    assert projects.extensions == ()
    assert sqlite3_lci_db.extensions == ()
    reopen()
    assert projects.extensions == ()


def test_attach_arguments(files, tmp_path):
    bio, inv = files
    with pytest.raises(ValueError, match="single"):
        projects.attach_extension([bio, inv], name="both")
    with pytest.raises(ValueError, match="No extension file"):
        projects.attach_extension([])
    projects.attach_extension(bio)
    with pytest.raises(ExtensionError, match="already an extension of this project, named 'bio'"):
        projects.attach_extension(bio, name="again")
    copy = tmp_path / "copy.db"
    copy.write_bytes(bio.read_bytes())
    with pytest.raises(ExtensionError, match="already has an extension named 'bio'"):
        projects.attach_extension(copy, name="bio")
    with pytest.raises(ExtensionError, match="same extension"):
        projects.attach_extension(copy)
    assert statuses() == {"bio": "available"}


def test_extensions_which_require_each_other(files):
    bio, inv = files
    # ``inv`` requires ``bio``; make ``bio`` require ``inv`` as well
    conn = sqlite3.connect(inv)
    (fingerprints,) = conn.execute(
        "SELECT value FROM extension_info WHERE key = 'fingerprints'"
    ).fetchone()
    conn.close()
    edit(
        bio,
        "CREATE TABLE extension_requires "
        "(database TEXT PRIMARY KEY, extension_id TEXT, fingerprint TEXT)",
        (
            "INSERT INTO extension_requires VALUES ('inv', 'inv-id', ?)",
            (json.loads(fingerprints)["inv"],),
        ),
    )
    with pytest.raises(ExtensionDependencyError, match="require each other"):
        projects.attach_extension([bio, inv])
    assert projects.extensions == ()


def test_signals(files):
    bio, inv = files
    received = []

    def attached(sender, names):
        received.append(("attached", names, sorted(databases)))

    def detached(sender, name, databases):
        received.append(("detached", name, databases))

    with on_extension_attached.connected_to(attached), on_extension_detached.connected_to(detached):
        projects.attach_extension([inv, bio])
        projects.detach_extension("inv")
    assert received == [
        ("attached", ["bio", "inv"], ["bio", "inv"]),
        ("detached", "inv", ("inv",)),
    ]


def test_unsaved_metadata_of_a_detached_database_is_forgotten(files):
    bio, inv = files
    projects.attach_extension(bio)
    databases["bio"]["dirty"] = True
    projects.detach_extension("bio")
    assert "bio" not in databases
    assert databases._proxies == {}


# Detaching


def test_detach(files):
    bio, inv = files
    projects.attach_extension([bio, inv])
    projects.detach_extension("inv")
    assert statuses() == {"bio": "available"}
    assert sorted(databases) == ["bio"]
    projects.detach_extension("bio")
    assert projects.extensions == ()
    assert sorted(databases) == []
    assert ActivityDataset._meta.table_name == "activitydataset"
    assert bio.is_file() and inv.is_file()
    reopen()
    assert projects.extensions == ()
    with pytest.raises(ValueError, match="no extension named 'bio'"):
        projects.detach_extension("bio")


def test_detach_refused_while_required(files):
    bio, inv = files
    projects.attach_extension([bio, inv])
    with pytest.raises(ExtensionError, match="extension 'inv' requires it.*force=True"):
        projects.detach_extension("bio")
    assert statuses() == {"bio": "available", "inv": "available"}
    with pytest.warns(UserWarning) as caught:
        projects.detach_extension("bio", force=True)
    messages = [str(warning.message) for warning in caught]
    assert any("Detaching extension 'bio': extension 'inv' requires it" in m for m in messages)
    assert any("'inv'" in m and "missing requirement" in m for m in messages)
    assert statuses() == {"inv": "missing requirement"}
    assert "not attached" in projects.extensions[0].problem
    assert sorted(databases) == []
    # Attaching what it needs makes it available again
    projects.attach_extension(bio)
    assert statuses() == {"inv": "available", "bio": "available"}
    assert sorted(databases) == ["bio", "inv"]


def test_detach_refused_while_the_project_links_to_it(files):
    bio, inv = files
    projects.attach_extension(bio)
    Database("mine").write(
        {("mine", "x"): {"exchanges": [{"input": ("bio", "0"), "amount": 1, "type": "biosphere"}]}}
    )
    with pytest.raises(ExtensionError, match="exchanges with inputs in it: 'mine'"):
        projects.detach_extension("bio")
    assert statuses() == {"bio": "available"}
    with pytest.warns(UserWarning, match="inputs in it: 'mine'"):
        projects.detach_extension("bio", force=True)
    assert projects.extensions == ()
    assert sorted(databases) == ["mine"]


# Extensions which can't be attached when the project is opened


def test_missing_file(files):
    bio, inv = files
    projects.attach_extension([bio, inv])
    inv.unlink()
    with pytest.warns(UserWarning, match="'inv' of project 'target' is not available"):
        reopen()
    assert statuses() == {"bio": "available", "inv": "missing file"}
    assert "doesn't exist" in projects.extensions[1].problem
    assert projects.extensions[1].databases == ()
    assert sorted(databases) == ["bio"]
    # It can still be detached
    projects.detach_extension("inv")
    assert statuses() == {"bio": "available"}


def test_missing_file_of_a_required_extension(files):
    bio, inv = files
    projects.attach_extension([bio, inv])
    moved = bio.rename(bio.with_name("moved.db"))
    with pytest.warns(UserWarning) as caught:
        reopen()
    assert len([w for w in caught if "is not available" in str(w.message)]) == 2
    assert statuses() == {"bio": "missing file", "inv": "missing requirement"}
    assert sorted(databases) == []
    assert ActivityDataset._meta.table_name == "activitydataset"
    # When the file is back, so are both
    moved.rename(bio)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        reopen()
    assert statuses() == {"bio": "available", "inv": "available"}
    assert sorted(databases) == ["bio", "inv"]


def test_replaced_file(files):
    bio, inv = files
    projects.attach_extension(bio)
    edit(bio, "UPDATE extension_info SET value = 'new-build' WHERE key = 'id'")
    with pytest.warns(UserWarning, match="has the id new-build instead of bio-id"):
        reopen()
    assert statuses() == {"bio": "invalid"}
    assert sorted(databases) == []
    projects.detach_extension("bio")
    projects.attach_extension(bio)
    assert statuses() == {"bio": "available"}
    assert projects.extensions[0].id == "new-build"


def test_file_which_is_no_longer_an_extension(files):
    bio, inv = files
    projects.attach_extension(bio)
    bio.write_text("not a database " * 100)
    with pytest.warns(UserWarning, match="not available \\(invalid\\)"):
        reopen()
    assert statuses() == {"bio": "invalid"}


def test_extensions_which_no_longer_fit_the_project(files):
    bio, inv = files
    projects.attach_extension(bio)
    moved = bio.rename(bio.with_name("moved.db"))
    with pytest.warns(UserWarning):
        reopen()
    # While the extension is away, the project gets a database with the same name
    Database("bio").write(BIO)
    moved.rename(bio)
    with pytest.warns(UserWarning, match="each database can only be stored in one file"):
        reopen()
    assert statuses() == {"bio": "invalid"}
    assert sqlite3_lci_db.extensions == ()
    assert len(Database("bio")) == 3


# Copying and deleting projects


def test_copy_project_keeps_the_extensions(files):
    bio, inv = files
    projects.attach_extension([bio, inv])
    projects.copy_project("copy")
    assert projects.current == "copy"
    assert statuses() == {"bio": "available", "inv": "available"}
    assert sorted(databases) == ["bio", "inv"]
    # Detaching in the copy leaves the original as it was
    projects.detach_extension("inv")
    projects.set_current("target")
    assert statuses() == {"bio": "available", "inv": "available"}


def test_delete_project_keeps_the_extension_files(files):
    bio, inv = files
    before = bio.read_bytes(), inv.read_bytes()
    projects.attach_extension([bio, inv])
    projects.delete_project("target", delete_dir=True)
    assert projects.current != "target"
    assert (bio.read_bytes(), inv.read_bytes()) == before
    assert projects.extensions == ()
    assert ActivityDataset._meta.table_name == "activitydataset"


# Tests and a new process


def test_bw2test_points_the_models_at_their_tables_again(files):
    bio, inv = files

    @bw2test
    def inner():
        projects.attach_extension(bio)
        assert ActivityDataset._meta.table_name == "lci_activitydataset"

    inner()
    assert ActivityDataset._meta.table_name == "activitydataset"
    assert sqlite3_lci_db.extensions == ()


def test_extensions_are_attached_when_bw2data_is_imported(files, tmp_path):
    bio, inv = files
    directory = tmp_path / "brightway"
    directory.mkdir()
    env = {**os.environ, "BRIGHTWAY2_DIR": str(directory)}

    def run(code):
        result = subprocess.run(
            [sys.executable, "-c", code, str(bio), str(inv)],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout.strip().splitlines()[-1]

    run("import sys, bw2data as bd; bd.projects.attach_extension(sys.argv[1:])")
    # The current project of a new process is ``default``, opened while importing
    assert run("import bw2data as bd; print(sorted(bd.databases))") == "['bio', 'inv']"
