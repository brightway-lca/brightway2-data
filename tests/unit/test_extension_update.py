"""A new version of a read-only LCI extension: ``projects.update_extension``."""

import os

import pytest

from bw2data import Database, Method, databases, projects
from bw2data.backends.extensions import extension_id
from bw2data.backends.schema import ExtensionDatabaseState
from bw2data.errors import ExtensionError
from bw2data.project import ProjectManager
from bw2data.signals import on_extension_attached, on_extension_detached

from .test_extension_attach import export, files  # noqa: F401
from .test_extension_projects import statuses


def new_version(path, extension_id="bio-2", add=("3",), remove=()):
    """Build another version of the extension ``bio`` at ``path``, in the project ``source``
    (where activities keep their ids): with the flows ``add`` added and ``remove`` removed."""
    current = projects.current
    projects.set_current("source")
    for code in add:
        Database("bio").new_node(code=code, name=f"flow {code}", type="emission", unit="kg").save()
    for code in remove:
        Database("bio").get(code).delete()
    export(path, ["bio"], extension_id=extension_id)
    projects.set_current(current)
    return path


def link_to_bio(code="0"):
    Database("mine").write(
        {("mine", "x"): {"exchanges": [{"input": ("bio", code), "amount": 1, "type": "biosphere"}]}}
    )


# Replaced while the project is closed, or moved


def test_update_after_the_file_was_replaced_while_closed(files, tmp_path):
    bio, inv = files
    projects.attach_extension(bio)
    projects.set_current("somewhere else")
    os.replace(new_version(tmp_path / "v2.db"), bio)
    with pytest.warns(UserWarning, match="projects.update_extension\\('bio'\\)"):
        projects.set_current("target")
    assert statuses() == {"bio": "invalid"}
    projects.update_extension("bio")
    assert statuses() == {"bio": "available"}
    assert len(Database("bio")) == 4


def test_update_to_a_new_path(files, tmp_path):
    bio, inv = files
    projects.attach_extension(bio)
    v2 = new_version(tmp_path / "v2.db")
    entry = projects.update_extension("bio", path=v2)
    assert entry.path == v2.resolve()
    assert entry.id == "bio-2"
    assert len(Database("bio")) == 4
    # The old file is left as it is
    assert extension_id(bio) == "bio-id"


def test_the_same_version_at_another_path(files, tmp_path):
    bio, inv = files
    projects.attach_extension(bio)
    Database("bio").process()
    state = list(ExtensionDatabaseState.select().dicts())
    received = []
    moved = tmp_path / "moved" / "bio.db"
    moved.parent.mkdir()
    projects.set_current("somewhere else")
    os.replace(bio, moved)
    with pytest.warns(UserWarning, match="missing file"):
        projects.set_current("target")
    with on_extension_attached.connected_to(lambda sender, names: received.append(names)):
        entry = projects.update_extension("bio", path=moved)
    assert entry.available
    assert entry.path == moved.resolve()
    assert entry.id == "bio-id"
    assert received == [["bio"]]
    # Nothing is reset: it is the same version
    assert list(ExtensionDatabaseState.select().dicts()) == state
    assert not databases["bio"].get("dirty")


def test_nothing_changes_for_the_same_file(files):
    bio, inv = files
    projects.attach_extension(bio)
    received = []
    with on_extension_attached.connected_to(lambda sender, names: received.append(names)):
        entry = projects.update_extension("bio")
    assert entry == projects.extensions[0]
    assert received == []


# Local state


def test_local_state_is_reset(files, tmp_path):
    bio, inv = files
    projects.attach_extension(bio)
    Database("bio").process()
    assert not databases["bio"].get("dirty")
    processed = Database("bio").filepath_processed()
    assert processed.is_file()
    received = []
    with (
        on_extension_attached.connected_to(
            lambda sender, names: received.append(("attached", names))
        ),
        on_extension_detached.connected_to(
            lambda sender, name, databases: received.append(("detached", name, databases))
        ),
    ):
        projects.update_extension("bio", path=new_version(tmp_path / "v2.db"))
    assert received == [("detached", "bio", ("bio",)), ("attached", ["bio"])]
    assert databases["bio"]["dirty"]
    assert not processed.exists()
    Database("bio").process()
    assert Database("bio").filepath_processed().is_file()


def test_databases_which_the_new_version_lacks_are_forgotten(files, tmp_path):
    bio, inv = files
    both = tmp_path / "both.db"
    projects.set_current("source")
    export(both, ["bio", "inv"], extension_id="both-1")
    projects.set_current("target")
    projects.attach_extension(both)
    Database("inv").process()
    processed = Database("inv").filepath_processed()
    assert processed.is_file()
    only_bio = tmp_path / "only-bio.db"
    projects.set_current("source")
    export(only_bio, ["bio"], extension_id="both-2")
    projects.set_current("target")
    projects.update_extension("both", path=only_bio)
    assert sorted(databases) == ["bio"]
    assert projects.extensions[0].databases == ("bio",)
    assert not processed.exists()
    assert [row.name for row in ExtensionDatabaseState.select()] == ["bio"]


# Refusals


def test_refused_while_another_extension_requires_it(files, tmp_path):
    bio, inv = files
    projects.attach_extension([bio, inv])
    v2 = new_version(tmp_path / "v2.db")
    with pytest.raises(ExtensionError, match="extension 'inv' requires it.*force=True"):
        projects.update_extension("bio", path=v2)
    assert statuses() == {"bio": "available", "inv": "available"}
    assert projects.extensions[0].id == "bio-id"
    with pytest.warns(UserWarning) as caught:
        projects.update_extension("bio", path=v2, force=True)
    messages = [str(warning.message) for warning in caught]
    assert any("Updating extension 'bio'" in m and "'inv' requires it" in m for m in messages)
    assert any("'inv'" in m and "missing requirement" in m for m in messages)
    assert statuses() == {"bio": "available", "inv": "missing requirement"}
    assert sorted(databases) == ["bio"]


def test_refused_while_the_project_links_to_what_is_gone(files, tmp_path):
    bio, inv = files
    projects.attach_extension(bio)
    link_to_bio("1")
    v2 = new_version(tmp_path / "v2.db", remove=["1"])
    with pytest.raises(ExtensionError, match="inputs which are not in the new version: 'mine'"):
        projects.update_extension("bio", path=v2)
    assert projects.extensions[0].id == "bio-id"
    with pytest.warns(UserWarning, match="inputs which are not in the new version: 'mine'"):
        projects.update_extension("bio", path=v2, force=True)
    assert projects.extensions[0].id == "bio-2"


def test_links_to_what_is_still_there_are_fine(files, tmp_path):
    bio, inv = files
    projects.attach_extension(bio)
    link_to_bio("0")
    projects.update_extension("bio", path=new_version(tmp_path / "v2.db", remove=["1"]))
    (exchange,) = Database("mine").get("x").biosphere()
    assert exchange.input["name"] == "flow 0"


def test_refused_while_methods_use_flows_which_are_gone(files, tmp_path):
    bio, inv = files
    projects.attach_extension(bio)
    Method(("m",)).write([(("bio", "1"), 2.0), (("bio", "0"), 1.0)])
    Method(("kept",)).write([(("bio", "0"), 1.0)])
    v2 = new_version(tmp_path / "v2.db", remove=["1"])
    with pytest.raises(ExtensionError) as caught:
        projects.update_extension("bio", path=v2)
    assert "methods have factors for flows which are not in the new version: ('m',)" in str(
        caught.value
    )
    assert "'kept'" not in str(caught.value)


def test_refused_while_methods_use_flows_which_are_gone_also_after_a_replacement(files, tmp_path):
    # The old version is not attached: every flow which is in none of the files counts
    bio, inv = files
    projects.attach_extension(bio)
    Method(("m",)).write([(("bio", "1"), 2.0)])
    projects.set_current("somewhere else")
    os.replace(new_version(tmp_path / "v2.db", remove=["1"]), bio)
    with pytest.warns(UserWarning, match="not available"):
        projects.set_current("target")
    with pytest.raises(ExtensionError, match="methods have factors.*\\('m',\\)"):
        projects.update_extension("bio")


def test_refused_files(files, tmp_path):
    bio, inv = files
    projects.attach_extension([bio, inv])
    with pytest.raises(ValueError, match="no extension named 'nope'"):
        projects.update_extension("nope")
    with pytest.raises(ExtensionError, match="doesn't exist"):
        projects.update_extension("bio", path=tmp_path / "missing.db")
    not_an_extension = tmp_path / "plain.db"
    not_an_extension.write_bytes(b"")
    with pytest.raises(ExtensionError, match="not a bw2data extension"):
        projects.update_extension("bio", path=not_an_extension)
    with pytest.raises(ExtensionError, match="already an extension of this project, named 'inv'"):
        projects.update_extension("bio", path=inv)
    copy = tmp_path / "copy.db"
    copy.write_bytes(inv.read_bytes())
    with pytest.raises(ExtensionError, match="named 'inv' \\(id inv-id\\)"):
        projects.update_extension("bio", path=copy)
    # A file which doesn't fit to the project
    Database("bio-clash").register()
    clash = tmp_path / "clash.db"
    projects.set_current("source")
    Database("bio-clash").write({("bio-clash", "1"): {"name": "x"}})
    export(clash, ["bio", "bio-clash"], extension_id="bio-3")
    projects.set_current("target")
    with pytest.raises(ExtensionError, match="bio-clash"):
        projects.update_extension("bio", path=clash, force=True)
    assert statuses() == {"bio": "available", "inv": "available"}
    assert projects.extensions[0].id == "bio-id"


def test_failed_update_changes_nothing(files, tmp_path, monkeypatch):
    bio, inv = files
    projects.attach_extension(bio)
    Database("bio").process()
    v2 = new_version(tmp_path / "v2.db")
    lci_db = projects._lci_db()
    original = lci_db.change_path

    def failing(filepath, extensions=()):
        if any(str(spec.path).endswith("v2.db") for spec in extensions):
            raise OSError("locked")
        return original(filepath, extensions)

    monkeypatch.setattr(lci_db, "change_path", failing)
    with pytest.raises(OSError, match="locked"):
        projects.update_extension("bio", path=v2)
    assert statuses() == {"bio": "available"}
    assert projects.extensions[0].id == "bio-id"
    assert len(Database("bio")) == 3
    assert not databases["bio"].get("dirty")
    assert Database("bio").filepath_processed().is_file()


def test_failed_reset_of_the_local_state_changes_nothing(files, tmp_path, monkeypatch):
    bio, inv = files
    projects.attach_extension(bio)
    Database("bio").process()
    v2 = new_version(tmp_path / "v2.db")

    def failing(*args, **kwargs):
        raise OSError("locked")

    # Called after the databases of the new version were given a new local state
    monkeypatch.setattr(ProjectManager, "_forget_extension_databases", staticmethod(failing))
    with pytest.raises(OSError, match="locked"):
        projects.update_extension("bio", path=v2)
    assert statuses() == {"bio": "available"}
    assert projects.extensions[0].id == "bio-id"
    assert len(Database("bio")) == 3
    assert not databases["bio"].get("dirty")
    assert Database("bio").filepath_processed().is_file()


# Extensions which are not available


def test_returning_extension_which_no_longer_fits_stays_unavailable(files, tmp_path):
    bio, inv = files
    # An extension which stays attached, next to the one which is updated
    other = tmp_path / "other.db"
    projects.set_current("source")
    Database("other").write({("other", "f"): {"name": "f", "type": "emission", "unit": "kg"}})
    export(other, ["other"], extension_id="other-1")
    projects.set_current("target")
    projects.attach_extension([bio, inv, other])
    away = tmp_path / "away.db"
    projects.set_current("somewhere else")
    os.replace(inv, away)
    with pytest.warns(UserWarning, match="missing file"):
        projects.set_current("target")
    # The project uses the name of the extension's database by the time its file is back
    Database("inv").register()
    os.replace(away, inv)
    moved = tmp_path / "moved-bio.db"
    moved.write_bytes(bio.read_bytes())
    with pytest.warns(UserWarning, match="'inv'.*not available \\(invalid\\)"):
        entry = projects.update_extension("bio", path=moved)
    assert entry.available
    assert entry.path == moved.resolve()
    assert statuses() == {"bio": "available", "inv": "invalid", "other": "available"}
    assert sorted(databases) == ["bio", "inv", "other"]
    assert not Database("inv").is_extension


def test_names_used_by_the_project_by_now_are_not_forgotten(files, tmp_path):
    bio, inv = files
    both = tmp_path / "both.db"
    only_bio = tmp_path / "only-bio.db"
    projects.set_current("source")
    export(both, ["bio", "inv"], extension_id="both-1")
    export(only_bio, ["bio"], extension_id="both-2")
    projects.set_current("target")
    projects.attach_extension(both)
    projects.set_current("somewhere else")
    os.replace(both, tmp_path / "away.db")
    with pytest.warns(UserWarning, match="missing file"):
        projects.set_current("target")
    # The project's own database, with a name which the extension had
    Database("inv").write({("inv", "mine"): {"name": "my own thing", "unit": "kg"}})
    Database("inv").process()
    processed = Database("inv").filepath_processed()
    assert processed.is_file()
    received = []
    with on_extension_detached.connected_to(
        lambda sender, name, databases: received.append((name, databases))
    ):
        projects.update_extension("both", path=only_bio)
    assert statuses() == {"both": "available"}
    assert sorted(databases) == ["bio", "inv"]
    assert processed.is_file()
    assert len(Database("inv").search("thing")) == 1
    # It was not available: no databases left the project
    assert received == [("both", ())]


def test_flows_of_other_unavailable_extensions_do_not_block(files, tmp_path):
    bio, inv = files
    other = tmp_path / "other.db"
    projects.set_current("source")
    Database("other").write({("other", "f"): {"name": "f", "type": "emission", "unit": "kg"}})
    export(other, ["other"], extension_id="other-1")
    projects.set_current("target")
    projects.attach_extension([bio, other])
    Method(("m",)).write([(("other", "f"), 1.0)])
    # Both files are replaced by new builds while the project is closed
    v2 = new_version(tmp_path / "v2.db")
    projects.set_current("source")
    export(tmp_path / "other-2.db", ["other"], extension_id="other-2")
    projects.set_current("somewhere else")
    os.replace(v2, bio)
    os.replace(tmp_path / "other-2.db", other)
    with pytest.warns(UserWarning, match="not available"):
        projects.set_current("target")
    assert statuses() == {"bio": "invalid", "other": "invalid"}
    projects.update_extension("bio")
    projects.update_extension("other")
    assert statuses() == {"bio": "available", "other": "available"}


def test_message_for_flows_which_are_gone_when_the_old_version_is_unknown(files, tmp_path):
    bio, inv = files
    projects.attach_extension(bio)
    Method(("m",)).write([(("bio", "1"), 2.0)])
    projects.set_current("somewhere else")
    os.replace(new_version(tmp_path / "v2.db", remove=["1"]), bio)
    with pytest.warns(UserWarning, match="not available"):
        projects.set_current("target")
    with pytest.raises(ExtensionError, match="in none of the project's files.*\\('m',\\)"):
        projects.update_extension("bio")
