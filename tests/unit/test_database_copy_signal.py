"""`Database.copy` sends `on_database_copy` once the new database has its data and parameters."""

import pytest

from bw2data import signals
from bw2data.database import DatabaseChooser
from bw2data.parameters import DatabaseParameter
from bw2data.project import projects
from bw2data.signals import on_database_copy, on_database_write
from bw2data.tests import bw2test


def copied_state():
    return {
        "nodes": len(DatabaseChooser("copy")),
        "parameters": DatabaseParameter.select()
        .where(DatabaseParameter.database == "copy")
        .count(),
        "revision": projects.dataset.revision,
    }


class Catcher:
    def __init__(self):
        self.calls = []

    def __call__(self, sender, **kwargs):
        self.calls.append((sender, kwargs, copied_state()))


def setup_database(mode):
    projects.set_current("plain")
    if mode == "sourced":
        projects.dataset.set_sourced()
    elif mode == "signalled":
        signals.set_signalled()
    database = DatabaseChooser("db")
    database.write({("db", "A"): {"name": "A", "exchanges": []}})
    DatabaseParameter.create(database="db", name="p1", amount=1.0)
    return database


@bw2test
@pytest.mark.parametrize("mode", ["default", "signalled", "sourced"])
def test_copy_signal_sent_once_when_copy_is_complete(mode):
    database = setup_database(mode)
    catcher = Catcher()

    with on_database_copy.connected_to(catcher):
        new_database = database.copy("copy")

    assert len(catcher.calls) == 1
    sender, kwargs, state = catcher.calls[0]
    assert sender is new_database
    assert kwargs == {"name": "copy", "source": "db"}
    assert state["nodes"] == 1
    assert state["parameters"] == 1


@bw2test
@pytest.mark.parametrize("mode", ["default", "signalled"])
def test_copy_signal_writes_no_revision_when_not_sourced(mode):
    database = setup_database(mode)

    database.copy("copy")

    assert projects.dataset.revision is None
    assert not projects.dataset.is_sourced


@bw2test
def test_copy_signal_adds_no_revision_when_sourced():
    database = setup_database("sourced")
    catcher = Catcher()

    with on_database_copy.connected_to(catcher):
        database.copy("copy")

    # All revisions for the copy were written before the signal was sent
    assert catcher.calls[0][2]["revision"] == projects.dataset.revision
    assert projects.dataset.revision is not None


@bw2test
@pytest.mark.parametrize("mode", ["signalled", "sourced"])
def test_write_signal_during_copy_is_sent_before_parameters_are_copied(mode):
    database = setup_database(mode)
    write_catcher, copy_catcher = Catcher(), Catcher()

    with on_database_write.connected_to(write_catcher):
        with on_database_copy.connected_to(copy_catcher):
            database.copy("copy")

    assert [call[1] for call in write_catcher.calls] == [{"name": "copy"}]
    assert write_catcher.calls[0][2]["nodes"] == 1
    assert write_catcher.calls[0][2]["parameters"] == 0
    assert copy_catcher.calls[0][2]["parameters"] == 1


@bw2test
def test_copy_signal_not_sent_for_write():
    projects.set_current("plain")
    signals.set_signalled()
    catcher = Catcher()

    with on_database_copy.connected_to(catcher):
        DatabaseChooser("copy").write({("copy", "A"): {"name": "A", "exchanges": []}})

    assert not catcher.calls
