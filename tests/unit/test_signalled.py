"""`bw2data.signals.set_signalled` sends the change signals which are costly to build on
projects which are not event sourced, with the same arguments as on sourced projects, and
without writing revisions."""

from unittest.mock import patch

import pytest
from blinker import Signal

from bw2data import databases, revisions, signals
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.serialization import PickledDict, SerializedDict
from bw2data.signals import (
    on_database_metadata_change,
    on_database_write,
    signaleddataset_on_save,
)
from bw2data.tests import bw2test


class Catcher:
    def __init__(self, callback=None):
        self.calls = []
        self.callback = callback

    def __call__(self, sender, **kwargs):
        if self.callback:
            kwargs["seen"] = self.callback()
        self.calls.append(kwargs)


def test_default_is_not_signalled():
    assert not signals.is_signalled()


def test_set_signalled():
    signals.set_signalled()
    assert signals.is_signalled()
    signals.set_signalled(False)
    assert not signals.is_signalled()


def test_signalled_context_manager_restores_previous_setting():
    with signals.signalled():
        assert signals.is_signalled()
        with signals.signalled(False):
            assert not signals.is_signalled()
        assert signals.is_signalled()
    assert not signals.is_signalled()


def test_signalled_context_manager_restores_after_error():
    with pytest.raises(ValueError):
        with signals.signalled():
            raise ValueError
    assert not signals.is_signalled()


@bw2test
def test_should_emit():
    projects.set_current("plain")
    assert not signals.should_emit()
    with signals.signalled():
        assert signals.should_emit()
    assert not signals.should_emit()
    projects.dataset.set_sourced()
    assert signals.should_emit()


@bw2test
def test_setting_kept_when_switching_projects():
    projects.set_current("one")
    signals.set_signalled()
    projects.set_current("two")
    assert signals.is_signalled()
    assert not projects.dataset.is_sourced


@bw2test
def test_node_save_sends_previous_and_new_version_without_revision():
    projects.set_current("plain")
    signals.set_signalled()
    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    names = []

    def receiver(sender, old, new):
        # `new` is the saved dataset itself, so read it now
        names.append((old.name if old else None, new.name))

    with signaleddataset_on_save.connected_to(receiver):
        with patch.object(revisions, "generate_delta") as generate:
            node.save()
            node["name"] = "B"
            node.save()

    assert names == [(None, "A"), ("A", "B")]
    generate.assert_not_called()
    assert projects.dataset.revision is None
    assert not projects.dataset.is_sourced


@bw2test
def test_edge_save_sends_signal():
    projects.set_current("plain")
    signals.set_signalled()
    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    catcher = Catcher()

    with signaleddataset_on_save.connected_to(catcher):
        node.new_edge(input=node, amount=1, type="production").save()

    assert len(catcher.calls) == 1
    assert catcher.calls[0]["new"].output_code == "A"
    assert projects.dataset.revision is None


@bw2test
def test_node_save_signal_false_sends_nothing():
    projects.set_current("plain")
    signals.set_signalled()
    database = DatabaseChooser("db")
    database.register()
    catcher = Catcher()

    with signaleddataset_on_save.connected_to(catcher):
        database.new_node(code="A", name="A").save(signal=False)

    assert not catcher.calls


@bw2test
def test_metadata_save_sends_old_and_new_without_revision():
    projects.set_current("plain")
    signals.set_signalled()
    database = DatabaseChooser("db")
    database.register()
    catcher = Catcher()

    with on_database_metadata_change.connected_to(catcher):
        database.metadata["foo"] = True
        database.metadata.save()

    assert len(catcher.calls) == 1
    assert "foo" not in catcher.calls[0]["old"]["db"]
    assert catcher.calls[0]["new"]["db"]["foo"] is True
    assert projects.dataset.revision is None


@bw2test
def test_metadata_setitem_sends_old_and_new():
    projects.set_current("plain")
    signals.set_signalled()
    catcher = Catcher()

    with on_database_metadata_change.connected_to(catcher):
        databases["db"] = {"backend": "sqlite", "depends": []}

    assert len(catcher.calls) == 1
    assert catcher.calls[0]["old"] == {}
    assert "db" in catcher.calls[0]["new"]


@bw2test
def test_database_write_sends_signal_without_building_deltas():
    projects.set_current("plain")
    signals.set_signalled()
    database = DatabaseChooser("db")
    catcher = Catcher(callback=lambda: len(DatabaseChooser("db")))

    with on_database_write.connected_to(catcher):
        with patch.object(revisions.Delta, "generate") as generate:
            database.write({("db", "A"): {"name": "A", "exchanges": []}})

    assert catcher.calls == [{"name": "db", "seen": 1}]
    generate.assert_not_called()
    assert projects.dataset.revision is None


@bw2test
def test_database_write_signal_false_sends_nothing():
    projects.set_current("plain")
    signals.set_signalled()
    catcher = Catcher()

    with on_database_write.connected_to(catcher):
        DatabaseChooser("db").write({("db", "A"): {"name": "A", "exchanges": []}}, signal=False)

    assert not catcher.calls


@bw2test
def test_database_write_sends_nothing_by_default():
    projects.set_current("plain")
    catcher = Catcher()

    with on_database_write.connected_to(catcher):
        DatabaseChooser("db").write({("db", "A"): {"name": "A", "exchanges": []}})

    assert not catcher.calls


@bw2test
def test_database_copy_sends_write_signal_for_new_database():
    projects.set_current("plain")
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "A"): {
                "name": "A",
                "exchanges": [{"input": ("db", "A"), "amount": 1, "type": "production"}],
            }
        }
    )
    signals.set_signalled()

    def copied_production_input():
        return [exc.input.key for exc in DatabaseChooser("copy").get("A").production()]

    catcher = Catcher(callback=copied_production_input)

    with on_database_write.connected_to(catcher):
        database.copy("copy")

    assert catcher.calls == [{"name": "copy", "seen": [("copy", "A")]}]
    assert projects.dataset.revision is None


class JSONStore(SerializedDict):
    filename = "store.json"
    _save_signal = Signal()


class PickleStore(PickledDict):
    filename = "store.pickle"
    _save_signal = Signal()


@bw2test
@pytest.mark.parametrize("store_class", [JSONStore, PickleStore])
def test_serialized_dict_save_signal(store_class):
    projects.set_current("plain")
    store = store_class()
    catcher = Catcher()

    with store._save_signal.connected_to(catcher):
        store["a"] = 1
        assert not catcher.calls

        signals.set_signalled()
        store["b"] = 2

    assert catcher.calls == [{"old": {"a": 1}, "new": {"a": 1, "b": 2}}]
    assert catcher.calls[0]["new"] is not store.data


@bw2test
@pytest.mark.parametrize("store_class", [JSONStore, PickleStore])
def test_serialized_dict_save_signal_when_file_is_missing(store_class):
    projects.set_current("plain")
    signals.set_signalled()
    store = store_class()
    store.filepath.unlink()
    catcher = Catcher()

    with store._save_signal.connected_to(catcher):
        store["a"] = 1

    assert catcher.calls == [{"old": {}, "new": {"a": 1}}]


@bw2test
@pytest.mark.parametrize("store_class", [JSONStore, PickleStore])
def test_serialized_dict_backup_sends_no_signal(store_class):
    projects.set_current("plain")
    signals.set_signalled()
    store = store_class()
    store["a"] = 1
    (projects.dir / "backups").mkdir(exist_ok=True)
    catcher = Catcher()

    with store._save_signal.connected_to(catcher):
        store.backup()

    assert not catcher.calls
    backups = list((projects.dir / "backups").glob(f"{store.filename}.*.backup"))
    assert len(backups) == 1
    assert store_class().data == {"a": 1}
