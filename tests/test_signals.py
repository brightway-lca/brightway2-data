import pytest
from blinker import signal

from bw2data.database import DatabaseChooser
from bw2data.project import ProjectDataset, projects
from bw2data.signals import on_database_write
from bw2data.tests import bw2test


class SignalCatcher:
    def __call__(self, sender, **kwargs):
        self.sender = sender
        self.kwargs = kwargs


@bw2test
def test_project_changed_signal():
    subscriber = SignalCatcher()
    signal("bw2data.project_changed").connect(subscriber)
    projects.set_current("foo")

    assert isinstance(subscriber.kwargs["dataset"], ProjectDataset)
    assert subscriber.kwargs["dataset"].name == "foo"


@bw2test
def test_project_created_signal():
    subscriber = SignalCatcher()
    signal("bw2data.project_created").connect(subscriber)
    projects.set_current("foo")

    assert isinstance(subscriber.kwargs["dataset"], ProjectDataset)
    assert subscriber.kwargs["dataset"].name == "foo"


@bw2test
def test_project_deleted_signal():
    projects.set_current("foo")
    subscriber = SignalCatcher()
    signal("bw2data.project_deleted").connect(subscriber)
    projects.delete_project("foo")

    assert isinstance(subscriber.kwargs["dataset"], ProjectDataset)
    assert subscriber.kwargs["dataset"].name == "foo"


@pytest.mark.parametrize("sourced", [False, True], ids=["unsourced", "sourced"])
@bw2test
def test_database_copy_sends_write_signal(sourced):
    """Copying a database notifies listeners exactly once in both project modes."""
    source = DatabaseChooser("source")
    source.write(
        {
            ("source", "process"): {
                "name": "A process",
                "unit": "kilogram",
                "location": "GLO",
                "exchanges": [{"input": ("source", "process"), "amount": 1, "type": "production"}],
            }
        },
        searchable=False,
    )
    if sourced:
        projects.dataset.set_sourced()
    assert projects.dataset.is_sourced is sourced
    received = []

    def receive(sender, name):
        """Capture written data while handling the notification."""
        received.append((sender, name, DatabaseChooser(name).load()))

    with on_database_write.connected_to(receive):
        copied = source.copy("copied")

    assert len(received) == 1
    assert received[0] == (None, "copied", copied.load())
    assert len(copied) == 1
    assert next(iter(copied.get("process").production())).input.key == ("copied", "process")
    if not sourced:
        assert projects.dataset.revision is None
