from blinker import signal

from bw2data.project import ProjectDataset, projects
from bw2data.tests import bw2test


class SignalCatcher:
    def __call__(self, arg):
        self.arg = arg


@bw2test
def test_project_changed_signal():
    subscriber = SignalCatcher()
    signal("bw2data.project_changed").connect(subscriber)
    projects.set_current("foo")

    assert isinstance(subscriber.arg, ProjectDataset)
    assert subscriber.arg.name == "foo"


@bw2test
def test_project_created_signal():
    subscriber = SignalCatcher()
    signal("bw2data.project_created").connect(subscriber)
    projects.set_current("foo")

    assert isinstance(subscriber.arg, ProjectDataset)
    assert subscriber.arg.name == "foo"
