from typing import Any, Optional

import pytest
from blinker import signal

from bw2data.project import ProjectDataset, projects
from bw2data.signals import test_signal
from bw2data.tests import bw2test


class Dummy:
    def __init__(self, input_arg: Optional[Any] = None):
        self.input_arg = input_arg

    def __call__(self, call_arg: Optional[Any] = None):
        self.call_arg = call_arg


@pytest.fixture
def catcher():
    test_signal_catcher = Dummy("test_signal_catcher class instance")
    test_signal.connect(test_signal_catcher)
    return test_signal_catcher


@bw2test
def test_project_changed_signal():
    subscriber = Dummy()
    signal("bw2data.project_changed").connect(subscriber)
    projects.set_current("foo")

    assert isinstance(subscriber.call_arg, ProjectDataset)
    assert subscriber.call_arg.name == "foo"


@bw2test
def test_project_created_signal():
    subscriber = Dummy()
    signal("bw2data.project_created").connect(subscriber)
    projects.set_current("foo")

    assert isinstance(subscriber.call_arg, ProjectDataset)
    assert subscriber.call_arg.name == "foo"


def test_test_signal(catcher):
    assert catcher.input_arg == "test_signal_catcher class instance"
    assert not hasattr(catcher, "call_arg")

    test_signal.send(7)

    assert catcher.call_arg == 7


def test_test_signal_disconnect(catcher):
    assert catcher.input_arg == "test_signal_catcher class instance"
    assert not hasattr(catcher, "call_arg")
    assert test_signal.receivers

    test_signal.disconnect(catcher)
    test_signal.send(7)
    assert not hasattr(catcher, "call_arg")
