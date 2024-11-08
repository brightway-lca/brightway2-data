import copy
from unittest.mock import Mock, patch

import pytest
from deepdiff import DeepDiff

import bw2data.project
from bw2data.backends.schema import ActivityDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.tests import bw2test


@bw2test
@patch("bw2data.signals.SignaledDataset.save")
def test_signaleddataset_save_is_called(*mocks: Mock):
    # On saving an `Activity`, the generic `SignaledDataset.save` method is called
    projects.set_current("activity-event")
    database = DatabaseChooser("db")
    database.register()
    activity = database.new_node(code="A", name="A")
    activity.save()
    for m in mocks:
        assert m.called


@bw2test
@patch("bw2data.signals.signaleddataset_on_save.send")
@patch("bw2data.signals.SignaledDataset.get")
def test_signal_is_sent(*mocks: Mock):
    # On saving an `Activity`, the signal is sent
    projects.set_current("activity-event")
    database = DatabaseChooser("db")
    database.register()
    activity = database.new_node(code="A", name="A")
    activity.save()
    for m in mocks:
        assert m.called


@bw2test
@patch("bw2data.project.ProjectDataset.add_revision")
def test_signal_received(*mocks: Mock):
    # When the signal is received, the revision is updated
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    db = DatabaseChooser("db")
    db.register()
    a = db.new_node(code="A", name="A")
    a.save()
    bw2data.project.signal_dispatcher("test", old=None, new=a._document)
    for m in mocks:
        assert m.called


@bw2test
def test_add_revision():
    # a = Activity(code="A", name="A", database="db", data={'a': 1})._document
    # a = SQLiteBackend("db").new_node(code="A", name="A", database="db", data={'a': 1})
    projects.set_current("activity-event")
    db = DatabaseChooser("db")
    db.register()
    a = db.new_node(code="A", name="A")
    a.save()
    new = copy.deepcopy(a)
    new["name"] = "B"
    new.save()
    projects.dataset.add_revision(a._document, new._document)
