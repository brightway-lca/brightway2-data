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
@patch("bw2data.backends.schema.SignaledDataset.save")
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
@patch("bw2data.signals.database_saved.send")
@patch("bw2data.backends.schema.SignaledDataset.get")
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
    bw2data.project._signal_dataset_saved("test", old=None, new=a._document)
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
    patch = DeepDiff(
        a._document.data,
        new._document.data,
        verbose_level=2,
    )
    cls = a._document.__class__.__name__.lower()
    mapper_str = f"dict_as_{cls}"
    assert mapper_str == "dict_as_activitydataset"
    mapper = getattr(bw2data.backends.utils, mapper_str)
    projects.dataset.add_revision({"type": cls, "id": new.id}, patch)
