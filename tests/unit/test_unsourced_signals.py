"""Revision setup is skipped when the project is not event sourced.

``signaleddataset_on_save`` is still emitted on unsourced saves (for UI / cache
listeners), but without loading the previous row. ``signal_dispatcher`` returns
before ``generate_delta`` on unsourced projects. Metadata flush still skips both
the file re-read and ``on_database_metadata_change`` when unsourced.
"""

from unittest.mock import patch

from bw2data import databases, revisions
from bw2data.backends.schema import ActivityDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.signals import on_database_metadata_change, signaleddataset_on_save
from bw2data.tests import bw2test


@bw2test
def test_save_skips_previous_version_query_but_sends_signal_when_not_sourced():
    projects.set_current("plain")
    assert not projects.dataset.is_sourced
    database = DatabaseChooser("db")
    database.register()

    with patch.object(ActivityDataset, "get_or_none") as get_or_none:
        with patch.object(
            signaleddataset_on_save, "send", wraps=signaleddataset_on_save.send
        ) as send:
            with patch.object(revisions, "generate_delta") as generate:
                database.new_node(code="A", name="A").save()

                get_or_none.assert_not_called()
                send.assert_called_once()
                assert send.call_args.kwargs["old"] is None
                assert send.call_args.kwargs["new"].name == "A"
                # signal_dispatcher returns before DeepDiff on unsourced projects
                generate.assert_not_called()
                assert ActivityDataset.get(ActivityDataset.code == "A").name == "A"


@bw2test
def test_save_sends_signal_with_previous_version_when_sourced():
    projects.set_current("plain")
    projects.dataset.set_sourced()
    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    first_revision = projects.dataset.revision
    assert first_revision is not None

    with patch.object(signaleddataset_on_save, "send", wraps=signaleddataset_on_save.send) as send:
        node["name"] = "B"
        node.save()

        send.assert_called_once()
        assert send.call_args.kwargs["old"].name == "A"
        assert send.call_args.kwargs["new"].name == "B"
        assert projects.dataset.revision != first_revision


@bw2test
def test_save_signal_false_still_skips_when_sourced():
    projects.set_current("plain")
    projects.dataset.set_sourced()
    database = DatabaseChooser("db")
    database.register()
    revision = projects.dataset.revision

    with patch.object(signaleddataset_on_save, "send") as send:
        database.new_node(code="A", name="A").save(signal=False)

        send.assert_not_called()
        assert projects.dataset.revision == revision


@bw2test
def test_metadata_flush_skips_reread_and_signal_when_not_sourced():
    projects.set_current("plain")
    database = DatabaseChooser("db")
    database.register()

    with patch.object(databases, "deserialize") as deserialize:
        with patch.object(on_database_metadata_change, "send") as send:
            database.metadata["foo"] = True
            databases.flush()

            deserialize.assert_not_called()
            send.assert_not_called()

    assert databases.deserialize()["db"]["foo"] is True


@bw2test
def test_metadata_flush_sends_signal_and_writes_revision_when_sourced():
    projects.set_current("plain")
    projects.dataset.set_sourced()
    database = DatabaseChooser("db")
    database.register()
    revision = projects.dataset.revision

    with patch.object(
        on_database_metadata_change, "send", wraps=on_database_metadata_change.send
    ) as send:
        database.metadata["foo"] = True
        databases.flush()

        send.assert_called_once()
        assert "foo" not in send.call_args.kwargs["old"]["db"]
        assert send.call_args.kwargs["new"]["db"]["foo"] is True
        assert projects.dataset.revision != revision
