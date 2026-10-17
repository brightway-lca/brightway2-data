from unittest.mock import patch

from bw2data import databases, revisions
from bw2data.database import DatabaseChooser
from bw2data.project import projects, signal_dispatcher, signal_dispatcher_on_database_write
from bw2data.tests import bw2test


@bw2test
def test_signal_dispatcher_skips_delta_when_not_sourced():
    projects.set_current("plain")
    assert not projects.dataset.is_sourced

    with patch.object(revisions, "generate_delta") as generate:
        result = signal_dispatcher(
            None,
            old={"id": 1, "a": 1},
            new={"id": 1, "a": 2},
            operation="activity_code_change",
        )

    generate.assert_not_called()
    assert result is None


@bw2test
def test_signal_dispatcher_generates_delta_when_sourced():
    projects.set_current("plain")
    projects.dataset.set_sourced()

    with patch.object(revisions, "generate_delta", wraps=revisions.generate_delta) as generate:
        signal_dispatcher(
            None,
            old={"id": 1, "a": 1},
            new={"id": 1, "a": 2},
            operation="activity_code_change",
        )

    generate.assert_called_once()
    assert projects.dataset.revision is not None


@bw2test
def test_metadata_flush_skips_delta_when_not_sourced():
    projects.set_current("plain")
    database = DatabaseChooser("db")
    database.register()

    with patch.object(revisions, "generate_delta") as generate:
        database.metadata["foo"] = True
        databases.flush()

    generate.assert_not_called()


@bw2test
def test_database_write_dispatcher_skips_deltas_when_not_sourced():
    projects.set_current("plain")
    database = DatabaseChooser("db")
    database.register()
    database.write({("db", "A"): {"name": "A"}})

    with patch.object(revisions.Delta, "generate") as generate:
        result = signal_dispatcher_on_database_write(None, name="db")

    generate.assert_not_called()
    assert result is None
