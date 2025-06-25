"""Fixtures for bw2data"""

import sqlite3

import pytest

sqlite3.enable_callback_tracebacks(True)


@pytest.fixture
def num_revisions():
    def _num_revisions(projects) -> int:
        return len(
            [
                fp
                for fp in (projects.dataset.dir / "revisions").iterdir()
                if fp.stem.lower() != "head" and fp.is_file()
            ]
        )

    return _num_revisions


@pytest.fixture(autouse=True, scope="function")
def close_database_handle_on_exit_to_avoid_windows_test_errors():
    from bw2data.backends import sqlite3_lci_db

    yield
    sqlite3_lci_db.db.close()
