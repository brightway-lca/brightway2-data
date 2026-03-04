"""Fixtures for bw2data"""

import gc
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
    from bw2data import config
    from bw2data.project import projects

    def close_handles():
        for _, substitutable_db in config.sqlite3_databases:
            try:
                if not substitutable_db.db.is_closed():
                    substitutable_db.db.close()
            except Exception:
                pass

        try:
            if not projects.db.db.is_closed():
                projects.db.db.close()
        except Exception:
            pass

        # Encourage cleanup of objects holding stale SQLite handles.
        gc.collect()

    close_handles()
    yield
    close_handles()
