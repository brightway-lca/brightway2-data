"""Fixtures for bw2data"""

import atexit
import os
import shutil
import sqlite3
import sys
import tempfile
import warnings

import pytest

sqlite3.enable_callback_tracebacks(True)

# Importing bw2data opens the "default" project, which creates tables and runs schema
# migrations in it. Point bw2data at a throwaway directory before anything imports it, so
# that running the tests never changes the user's real data. Tests then switch to their own
# temporary directories with ``bw2test``.
if "bw2data" in sys.modules:
    warnings.warn("bw2data was imported before conftest.py; tests may change the real data dir")
_TEST_DATA_DIR = tempfile.mkdtemp(prefix="bw2data-tests-")
os.environ["BRIGHTWAY2_DIR"] = _TEST_DATA_DIR
atexit.register(shutil.rmtree, _TEST_DATA_DIR, True)


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

    close_handles()
    yield
    close_handles()
