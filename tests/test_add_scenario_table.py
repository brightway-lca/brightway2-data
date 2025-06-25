import tempfile
from pathlib import Path

from peewee import SqliteDatabase

from bw_processing import safe_filename
from bw2data import projects
from bw2data.backends.schema import ActivityDataset, ExchangeDataset, Scenario
import bw2io as bi
from bw2data.tests import bw2test


@bw2test
def test_addition_of_scenario_fk_column(fixture_dir):
    import logging
    logger = logging.getLogger('peewee')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    bi.restore_project_directory(
        fixture_dir / "without-scenario-table.tar.gz",
        project_name="test-scenario",
        switch=False
    )

    db = SqliteDatabase(projects._base_data_dir / safe_filename("test-scenario", full=False) / "lci" / "databases.db")
    db.connect()
    assert "scenario" not in db.get_tables()
    assert "scenario_id" not in {col.name for col in db.get_columns('exchangedataset')}
    db.close()

    projects.set_current("test-scenario")

    db = SqliteDatabase(projects._base_data_dir / safe_filename("test-scenario", full=False) / "lci" / "databases.db")
    db.connect()
    assert "scenario" in db.get_tables()
    assert "scenario_id" in {col.name for col in db.get_columns('exchangedataset')}
    db.close()
