import shutil
from pathlib import Path

from peewee import SqliteDatabase

from bw2data.project import ProjectDataset, add_sourced_columns, config, projects
from bw2data.tests import bw2test

original_projects_db = Path(__file__).parent / "fixtures" / "projects.db"


def test_add_sourced_columns(tmp_path):
    new_projects_db = tmp_path / "projects.db"
    shutil.copy(original_projects_db, new_projects_db)
    assert new_projects_db.is_file()

    db = SqliteDatabase(new_projects_db)
    db.connect()

    columns = {o.name for o in db.get_columns("projectdataset")}
    assert "is_sourced" not in columns
    assert "revision" not in columns

    add_sourced_columns(tmp_path, db)

    assert (tmp_path / "projects.backup-is-sourced.db").is_file()
    assert new_projects_db.is_file()

    columns = {o.name: o for o in db.get_columns("projectdataset")}
    assert "is_sourced" in columns
    assert columns["is_sourced"].data_type.upper() == "INTEGER"
    assert columns["is_sourced"].default == "0"
    assert columns["is_sourced"].null is True
    assert "revision" in columns
    assert columns["revision"].data_type.upper() == "INTEGER"
    assert columns["revision"].default is None
    assert columns["revision"].null is True

    db = SqliteDatabase(tmp_path / "projects.backup-is-sourced.db")
    db.connect()
    columns = {o.name for o in db.get_columns("projectdataset")}
    assert "is_sourced" not in columns


@bw2test
def test_project_migration(tmp_path):
    new_projects_db = tmp_path / "projects.db"
    shutil.copy(original_projects_db, new_projects_db)

    db = SqliteDatabase(new_projects_db)
    db.connect()

    columns = {o.name for o in db.get_columns("projectdataset")}
    assert "is_sourced" not in columns
    assert "revision" not in columns

    projects._base_data_dir = tmp_path
    projects._create_base_directories()
    add_sourced_columns(base_data_dir=projects._base_data_dir, db=db)

    config.cache = {}
    projects.db.change_path(new_projects_db)
    projects.set_current("default")
    default = projects.dataset = ProjectDataset.get(ProjectDataset.name == projects._project_name)

    assert not default.is_sourced
    default.set_sourced()

    default = ProjectDataset.get(ProjectDataset.name == projects._project_name)
    assert default.is_sourced
