"""Manages Brightway2 projects."""
import tempfile
from pathlib import Path
from typing import Callable, Dict, List, NoReturn

from bw_projects import Configuration as PMConfiguration
from bw_projects import ProjectsManager

from . import config
from .filesystem import create_dir


class BW2DataProjectManager(ProjectsManager):
    """Manage Brightway2 projects."""

    def __init__(
        self,
        dir_base_data: str = None,
        dir_base_logs: str = None,
        database_name: str = "projects.db",
        output_dir_name: str = None,
        max_repr_len: int = 25,
        pm_config: PMConfiguration = PMConfiguration(),
        callbacks_activate_project: List[
            Callable[["ProjectsManager", str, Dict[str, str], str], NoReturn]
        ] = None,
        callbacks_create_project: List[
            Callable[["ProjectsManager", str, Dict[str, str], str], NoReturn]
        ] = None,
        callbacks_delete_project: List[
            Callable[["ProjectsManager", str, Dict[str, str], str], NoReturn]
        ] = None,
        callbacks_copy_project: List[
            Callable[["ProjectsManager", str, Dict[str, str], str], NoReturn]
        ] = None,
    ) -> None:
        if callbacks_activate_project is None:
            callbacks_activate_project = []
        callbacks_activate_project.extend(
            [
                BW2DataProjectManager._reset_meta,
                self._reset_sqlite3_databases,
            ]
        )
        super().__init__(
            dir_base_data,
            dir_base_logs,
            database_name,
            output_dir_name,
            max_repr_len,
            pm_config,
            callbacks_activate_project,
            callbacks_create_project,
            callbacks_delete_project,
            callbacks_copy_project,
        )

    @staticmethod
    def _reset_meta(*_, **__) -> None:
        for obj in config.metadata:
            obj.__init__()

    def _reset_sqlite3_databases(self, *_, **__) -> None:
        for relative_path, substitutable_db in config.sqlite3_databases:
            substitutable_db.change_path(self.data_dir / relative_path)

    def request_directory(self, name):
        """Return the absolute path to the subdirectory ``dirname``, creating it if necessary.
        Returns ``False`` if directory can't be created."""
        abs_path = self.data_dir / str(name)
        create_dir(abs_path)
        if not abs_path.is_dir():
            return False
        return abs_path

    def _use_temp_directory(self):
        """Point the ProjectManager towards a temporary directory instead of `user_data_dir`.
        Used exclusively for tests."""
        temp_dir = Path(tempfile.mkdtemp())
        dir_base_data = temp_dir / "data"
        dir_base_data.mkdir(parents=True, exist_ok=True)
        dir_base_logs = temp_dir / "logs"
        dir_base_logs.mkdir(parents=True, exist_ok=True)
        projects = BW2DataProjectManager(dir_base_data, dir_base_logs)
        projects.create_project("default", activate=True, exist_ok=True)
        return temp_dir


projects = BW2DataProjectManager()
projects.create_project("default", activate=True, exist_ok=True)
