import os
import shutil
import warnings
from collections.abc import Iterable
from copy import copy
from pathlib import Path
from typing import Optional

import wrapt
from bw_processing import safe_filename
from peewee import SQL, BooleanField, DoesNotExist, Model, TextField
from platformdirs import PlatformDirs

from bw2data import config
from bw2data.filesystem import create_dir
from bw2data.signals import project_changed, project_created
from bw2data.sqlite import PickleField, SubstitutableDatabase
from bw2data.utils import maybe_path

READ_ONLY_PROJECT = """
***Read only project***

This project is being used by another process and no writes can be made until:
    1. You close the other program, or switch to a different project, *and*
    2. You call `projects.enable_writes` *and* get the response `True`.

    If you are **sure** that this warning is incorrect, call
    `projects.enable_writes(force=True)` to enable writes.
"""


def lockable():
    return False


class ProjectDataset(Model):
    data = PickleField()
    name = TextField(index=True, unique=True)
    # Peewee doesn't set defaults in the database but rather in Python.
    # But for backwards compatibility we need a default `True` value
    # and this hack is the recommended way to get this behaviour.
    # See https://docs.peewee-orm.com/en/latest/peewee/models.html?highlight=table%20generation
    full_hash = BooleanField(default=True, constraints=[SQL("DEFAULT 1")])

    def __str__(self):
        return "Project: {}".format(self.name)

    __repr__ = lambda x: str(x)

    def __lt__(self, other):
        if not isinstance(other, ProjectDataset):
            raise TypeError
        else:
            return self.name.lower() < other.name.lower()


class ProjectManager(Iterable):
    _basic_directories = (
        "backups",
        "intermediate",
        "lci",
        "processed",
    )
    _is_temp_dir = False
    read_only = False

    def __init__(self):
        self._base_data_dir, self._base_logs_dir = self._get_base_directories()
        self._create_base_directories()
        self.db = SubstitutableDatabase(self._base_data_dir / "projects.db", [ProjectDataset])

        columns = {o.name for o in self.db._database.get_columns("projectdataset")}
        if "full_hash" not in columns:
            src_filepath = self._base_data_dir / "projects.db"
            backup_filepath = self._base_data_dir / "projects.backup.db"
            shutil.copy(src_filepath, backup_filepath)

            MIGRATION_WARNING = """Adding a column to the projects database. A backup copy of this database '{}' was made at '{}'; if you have problems, file an issue, and restore the backup data to use the stable version of Brightway2."""

            print(MIGRATION_WARNING.format(src_filepath, backup_filepath))

            ADD_FULL_HASH_COLUMN = (
                """ALTER TABLE projectdataset ADD COLUMN "full_hash" integer default 1"""
            )
            self.db.execute_sql(ADD_FULL_HASH_COLUMN)

            # We don't do this, as the column added doesn't have a default
            # value, meaning that one would get error from using the
            # development branch alongside the stable branch.

            # from playhouse.migrate import SqliteMigrator, migrate
            # migrator = SqliteMigrator(self.db._database)
            # full_hash = BooleanField(default=True)
            # migrate(migrator.add_column("projectdataset", "full_hash", full_hash),)
        self.set_current("default", update=False)

    def __iter__(self):
        for project_ds in ProjectDataset.select():
            yield project_ds

    def __contains__(self, name):
        return ProjectDataset.select().where(ProjectDataset.name == name).count() > 0

    def __len__(self):
        return ProjectDataset.select().count()

    def __repr__(self):
        if len(self) > 20:
            return (
                "Brightway2 projects manager with {} objects, including:"
                "{}\nUse `sorted(projects)` to get full list, "
                "`projects.report()` to get\n\ta report on all projects."
            ).format(
                len(self),
                "".join(["\n\t{}".format(x) for x in sorted([x.name for x in self])[:10]]),
            )
        else:
            return (
                "Brightway2 projects manager with {} objects:{}"
                "\nUse `projects.report()` to get a report on all projects."
            ).format(
                len(self),
                "".join(["\n\t{}".format(x) for x in sorted([x.name for x in self])]),
            )

    ### Internal functions for managing projects

    def _get_base_directories(self):
        envvar = maybe_path(os.getenv("BRIGHTWAY2_DIR"))
        if envvar:
            if not envvar.is_dir():
                raise OSError(
                    ("BRIGHTWAY2_DIR variable is {}, but this is not" " a valid directory").format(
                        envvar
                    )
                )
            else:
                print(
                    "Using environment variable BRIGHTWAY2_DIR for data "
                    "directory:\n{}".format(envvar)
                )
                envvar = envvar.absolute()
                logs_dir = envvar / "logs"
                create_dir(logs_dir)
                return envvar, logs_dir

        dirs = PlatformDirs("Brightway3", "pylca")
        data_dir = Path(dirs.user_data_dir)
        logs_dir = Path(dirs.user_log_dir)
        return data_dir, logs_dir

    def _create_base_directories(self):
        create_dir(self._base_data_dir)
        create_dir(self._base_logs_dir)

    def change_base_directories(
        self,
        base_dir: Path,
        base_logs_dir: Optional[Path] = None,
        project_name: Optional[str] = "default",
        update: Optional[bool] = True,
    ) -> None:
        if not isinstance(base_dir, Path) and base_dir.is_dir():
            raise ValueError(f"{base_dir} is not a `Path` or not a directory")
        if not os.access(base_dir, os.W_OK) and os.access(base_dir, os.R_OK):
            raise ValueError(f"{base_dir} doesn't have read and write permissions")
        self._base_data_dir = base_dir

        if base_logs_dir is not None:
            if not isinstance(base_logs_dir, Path) and base_logs_dir.is_dir():
                raise ValueError(f"{base_logs_dir} is not a `Path` or not a directory")
            if not os.access(base_logs_dir, os.W_OK):
                raise ValueError(f"{base_logs_dir} doesn't have write permission")
            self._base_logs_dir = base_logs_dir

        config.cache = {}
        self.db.change_path(base_dir / "projects.db")
        self.set_current(project_name, update=update)

    @property
    def current(self):
        return self._project_name

    @property
    def twofive(self):
        return bool(self.dataset.data.get("25"))

    def set_current(self, name, writable=True, update=True):
        self._project_name = str(name)

        # Need to allow writes when creating a new project
        # for new metadata stores
        self.read_only = False
        self.create_project(name)
        self.dataset = ProjectDataset.get(ProjectDataset.name == self._project_name)
        self._reset_meta()
        self._reset_sqlite3_databases()
        project_changed.send(self.dataset)

        if not writable:
            self.read_only = True

        if not self.read_only and update:
            self._do_automatic_updates()

    def _do_automatic_updates(self):
        """Run any available automatic updates"""
        from bw2data.updates import Updates

        for update_name in Updates.check_automatic_updates():
            print("Applying automatic update: {}".format(update_name))
            Updates.do_update(update_name)

    def _reset_meta(self):
        for obj in config.metadata:
            obj.__init__()

    def _reset_sqlite3_databases(self):
        for relative_path, substitutable_db in config.sqlite3_databases:
            substitutable_db.change_path(self.dir / relative_path)

    ### Public API
    @property
    def dir(self):
        return Path(self._base_data_dir) / safe_filename(self.current, full=self.dataset.full_hash)

    @property
    def logs_dir(self):
        return Path(self._base_logs_dir) / safe_filename(self.current, full=self.dataset.full_hash)

    @property
    def output_dir(self):
        """Get directory for output files.

        Uses environment variable ``BRIGHTWAY2_OUTPUT_DIR``; ``preferences['output_dir']``; or directory ``output`` in current project.

        Returns output directory path.

        """
        ep, pp = (
            maybe_path(os.getenv("BRIGHTWAY2_OUTPUT_DIR")),
            maybe_path(config.p.get("output_dir")),
        )
        if ep and ep.is_dir():
            return ep
        elif pp and pp.is_dir():
            return pp
        else:
            return self.request_directory("output")

    def create_project(self, name=None, **kwargs):
        name = name or self.current

        kwargs["25"] = True
        full_hash = kwargs.pop("full_hash", False)
        try:
            self.dataset = ProjectDataset.get(ProjectDataset.name == name)
        except DoesNotExist:
            self.dataset = ProjectDataset.create(data=kwargs, name=name, full_hash=full_hash)
            project_created.send(self.dataset)
        create_dir(self.dir)
        for dir_name in self._basic_directories:
            create_dir(self.dir / dir_name)
        create_dir(self.logs_dir)

    def copy_project(self, new_name, switch=True):
        """Copy current project to a new project named ``new_name``. If ``switch``, switch to new project."""
        if new_name in self:
            raise ValueError("Project {} already exists".format(new_name))
        fp = self._base_data_dir / safe_filename(new_name, full=self.dataset.full_hash)
        if fp.exists():
            raise ValueError("Project directory already exists")
        project_data = ProjectDataset.get(ProjectDataset.name == self.current).data
        ProjectDataset.create(data=project_data, name=new_name, full_hash=self.dataset.full_hash)
        shutil.copytree(self.dir, fp)
        create_dir(self._base_logs_dir / safe_filename(new_name))
        if switch:
            self.set_current(new_name)

    def request_directory(self, name):
        """Return the absolute path to the subdirectory ``dirname``, creating it if necessary.

        Returns ``False`` if directory can't be created."""
        fp = self.dir / str(name)
        create_dir(fp)
        if not fp.is_dir():
            return False
        return fp

    def migrate_project_25(self):
        """Migrate project to Brightway 2.5.

        Reprocesses all databases and LCIA objects."""
        assert not self.twofive, "Project is already 2.5 compatible"

        from bw2data.updates import Updates

        Updates()._reprocess_all()

        self.dataset.data["25"] = True
        self.dataset.save()

    def delete_project(self, name=None, delete_dir=False):
        """Delete project ``name``, or the current project.

        ``name`` is the project to delete. If ``name`` is not provided, delete the current project.

        By default, the underlying project directory is not deleted; only the project name is removed from the list of active projects. If ``delete_dir`` is ``True``, then also delete the project directory.

        If deleting the current project, this function sets the current directory to ``default`` if it exists, or to a random project.

        Returns the current project."""
        victim = name or self.current
        if victim not in self:
            raise ValueError("{} is not a project".format(victim))

        if len(self) == 1:
            raise ValueError("Can't delete only remaining project")

        ProjectDataset.delete().where(ProjectDataset.name == victim).execute()

        if delete_dir:
            dir_path = self._base_data_dir / safe_filename(victim)
            assert dir_path.is_dir(), "Can't find project directory"
            shutil.rmtree(dir_path)

        if name is None or name == self.current:
            if "default" in self:
                self.set_current("default")
            else:
                self.set_current(next(iter(self)).name)
        return self.current

    def purge_deleted_directories(self):
        """Delete project directories for projects which are no longer registered.

        Returns number of directories deleted."""
        registered = {safe_filename(obj.name) for obj in self}
        bad_directories = [
            self._base_data_dir / dirname
            for dirname in os.listdir(self._base_data_dir)
            if (self._base_data_dir / dirname).is_dir() and dirname not in registered
        ]

        for fp in bad_directories:
            shutil.rmtree(fp)

        return len(bad_directories)

    def report(self):
        """Give a report on current projects, including installed databases and file sizes.

        Returns tuples of ``(project name, number of databases, size of all databases (GB))``.
        """
        from bw2data import databases

        _current = self.current
        data = []

        def get_dir_size(dirpath):
            """Modified from http://stackoverflow.com/questions/12480367/how-to-generate-directory-size-recursively-in-python-like-du-does.

            Does not follow symbolic links"""
            return sum(
                sum(os.path.getsize(Path(root) / name) for name in files)
                for root, dirs, files in os.walk(dirpath)
            )

        names = sorted([x.name for x in self])
        for obj in names:
            self.set_current(obj, update=False, writable=False)
            data.append((obj, len(databases), get_dir_size(projects.dir) / 1e9))
        self.set_current(_current)
        return data

    def rename_project(self, new_name: str) -> None:
        """Rename current project, and switch to the new project."""
        from bw2data import databases

        if new_name in databases:
            raise ValueError(f"Project `{new_name}` already exists.")

        warnings.warn(
            "Renaming current project; this is relatively expensive and could take a little while."
        )

        old_name = copy(self.current)
        self.copy_project(new_name)
        self.set_current(new_name)
        self.delete_project(old_name, delete_dir=True)

    def use_short_hash(self):
        if not self.dataset.full_hash:
            return
        try:
            old_dir, old_logs_dir = self.dir, self.logs_dir
            self.dataset.full_hash = False
            if self.dir.exists():
                raise OSError("Target directory {} already exists".format(self.dir))
            if self.logs_dir.exists():
                raise OSError("Target directory {} already exists".format(self.logs_dir))
            old_dir.rename(self.dir)
            old_logs_dir.rename(self.logs_dir)
            self.dataset.save()
        except Exception as ex:
            self.dataset.full_hash = True
            raise ex

    def use_full_hash(self):
        if self.dataset.full_hash:
            return
        try:
            old_dir, old_logs_dir = self.dir, self.logs_dir
            self.dataset.full_hash = True
            if self.dir.exists():
                raise OSError("Target directory {} already exists".format(self.dir))
            if self.logs_dir.exists():
                raise OSError("Target directory {} already exists".format(self.logs_dir))
            old_dir.rename(self.dir)
            old_logs_dir.rename(self.logs_dir)
            self.dataset.save()
        except Exception as ex:
            self.dataset.full_hash = False
            raise ex


projects = ProjectManager()


@wrapt.decorator
def writable_project(wrapped, instance, args, kwargs):
    warnings.warn("`writable_project` is obsolete and does nothing", DeprecationWarning)
    return wrapped(*args, **kwargs)
