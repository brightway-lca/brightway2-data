# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from future.utils import python_2_unicode_compatible
from eight import *

from . import config
from .errors import ReadOnlyProject
from .filesystem import safe_filename, create_dir
from .sqlite import PickleField, create_database
from fasteners import InterProcessLock
from functools import wraps
from peewee import Model, TextField, BlobField
import appdirs
import collections
import os
import re
import shutil
import sys
import tempfile
import warnings
import wrapt


READ_ONLY_PROJECT = """
***Read only project***

This project is being used by another process and no writes can be made until:
    1. You close the other program, or switch to a different project, *and*
    2. You call `projects.enable_writes` *and* get the response `True`.
"""

@python_2_unicode_compatible
class ProjectDataset(Model):
    data = PickleField()
    name = TextField(index=True, unique=True)

    def __str__(self):
        return "Project: {}".format(self.name)

    __repr__ = lambda x: str(x)

    def __lt__(self, other):
        if not isinstance(other, ProjectDataset):
            raise TypeError
        else:
            return self.name.lower() < other.name.lower()


class ProjectManager(collections.Iterable):
    _basic_directories = (
        "backups",
        "intermediate",
        "lci",
        "processed",
    )

    def __init__(self):
        self._base_data_dir, self._base_logs_dir = self._get_base_directories()
        self.read_only = True
        self._create_base_directories()
        self.db = self._create_projects_database()
        self._set_project("default", False)

    def __iter__(self):
        for project_ds in ProjectDataset.select():
            yield project_ds

    def __contains__(self, name):
        return ProjectDataset.select().where(ProjectDataset.name == name).count() > 0

    def __len__(self):
        return ProjectDataset.select().count()

    def __repr__(self):
        if len(self) > 20:
            return ("Brightway2 projects manager with {} objects, including:"
                    "{}\nUse `sorted(projects)` to get full list, "
                    "`projects.report()` to get\n\ta report on all projects.").format(
                len(self),
                "".join(["\n\t{}".format(x) for x in sorted([x.name for x in self])[:10]])
            )
        else:
            return ("Brightway2 projects manager with {} objects:{}"
                    "\nUse `projects.report()` to get a report on all projects.").format(
                len(self),
                "".join(["\n\t{}".format(x) for x in sorted([x.name for x in self])])
            )

    ### Internal functions for managing projects

    def _get_base_directories(self):
        envvar = os.getenv("BRIGHTWAY2_DIR")
        if envvar:
            if not os.path.isdir(envvar):
                raise OSError(("BRIGHTWAY2_DIR variable is {}, but this is not"
                               " a valid directory").format(envvar))
            else:
                print("Using environment variable BRIGHTWAY2_DIR for data "
                      "directory:\n{}".format(envvar))
                envvar = os.path.abspath(envvar)
                logs_dir = os.path.join(envvar, "logs")
                return envvar, logs_dir

        LABEL = "Brightway2" if sys.version_info < (3, 0) else "Brightway3"
        data_dir = appdirs.user_data_dir(LABEL, "pylca")
        logs_dir = appdirs.user_log_dir(LABEL, "pylca")
        return data_dir, logs_dir

    def _create_base_directories(self):
        create_dir(self._base_data_dir)
        create_dir(self._base_logs_dir)

    def _create_projects_database(self):
        return create_database(
            os.path.join(self._base_data_dir, "projects.db"),
            [ProjectDataset]
        )

    def _get_project(self):
        return self._project_name

    def _set_project(self, name, update=True):
        if name == getattr(self, "_project_name", None):
            return

        if not self.read_only:
            self._lock.release()
        self._project_name = str(name)
        self.create_project(name)
        self._lock = InterProcessLock(os.path.join(self.dir, "write-lock"))
        self.read_only = not self._lock.acquire(timeout = 0.05)

        if self.read_only:
            warnings.warn(READ_ONLY_PROJECT)

        self._reset_meta()
        self._reset_databases()

        if not self.read_only and update:
            self._do_automatic_updates()

    def _do_automatic_updates(self):
        """Run any available automatic updates"""
        from .updates import Updates
        for update_name in Updates.check_automatic_updates():
            print("Applying automatic update: {}".format(update_name))
            Updates.do_update(update_name)

    def _reset_meta(self):
        for obj in config.metadata:
            obj.__init__()

    def _reset_databases(self):
        for name, obj, include_project in config.sqlite3_databases:
            if include_project:
                fp = os.path.join(self.dir, name)
            else:
                fp = os.path.join(self._base_data_dir, name)
            obj.close()
            obj.database = fp
            obj.connect()
            obj.create_tables(
                obj._tables,
                safe=True
            )


    ### Public API

    current = property(_get_project, _set_project)

    @property
    def dir(self):
        return os.path.join(
            self._base_data_dir,
            safe_filename(self.current)
        )

    @property
    def logs_dir(self):
        return os.path.join(
            self._base_logs_dir,
            safe_filename(self.current)
        )

    def create_project(self, name=None, **kwargs):
        name = name or self.current
        if not ProjectDataset().select().where(
                ProjectDataset.name == name).count():
            ProjectDataset().create(
                data=kwargs,
                name=name
            )
        if not os.path.isdir(self.dir):
            create_dir(self.dir)
            for dir_name in self._basic_directories:
                create_dir(os.path.join(self.dir, dir_name))
        if not os.path.isdir(self.logs_dir):
            create_dir(self.logs_dir)

    def enable_writes(self, force=True):
        """Enable writing for the current project."""
        if force:
            os.remove(os.path.join(self.dir, "write-lock"))
        self.read_only = not self._lock.acquire(timeout = 0.05)
        if not self.read_only:
            self._reset_meta()
            self._reset_databases()
        return not self.read_only

    def copy_project(self, new_name, switch=True):
        """Copy current project to a new project named ``new_name``. If ``switch``, switch to new project."""
        if new_name in self:
            raise ValueError("Project {} already exists".format(new_name))
        fp = os.path.join(self._base_data_dir, safe_filename(new_name))
        if os.path.exists(fp):
            raise ValueError("Project directory already exists")
        project_data = ProjectDataset.select(ProjectDataset.name == self.current).get().data
        ProjectDataset().create(data=project_data, name=new_name)
        shutil.copytree(self.dir, fp)
        create_dir(os.path.join(
            self._base_logs_dir,
            safe_filename(new_name)
        ))
        if switch:
            self.current = new_name

    def request_directory(self, name):
        """Return the absolute path to the subdirectory ``dirname``, creating it if necessary.

        Returns ``False`` if directory can't be created."""
        fp = os.path.join(self.dir, str(name))
        create_dir(fp)
        return fp

    def use_temp_directory(self):
        """Use a temporary directory instead of `user_data_dir`. Used for tests."""
        temp_dir = tempfile.mkdtemp()
        self._base_data_dir = os.path.join(temp_dir, "data")
        create_dir(self._base_data_dir)
        self._base_logs_dir = os.path.join(temp_dir, "logs")
        create_dir(self._base_logs_dir)
        self.db = self._create_projects_database()
        self._create_base_directories()
        self.create_project()
        self._reset_meta()
        self._reset_databases()
        return temp_dir

    def delete_project(self, name=None, delete_dir=False):
        """Delete project ``name``, or the current project.

        ``name`` is the project to delete. If ``name`` is not provided, delete the current project.

        By default, the underlying project directory is not deleted; only the project name is removed from the list of active projects. If ``delete_dir`` is ``True``, then also delete the project directory.

        If deleting the current project, this function sets the current directory to ``default`` if it exists, or to a random project.

        Returns the current project."""
        victim = name or self.current
        if victim not in self:
            raise ValueError("{} is not a project".format(victim))
        ProjectDataset.delete().where(ProjectDataset.name == victim).execute()

        if delete_dir:
            dir_path = os.path.join(
                self._base_data_dir,
                safe_filename(victim)
            )
            assert os.path.isdir(dir_path), "Can't find project directory"
            shutil.rmtree(dir_path)

        if name is None:
            if "default" in self:
                self.current = "default"
            else:
                self.current = next(iter(self)).name
        return self.current

    def purge_deleted_directories(self):
        """Delete project directories for projects which are no longer registered.

        Returns number of directories deleted."""
        registered = {safe_filename(obj.name) for obj in self}
        bad_directories = [os.path.join(self._base_data_dir, dirname)
                           for dirname in os.listdir(self._base_data_dir)
                           if os.path.isdir(os.path.join(self._base_data_dir, dirname))
                           and dirname not in registered]

        for fp in bad_directories:
            shutil.rmtree(fp)

        return len(bad_directories)

    def report(self):
        """Give a report on current projects, including installed databases and file sizes.

        Returns tuples of ``(project name, number of databases, size of all databases (GB))``."""
        from . import databases
        _current = self.current
        data = []

        def get_dir_size(dirpath):
            """Modified from http://stackoverflow.com/questions/12480367/how-to-generate-directory-size-recursively-in-python-like-du-does.

            Does not follow symbolic links"""
            return sum(
                sum(os.path.getsize(os.path.join(root, name))
                for name in files
            ) for root, dirs, files in os.walk(dirpath))

        names = sorted([x.name for x in self])
        for obj in names:
            self.current = obj
            data.append((obj, len(databases), get_dir_size(projects.dir) / 1e9))
        self.current = _current
        return data

projects = ProjectManager()


@wrapt.decorator
def writable_project(wrapped, instance, args, kwargs):
    if projects.read_only:
        raise ReadOnlyProject(READ_ONLY_PROJECT)
    return wrapped(*args, **kwargs)
