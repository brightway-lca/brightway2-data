# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from eight import *
from .filesystem import safe_filename, create_dir
from .sqlite import PickleField, create_database
from peewee import Model, TextField, BlobField
import appdirs
import collections
import os
import re
import shutil
import tempfile


class ProjectDataset(Model):
    data = PickleField()
    name = TextField(index=True, unique=True)


class ProjectManager(collections.Iterable):
    _basic_directories = (
        "backups",
        "intermediate",
        "lci",
        "processed",
    )

    def __init__(self):
        self._base_data_dir = appdirs.user_data_dir(
            "Brightway2",
            "pylca",
        )
        self._base_logs_dir = appdirs.user_log_dir(
            "Brightway2",
            "pylca",
        )
        self._create_base_directories()
        self._project_name = "default"
        self.db = self._create_projects_database()
        self.create_project()

    def __iter__(self):
        for project_ds in ProjectDataset.select():
            yield project_ds

    def __contains__(self, name):
        return ProjectDataset.select(ProjectDataset.name == name).count() > 0

    ### Internal functions for managing projects

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

    def _set_project(self, name):
        self._project_name = str(name)
        self.create_project(name)
        self._reset_meta()
        self._reset_databases()

    def _reset_meta(self):
        from . import config
        for obj in config.metadata:
            obj.__init__()
        config.load_preferences()

    def _reset_databases(self):
        from . import config
        for name, obj, include_project in config.sqlite3_databases:
            if include_project:
                fp = os.path.join(self.dir, name)
            else:
                fp = os.path.join(self._base_data_dir, name)
            obj.close()
            obj.database = fp
            obj.connect()

    ### Public API

    project = property(_get_project, _set_project)

    @property
    def dir(self):
        return os.path.join(
            self._base_data_dir,
            safe_filename(self.project)
        )

    @property
    def logs_dir(self):
        return os.path.join(
            self._base_logs_dir,
            safe_filename(self.project)
        )

    def create_project(self, name="default", **kwargs):
        if not os.path.isdir(self.dir):
            ProjectDataset().create(data=kwargs, name="default")
            create_dir(self.dir)
            create_dir(self.logs_dir)
            for dir_name in self._basic_directories:
                create_dir(os.path.join(self.dir, dir_name))

    def copy_project(self, new_name, switch=True):
        """Copy current project to a new project named ``new_name``. If ``switch``, switch to new project."""
        if new_name in self:
            raise ValueError("Project {} already exists".format(new_name))
        fp = os.path.join(self._base_data_dir, safe_filename(new_name))
        if os.path.exists(fp):
            raise ValueError("Project directory already exists")
        project_data = ProjectDataset.select(ProjectDataset.name == self.project).get().data
        ProjectDataset().create(data=project_data, name=new_name)
        shutil.copytree(self.dir, fp)
        create_dir(os.path.join(
            self._base_logs_dir,
            safe_filename(new_name)
        ))
        if switch:
            self.project = new_name

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


projects = ProjectManager()
