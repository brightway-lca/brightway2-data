# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import databases, projects, config
from .sqlite import PickleField, create_database
from .utils import python_2_unicode_compatible
from bw2parameters import ParameterSet
from peewee import Model, TextField, FloatField, BooleanField
import os


@python_2_unicode_compatible
class WarningLabel(Model):
    kind = TextField()
    database = TextField(index=True, null=True)


@python_2_unicode_compatible
class ProjectParameter(Model):
    name = TextField(index=True, unique=True)
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    def __str__(self):
        return "Project parameter: {}".format(self.name)

    __repr__ = lambda x: str(x)

    def __lt__(self, other):
        if not isinstance(other, ProjectParameter):
            raise TypeError
        else:
            return self.name.lower() < other.name.lower()

    def save(self, *args, **kwargs):
        WarningLabel.get_or_create(kind='project')
        super(ProjectParameter, self).save(*args, **kwargs)

    @staticmethod
    def expired():
        return bool(WarningLabel.select().where(WarningLabel.kind=='project').count())

    @property
    def dict(self):
        obj = {
            'name': self.name,
            'formula': self.formula,
            'amount': self.amount,
        }
        obj.update(self.data)
        return obj


@python_2_unicode_compatible
class DatabaseParameter(Model):
    database = TextField(index=True)
    name = TextField(index=True, unique=True)
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    def __str__(self):
        return "Project parameters: {}".format(self.name)

    __repr__ = lambda x: str(x)

    def __lt__(self, other):
        if not isinstance(other, DatabaseParameter):
            raise TypeError
        else:
            return self.name.lower() < other.name.lower()

    @staticmethod
    def expired():
        for m in WarningLabel.select(WarningLabel.database).where(
                WarningLabel.kind=='database').tuples():
            yield m[0]

    def save(self, *args, **kwargs):
        WarningLabel.get_or_create(kind='database', database=self.database)
        super(DatabaseParameter, self).save(*args, **kwargs)

    def dict(self):
        obj = {
            'name': self.name,
            'formula': self.formula,
            'amount': self.amount,
        }
        obj.update(self.data)
        return obj


@python_2_unicode_compatible
class ParameterGroup(Model):
    # Named group of one or more activities
    # Order of resolution is: activities in order of insertion, database, global
    # Store: name, dependencies (list), data (including mangled namespace), dirty flag

    def add_activity(activity):
        # Add in all activity-level and exchange-level parameters
        # Raise error if exchange-level parameters have name conflicts
        # Also modifies activity to add `group` attribute
        pass

    def add_dependency(group):
        # Add group to self.dependencies
        # Atomic
        pass


class ParameterManager(object):
    def __init__(self):
        self.db = create_database(
            os.path.join(projects.dir, "parameters.db"),
            [DatabaseParameter, ProjectParameter, WarningLabel]
        )
        config.sqlite3_databases.append((
            "parameters.db",
            self.db,
            [DatabaseParameter, ProjectParameter, WarningLabel]
        ))

    @property
    def project(self):
        return ProjectParameter.select()

    def database(self, database):
        assert database in databases, "Database not found"
        return DatabaseParameter.select().where(DatabaseParameter.database == database)

    def update_all(self):
        if ProjectParameter.expired():
            update_project_parameters()
        for db in DatabaseParameter.expired():
            update_database_parameters(db)

    def __len__(self):
        return DatabaseParameter.select().count() + ProjectParameter.select().count()

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


parameters = ParameterManager()
