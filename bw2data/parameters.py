# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import databases, projects, config
from .sqlite import PickleField, create_database
from .utils import python_2_unicode_compatible
from bw2parameters import ParameterSet
from peewee import Model, TextField, FloatField, BooleanField
import asteval
import os
import re


# https://stackoverflow.com/questions/34544784/arbitrary-string-to-valid-python-name
clean = lambda varStr: re.sub('\W|^(?=\d)','_', varStr)
nonempty = lambda dct: {k: v for k, v in dct.items() if v is not None}


@python_2_unicode_compatible
class WarningLabel(Model):
    kind = TextField()
    database = TextField(index=True, null=True)
    code = TextField(index=True, null=True)


@python_2_unicode_compatible
class __ParameterBase(Model):
    __repr__ = lambda x: str(x)

    def __lt__(self, other):
        if type(self) != type(other):
            raise TypeError
        else:
            return self.name.lower() < other.name.lower()


class ProjectParameter(__ParameterBase):
    name = TextField(index=True, unique=True)
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    def __str__(self):
        return "Project parameter: {}".format(self.name)

    def save(self, *args, **kwargs):
        WarningLabel.get_or_create(kind='project')
        super(ProjectParameter, self).save(*args, **kwargs)

    @staticmethod
    def load():
        def reformat(o):
            o = o.dict
            return (o.pop("name"), o)
        return dict([reformat(o) for o in ProjectParameter.select()])

    @staticmethod
    def static():
        return ProjectParameter.select(ProjectParameter.name,
            ProjectParameter.amount).dicts()

    @staticmethod
    def expired():
        return bool(WarningLabel.select().where(WarningLabel.kind=='project').count())

    @staticmethod
    def recalculate():
        if not ProjectParameter.expired():
            return
        data = ProjectParameter.load()
        ParameterSet(data).evaluate_and_set_amount_field()
        with parameters.db.atomic() as _:
            for key, value in data.items():
                # TODO: Update `data` too
                ProjectParameter.update(amount=value['amount']).\
                where(ProjectParameter.name==key).execute()
        WarningLabel.delete().where(WarningLabel.kind=='project').execute()

    @property
    def dict(self):
        obj = nonempty({
            'name': self.name,
            'formula': self.formula,
            'amount': self.amount,
        })
        obj.update(self.data)
        return obj


class DatabaseParameter(__ParameterBase):
    database = TextField(index=True)
    name = TextField(index=True)
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    class Meta:
        indexes = (
            (('database', 'name'), True),
        )

    def __str__(self):
        return "Project parameter: {}:{}".format(self.database, self.name)

    @staticmethod
    def load(database):
        return [o.dict for o in DatabaseParameter.select().where(
            DatabaseParameter.database == database)]

    @staticmethod
    def expired():
        for m in WarningLabel.select(WarningLabel.database).where(
                WarningLabel.kind=='database').tuples():
            yield m[0]

    def save(self, *args, **kwargs):
        WarningLabel.get_or_create(kind='database', database=self.database)
        super(DatabaseParameter, self).save(*args, **kwargs)

    def dict(self):
        obj = nonempty({
            'database': self.database,
            'name': self.name,
            'formula': self.formula,
            'amount': self.amount,
        })
        obj.update(self.data)
        return obj


class ActivityParameter(__ParameterBase):
    database = TextField(index=True)
    code = TextField(index=True)
    name = TextField(index=True)
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    class Meta:
        indexes = (
            (('database', 'code', 'name'), True),
        )

    def __str__(self):
        return "Activity parameter: {}".format(self.name)

    @staticmethod
    def load(activity):
        database, code = activity[0], activity[1]
        return [
            o.dict for o in ActivityParameter.select().where(
            ActivityParameter.database == database,
            ActivityParameter.code == code)
        ]

    @staticmethod
    def expired():
        return WarningLabel.select(WarningLabel.database, WarningLabel.code
            ).where(WarningLabel.kind=='activity').tuples()

    def save(self, *args, **kwargs):
        WarningLabel.get_or_create(kind='activity', database=self.database, code=self.code)
        super(ActivityParameter, self).save(*args, **kwargs)

    def dict(self):
        obj = nonempty({
            'database': self.database,
            'code': self.code,
            'name': self.name,
            'formula': self.formula,
            'amount': self.amount,
        })
        obj.update(self.data)
        return obj


@python_2_unicode_compatible
class ParameterGroup(Model):
    # Named group of one or more activities
    # Order of resolution is: activities in order of insertion, database, global
    # Database params can't depend on other databases
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
            [DatabaseParameter, ProjectParameter,
             ActivityParameter, WarningLabel]
        )
        config.sqlite3_databases.append((
            "parameters.db",
            self.db,
            [DatabaseParameter, ProjectParameter,
             ActivityParameter, WarningLabel]
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
        return (DatabaseParameter.select().count() + ProjectParameter.select().count() +
            ActivityParameter.select().count())

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


def get_new_symbols(data, context=None):
    interpreter = asteval.Interpreter()
    BUILTIN_SYMBOLS = set(interpreter.symtable) + (context or set())
    found = set()
    for ds in data:
        if 'formula' in ds:
            nf = asteval.NameFinder()
        nf.generic_visit(interpreter.parse(expression))
        found.add(nf.names)
    return found.difference(BUILTIN_SYMBOLS)

