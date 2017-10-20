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
clean = lambda x: re.sub('\W|^(?=\d)','_', x)
clean_db = lambda x: "db_" + clean(x)
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
        ParameterGroup.get_or_create(name='project')
        WarningLabel.get_or_create(kind='project')
        super(ProjectParameter, self).save(*args, **kwargs)

    @staticmethod
    def load():
        def reformat(o):
            o = o.dict
            return (o.pop("name"), o)
        return dict([reformat(o) for o in ProjectParameter.select()])

    @staticmethod
    def static(only=None):
        qs = ProjectParameter.select(ProjectParameter.name, ProjectParameter.amount)
        if qs is not None:
            qs = qs.where(ProjectParameter.name << tuple(only))
        return dict(qs.tuples())

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
        return "Database parameter: {}:{}".format(self.database, self.name)

    @staticmethod
    def load(database):
        def reformat(o):
            o = o.dict
            return (o.pop("name"), o)
        return dict([reformat(o) for o in DatabaseParameter.select().where(
            DatabaseParameter.database == database)])

    @staticmethod
    def expired(database=None):
        qs = WarningLabel.select(WarningLabel.database)
        if database is None:
            if not ProjectParameter.expired():
                qs = qs.where(WarningLabel.kind=='database')
            for m in qs.tuples():
                yield m[0]
        else:
            return (ProjectParameter.expired() or
                qs.where(
                    WarningLabel.kind=='database',
                    WarningLabel.database==database,
                ).count())

    @staticmethod
    def static(database):
        return dict(DatabaseParameter.select(DatabaseParameter.name,
            DatabaseParameter.amount).where(
            DatabaseParameter.database==database).tuples())

    @staticmethod
    def recalculate(database):
        if ProjectParameter.expired():
            ProjectParameter.recalculate()
        if not DatabaseParameter.expired(database):
            return

        data = DatabaseParameter.load(database)

        # Add or delete `project` dependency if needed
        new_symbols = get_new_symbols(data.values(), set(data))
        found_symbols = {x[0] for x in ProjectParameter.select(
            ProjectParameter.name).tuples()}
        missing = new_symbols.difference(found_symbols)
        if missing:
            raise ValueError("The following symbols aren't defined:\t{}".format(";".join(missing)))
        if new_symbols:
            GroupDependency.get_or_create(
                group=clean_db(database), depends="project"
            )
            glo = ProjectParameter.static(new_symbols)
        else:
            glo = None

        ParameterSet(data, glo).evaluate_and_set_amount_field()
        with parameters.db.atomic() as _:
            for key, value in data.items():
                # TODO: Update `data` too
                DatabaseParameter.update(amount=value['amount']).where(
                    DatabaseParameter.name==key,
                    DatabaseParameter.database==database,
                ).execute()
        WarningLabel.delete().where(
            WarningLabel.kind=='database',
            WarningLabel.database==database,
        ).execute()
        # TODO: Expire any activities linking to this group

    def save(self, *args, **kwargs):
        ParameterGroup.get_or_create(name=clean_db(self.database))
        WarningLabel.get_or_create(kind='database', database=self.database)
        super(DatabaseParameter, self).save(*args, **kwargs)

    @property
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
    name = TextField()
    data = PickleField(default={})

    # Named group of one or more activities
    # Order of resolution is: activities in order of insertion, database, global
    # Database params can't depend on other databases

    def add_activity(activity):
        # Add in all activity-level and exchange-level parameters
        # Raise error if exchange-level parameters have name conflicts
        # Also modifies activity to add `group` attribute
        pass

    def add_dependency(group):
        # Add group to self.dependencies
        # Atomic
        pass


@python_2_unicode_compatible
class GroupDependency(Model):
    group = TextField()
    depends = TextField()

    class Meta:
        indexes = (
            (('group', 'depends'), True),
        )


class ParameterManager(object):
    def __init__(self):
        self.db = create_database(
            os.path.join(projects.dir, "parameters.db"),
            [DatabaseParameter, ProjectParameter,
             ActivityParameter, WarningLabel,
             ParameterGroup, GroupDependency]
        )
        config.sqlite3_databases.append((
            "parameters.db",
            self.db,
            [DatabaseParameter, ProjectParameter,
             ActivityParameter, WarningLabel,
             ParameterGroup, GroupDependency]
        ))

    @property
    def project(self):
        return ProjectParameter.select()

    def database(self, database):
        assert database in databases, "Database not found"
        return DatabaseParameter.select().where(DatabaseParameter.database == database)

    def insert_project_parameters(self, data):
        """Efficiently and correctly enter multiple parameters.

        ``data`` should be a list of dictionaries."""
        keyed = {ds['name']: ds for ds in data}
        assert len(keyed) == len(data), "Nonunique names"
        ParameterSet(keyed).evaluate_and_set_amount_field()

        def reformat(ds):
            return nonempty({
                'name': ds.pop('name'),
                'amount': ds.pop('amount'),
                'formula': ds.pop('formula', None),
                'data': ds
            })
        data = [reformat(ds) for ds in data]

        with self.db.atomic():
            ProjectParameter.delete().execute()
            for idx in range(0, len(data), 100):
                ProjectParameter.insert_many(data[idx:idx+100]).execute()
        WarningLabel.delete().where(WarningLabel.kind=='project')

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
    BUILTIN_SYMBOLS = set(interpreter.symtable).union(set(context or set()))
    found = set()
    for ds in data:
        if 'formula' in ds:
            nf = asteval.NameFinder()
            nf.generic_visit(interpreter.parse(ds['formula']))
            found.update(set(nf.names))
    return found.difference(BUILTIN_SYMBOLS)
