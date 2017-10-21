# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import databases, projects, config, get_activity
from .sqlite import PickleField, create_database
from .utils import python_2_unicode_compatible
from collections import defaultdict
from bw2parameters import ParameterSet
from bw2parameters.errors import MissingName
from peewee import (
    BooleanField,
    Check,
    DateTimeField,
    FloatField,
    IntegerField,
    Model,
    TextField,
)
import asteval
import os
import re
import datetime


# https://stackoverflow.com/questions/34544784/arbitrary-string-to-valid-python-name
clean = lambda x: re.sub('\W|^(?=\d)','_', x)
nonempty = lambda dct: {k: v for k, v in dct.items() if v is not None}

TRIGGER = """CREATE TRIGGER IF NOT EXISTS {table}_{action}_trigger AFTER {action} ON {table} BEGIN
    UPDATE group_table SET updated = datetime('now') WHERE name = {name};
END;"""

_AP_TEMPLATE = """CREATE TRIGGER IF NOT EXISTS ap_crossdatabase_{action} BEFORE {action} ON activityparameter BEGIN
    SELECT CASE WHEN
        ((SELECT COUNT(*) FROM activityparameter WHERE "group" = NEW."group") > 0)
    AND (NEW.database NOT IN (SELECT DISTINCT "database" FROM activityparameter where "group" = NEW."group"))
    THEN RAISE(ABORT,'Cross database group')
    END;
END;"""
AP_INSERT_TRIGGER = _AP_TEMPLATE.format(action="INSERT")
AP_UPDATE_TRIGGER = _AP_TEMPLATE.format(action="UPDATE")

_CLOSURE_TEMPLATE = """CREATE TRIGGER IF NOT EXISTS gd_circular_{action} BEFORE {action} ON groupdependency BEGIN
    SELECT CASE WHEN EXISTS (SELECT * FROM groupdependency AS g WHERE g."group" = NEW.depends AND g.depends = NEW."group")
    THEN RAISE(ABORT,'Circular dependency')
    END;
END;
"""
GD_INSERT_TRIGGER = _CLOSURE_TEMPLATE.format(action="INSERT")
GD_UPDATE_TRIGGER = _CLOSURE_TEMPLATE.format(action="UPDATE")


@python_2_unicode_compatible
class ParameterBase(Model):
    __repr__ = lambda x: str(x)

    def __lt__(self, other):
        if type(self) != type(other):
            raise TypeError
        else:
            return self.name.lower() < other.name.lower()

    @classmethod
    def create_table(cls, fail_silently=False):
        super(ParameterBase, cls).create_table(fail_silently)
        cls._meta.database.execute_sql(
            TRIGGER.format(
                action="INSERT",
                name=cls._new_name,
                table=cls._db_table
        ))
        for action in ("UPDATE", "DELETE"):
            cls._meta.database.execute_sql(
                TRIGGER.format(
                    action=action,
                    name=cls._old_name,
                    table=cls._db_table
            ))


class ProjectParameter(ParameterBase):
    name = TextField(index=True, unique=True)
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    _old_name = "'project'"
    _new_name = "'project'"
    _db_table = "projectparameter"

    def __str__(self):
        return "Project parameter: {}".format(self.name)

    def save(self, *args, **kwargs):
        Group.get_or_create(name='project')[0].expire()
        super(ProjectParameter, self).save(*args, **kwargs)

    @staticmethod
    def load():
        def reformat(o):
            o = o.dict
            return (o.pop("name"), o)
        return dict([reformat(o) for o in ProjectParameter.select()])

    @staticmethod
    def static(only=None):
        result = dict(ProjectParameter.select(
            ProjectParameter.name,
            ProjectParameter.amount
        ).tuples())
        if only is not None:
            result = {k: v for k, v in result.items() if k in only}
        return result

    @staticmethod
    def expired():
        try:
            return not Group.get(name='project').fresh
        except Group.DoesNotExist:
            return False

    @staticmethod
    def recalculate():
        if not ProjectParameter.expired():
            return
        data = ProjectParameter.load()
        if not data:
            return
        ParameterSet(data).evaluate_and_set_amount_field()
        with parameters.db.atomic() as _:
            for key, value in data.items():
                ProjectParameter.update(
                    amount=value['amount'],
                ).where(ProjectParameter.name==key).execute()
            Group.get_or_create(name='project')[0].freshen()
            Group.update(fresh=False).where(
                Group.name << GroupDependency.select(
                    GroupDependency.group
                ).where(GroupDependency.depends=='project')
            ).execute()

    @property
    def dict(self):
        obj = nonempty({
            'name': self.name,
            'formula': self.formula,
            'amount': self.amount,
        })
        obj.update(self.data)
        return obj


class DatabaseParameter(ParameterBase):
    database = TextField(index=True)
    name = TextField(index=True)
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    _old_name = "OLD.database"
    _new_name = "NEW.database"
    _db_table = "databaseparameter"

    class Meta:
        indexes = (
            (('database', 'name'), True),
        )
        constraints = [Check("database != 'project'")]

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
    def expired(database):
        try:
            return not Group.get(name=database).fresh
        except Group.DoesNotExist:
            return False

    @staticmethod
    def static(database, only=None):
        result = dict(DatabaseParameter.select(
            DatabaseParameter.name,
            DatabaseParameter.amount
        ).where(DatabaseParameter.database==database).tuples())
        if only is not None:
            result = {k: v for k, v in result.items() if k in only}
        return result

    @staticmethod
    def recalculate(database):
        if ProjectParameter.expired():
            ProjectParameter.recalculate()

        # Can we avoid doing anything?
        if not DatabaseParameter.expired(database):
            return
        data = DatabaseParameter.load(database)
        if not data:
            return

        # Parse all formulas, find missing variables
        new_symbols = get_new_symbols(data.values(), set(data))
        found_symbols = {x[0] for x in ProjectParameter.select(
            ProjectParameter.name).tuples()}
        missing = new_symbols.difference(found_symbols)
        if missing:
            raise MissingName("The following variables aren't defined:\n{}".format("|".join(missing)))

        # Add or delete `project` dependency if needed
        if new_symbols:
            GroupDependency.get_or_create(
                group=database,
                depends="project"
            )
            # Load needed project variables as {'foo': 42} dict
            glo = ProjectParameter.static(new_symbols)
        else:
            GroupDependency.delete().where(
                GroupDependency.group==database,
                GroupDependency.depends=="project"
            ).execute()
            glo = None

        # Update database parameter values
        ParameterSet(data, glo).evaluate_and_set_amount_field()
        with parameters.db.atomic():
            for key, value in data.items():
                DatabaseParameter.update(
                    amount=value['amount'],
                ).where(
                    DatabaseParameter.name==key,
                    DatabaseParameter.database==database,
                ).execute()
            Group.get(name=database).freshen()
            # Expire any activity parameters that depend on this group
            Group.update(fresh=False).where(
                Group.name << GroupDependency.select(
                    GroupDependency.group
                ).where(GroupDependency.depends==database)
            ).execute()

    def save(self, *args, **kwargs):
        Group.get_or_create(name=self.database)[0].expire()
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


class ActivityParameter(ParameterBase):
    group = TextField()
    database = TextField()
    code = TextField()
    name = TextField()
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    _old_name = 'OLD."group"'
    _new_name = 'NEW."group"'
    _db_table = "activityparameter"

    class Meta:
        indexes = (
            (('name', 'database', 'code'), True),
            (('group', 'name'), True),
        )
        constraints = [Check("""("group" != 'project') AND ("group" != database)""")]

    def __str__(self):
        return "Activity parameter: {}:{}".format(self.group, self.name)

    @staticmethod
    def load(group):
        """Load all parameter dictionaries for this group ({name: data})."""
        def reformat(o):
            o = o.dict
            return (o.pop("name"), o)
        return dict([reformat(o) for o in ActivityParameter.select().where(
            ActivityParameter.group == group)])

    @staticmethod
    def static(group):
        result = dict(ActivityParameter.select(
            ActivityParameter.name,
            ActivityParameter.amount
        ).where(ActivityParameter.group==group).tuples())
        if only is not None:
            result = {k: v for k, v in result.items() if k in only}
        return result

    @staticmethod
    def expired(group):
        try:
            return not Group.get(name=group).fresh
        except Group.DoesNotExist:
            return False

    @staticmethod
    def recalculate_group(group):
        """Recalculate all values for activity parameters in this group, and update their underlying `Activity` and `Exchange` values."""
        # Start by traversing and updating the list of dependencies
        if not ActivityParameter.expired(group):
            return
        data = ActivityParameter.load(group)
        if not data:
            return

        database = ActivityParameter.get(group=group).database

        # Parse all formulas, find missing variables
        needed = get_new_symbols(data.values(), set(data))
        found = defaultdict(set)
        depends = []

        # Iteratively search through other activity params,
        # then db params, then project params
        for new_group in Group.get(name=group).order:
            for name in ActivityParameter.static(new_group, only=needed):
                found[new_group].add(name)
                needed.remove(name)
            if new_group in found:
                depends.append(new_group)

        if needed:
            for name in DatabaseParameter.static(database, only=needed):
                found[database].add(name)
                needed.remove(name)
            if database in found:
                depends.append(database)
        if needed:
            for name in ProjectParameter.static(only=needed):
                found['project'].add(name)
                needed.remove(name)
            if 'project' in found:
                depends.append('project')
        if needed:
            raise MissingName("The following variables aren't defined:\n{}".format("|".join(needed)))

        # Reset
        obj = Group.get(name=group).order

        found_symbols = {x[0] for x in ProjectParameter.select(
            ProjectParameter.name).tuples()}
        missing = new_symbols.difference(found_symbols)
        if missing:
            raise ValueError("The following variables aren't defined:\t{}".format(";".join(missing)))

        # Add or delete `project` dependency if needed
        if new_symbols:
            GroupDependency.get_or_create(
                group=database,
                depends="project"
            )
            # Load needed project variables as {'foo': 42} dict
            glo = ProjectParameter.static(new_symbols)
        else:
            GroupDependency.delete().where(
                GroupDependency.group==database,
                GroupDependency.depends=="project"
            ).execute()
            glo = None

        # Update database parameter values
        ParameterSet(data, glo).evaluate_and_set_amount_field()
        with parameters.db.atomic():
            for key, value in data.items():
                DatabaseParameter.update(
                    amount=value['amount'],
                ).where(
                    DatabaseParameter.name==key,
                    DatabaseParameter.database==database,
                ).execute()
            Group.get(name=database).freshen()
            # Expire any activity parameters that depend on this group
            Group.update(fresh=False).where(
                Group.name << GroupDependency.select(
                    GroupDependency.group
                ).where(GroupDependency.depends==database)
            ).execute()

    def save(self, *args, **kwargs):
        Group.get_or_create(name=self.group)[0].expire()
        super(ActivityParameter, self).save(*args, **kwargs)

    @classmethod
    def create_table(cls, fail_silently=False):
        super(ActivityParameter, cls).create_table(fail_silently)
        cls._meta.database.execute_sql(AP_UPDATE_TRIGGER)
        cls._meta.database.execute_sql(AP_INSERT_TRIGGER)

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
class Group(Model):
    name = TextField(unique=True)
    fresh = BooleanField(default=True)
    updated = DateTimeField(default=datetime.datetime.now)
    order = PickleField(default=[])

    def expire(self):
        self.fresh = False
        self.save()

    def freshen(self):
        self.fresh = True
        self.save()

    def save(self, *args, **kwargs):
        self.purge_order()
        super(Group, self).save(*args, **kwargs)

    def purge_order(self):
        reserved = set(databases).union(set(['project']))
        self.order = [x for x in self.order if x not in reserved]

    class Meta:
        db_table = "group_table"


@python_2_unicode_compatible
class GroupDependency(Model):
    group = TextField()
    depends = TextField()

    class Meta:
        indexes = (
            (('group', 'depends'), True),
        )
        constraints = [Check('"group" != depends')]

    def save(self, *args, **kwargs):
        if self.group == 'project':
            raise ValueError("`project` group can't have dependencies")
        elif self.group in databases and self.depends != 'project':
            raise ValueError("Database groups can only depend on `project`")
        super(GroupDependency, self).save(*args, **kwargs)

    @classmethod
    def create_table(cls, fail_silently=False):
        super(GroupDependency, cls).create_table(fail_silently)
        cls._meta.database.execute_sql(GD_UPDATE_TRIGGER)
        cls._meta.database.execute_sql(GD_INSERT_TRIGGER)


class ParameterManager(object):
    def __init__(self):
        self.db = create_database(
            os.path.join(projects.dir, "parameters.db"),
            [DatabaseParameter, ProjectParameter, ActivityParameter,
             Group, GroupDependency]
        )
        config.sqlite3_databases.append((
            "parameters.db",
            self.db,
            [DatabaseParameter, ProjectParameter, ActivityParameter,
             Group, GroupDependency]
        ))

    def parameters_for_group(self, group):
        if group == 'project':
            return ProjectParameter.select()
        elif group in databases:
            return DatabaseParameter.select().where(DatabaseParameter.database==group)
        else:
            return ActivityParameter.select().where(ActivityParameter.group==group)

    def add_to_group(self, group, activity):
        """Add `activity` to group.

        Deletes `parameters` from `Activity`."""
        Group.get_or_create(name=group)

        activity = get_activity((activity[0], activity[1]))
        if 'parameters' not in activity:
            return

        # Avoid duplicate by deleting existing parameters
        ActivityParameter.delete().where(
            ActivityParameter.database == activity['database'],
            ActivityParameter.code == activity['code']
        ).execute()

        def reformat(o):
            skipped = ('variable', 'amount', 'formula')
            return [nonempty({
                'group': group,
                'database': o['database'],
                'code': o['code'],
                'name': p['variable'],
                'formula': p.get('formula'),
                'amount': p.get('amount', 0),
                'data': {k: v for k, v in p.items() if k not in skipped}
            }) for p in o.get('parameters', [])]

        # Get formatted parameters
        with self.db.atomic():
            for row in reformat(activity):
                ActivityParameter.create(**row)

        # Parameters are now "active", remove from `Activity`
        del activity['parameters']
        activity.save()

        return ActivityParameter.select().where(
            ActivityParameter.database == activity['database'],
            ActivityParameter.code == activity['code']
        ).count()

    def remove_from_group(self, group, activity):
        raise NotImplementedError

    def new_project_parameters(self, data):
        """Efficiently and correctly enter multiple parameters.

        ``data`` should be a list of dictionaries."""
        names = {ds['name'] for ds in data}
        assert len(names) == len(data), "Nonunique names"

        def reformat(ds):
            return nonempty({
                'name': ds.pop('name'),
                'amount': ds.pop('amount'),
                'formula': ds.pop('formula', None),
                'data': ds
            })
        data = [reformat(ds) for ds in data]

        with self.db.atomic():
            # Remove existing values
            ProjectParameter.delete().execute()
            for idx in range(0, len(data), 100):
                ProjectParameter.insert_many(data[idx:idx+100]).execute()
            Group.get_or_create(name='project')[0].expire()
            ProjectParameter.recalculate()

    def new_database_parameters(self, data, database):
        """Efficiently and correctly enter multiple parameters.

        ``data`` should be a list of dictionaries. ``database`` should be an existing database."""
        assert database in databases, "Unknown database"

        names = {ds['name'] for ds in data}
        assert len(names) == len(data), "Nonunique names"

        def reformat(ds):
            return nonempty({
                'database': database,
                'name': ds.pop('name'),
                'amount': ds.pop('amount'),
                'formula': ds.pop('formula', None),
                'data': ds
            })
        data = [reformat(ds) for ds in data]

        with self.db.atomic():
            # Remove existing values
            DatabaseParameter.delete().where(
                DatabaseParameter.database==database
            ).execute()
            for idx in range(0, len(data), 100):
                DatabaseParameter.insert_many(data[idx:idx+100]).execute()
            # Just evaluated, so group is fresh
            Group.get_or_create(name=database)[0].expire()
            DatabaseParameter.recalculate(database)

    def recalculate(self):
        if ProjectParameter.expired():
            ProjectParameter.recalculate()
        for db in databases:
            if DatabaseParameter.expired(db):
                DatabaseParameter.recalculate(db)
        for obj in Group.select().where(
                Group.fresh==False):
            # Shouldn't be possible? Maybe concurrent access?
            if obj.name in databases or obj.name == 'project':
                continue
            ActivityParameter.recalculate_group(obj.name)

    def __len__(self):
        return (DatabaseParameter.select().count() + ProjectParameter.select().count() +
            ActivityParameter.select().count())

    def __repr__(self):
        return "Parameters manager with {} objects".format(len(self))


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


# Add ParameterizedExchange table with references to exchanges
#   Populate when `add_activity`
