from . import databases, projects, config, get_activity
from .backends.schema import ExchangeDataset
from .sqlite import PickleField, SubstitutableDatabase
from asteval import Interpreter
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
import datetime
import itertools
import re
import uuid


# https://stackoverflow.com/questions/34544784/arbitrary-string-to-valid-python-name
clean = lambda x: re.sub(r"\W|^(?=\d)", "_", x)
nonempty = lambda dct: {k: v for k, v in dct.items() if v is not None}

"""Autoupdate `updated` field in Group when parameters change"""
AUTOUPDATE_TRIGGER = """CREATE TRIGGER IF NOT EXISTS {table}_{action}_trigger AFTER {action} ON {table} BEGIN
    UPDATE group_table SET updated = datetime('now') WHERE name = {name};
END;"""

"""Activity parameter groups can't cross databases"""
_CROSSDATABASE_TEMPLATE = """CREATE TRIGGER IF NOT EXISTS ap_crossdatabase_{action} BEFORE {action} ON activityparameter BEGIN
    SELECT CASE WHEN
        ((SELECT COUNT(*) FROM activityparameter WHERE "group" = NEW."group") > 0)
    AND (NEW.database NOT IN (SELECT DISTINCT "database" FROM activityparameter where "group" = NEW."group"))
    THEN RAISE(ABORT,'Cross database group')
    END;
END;"""
CROSSDATASE_INSERT_TRIGGER = _CROSSDATABASE_TEMPLATE.format(action="INSERT")
CROSSDATASE_UPDATE_TRIGGER = _CROSSDATABASE_TEMPLATE.format(action="UPDATE")

"""Activities can't be in multiple activity parameter groups"""
_CROSSGROUP_TEMPLATE = """CREATE TRIGGER IF NOT EXISTS ap_crossgroup_{action} BEFORE {action} ON activityparameter BEGIN
    SELECT CASE WHEN EXISTS (SELECT * FROM activityparameter AS a WHERE
            a.database = NEW.database AND
            a.code = NEW.code AND
            a."group" != NEW."group")
    THEN RAISE(ABORT,'Cross group activity')
    END;
END;"""
CROSSGROUP_INSERT_TRIGGER = _CROSSGROUP_TEMPLATE.format(action="INSERT")
CROSSGROUP_UPDATE_TRIGGER = _CROSSGROUP_TEMPLATE.format(action="UPDATE")

"""No circular dependences in activity parameter group dependencies"""
_CLOSURE_TEMPLATE = """CREATE TRIGGER IF NOT EXISTS gd_circular_{action} BEFORE {action} ON groupdependency BEGIN
    SELECT CASE WHEN EXISTS (SELECT * FROM groupdependency AS g WHERE g."group" = NEW.depends AND g.depends = NEW."group")
    THEN RAISE(ABORT,'Circular dependency')
    END;
END;
"""
GD_INSERT_TRIGGER = _CLOSURE_TEMPLATE.format(action="INSERT")
GD_UPDATE_TRIGGER = _CLOSURE_TEMPLATE.format(action="UPDATE")

"""Parameterized exchange groups must be in activityparameters table"""
_PE_GROUP_TEMPLATE = """CREATE TRIGGER IF NOT EXISTS pe_group_{action} BEFORE {action} ON parameterizedexchange BEGIN
    SELECT CASE WHEN
        ((SELECT COUNT(*) FROM activityparameter WHERE "group" = NEW."group") < 1)
    THEN RAISE(ABORT,'Missing activity parameter group')
    END;
END;
"""
PE_INSERT_TRIGGER = _PE_GROUP_TEMPLATE.format(action="INSERT")
PE_UPDATE_TRIGGER = _PE_GROUP_TEMPLATE.format(action="UPDATE")


class ParameterBase(Model):
    __repr__ = lambda x: str(x)

    def __lt__(self, other):
        if type(self) != type(other):
            raise TypeError
        else:
            return self.name.lower() < other.name.lower()

    @classmethod
    def create_table(cls):
        super(ParameterBase, cls).create_table()
        cls._meta.database.execute_sql(
            AUTOUPDATE_TRIGGER.format(
                action="INSERT", name=cls._new_name, table=cls._db_table
            )
        )
        for action in ("UPDATE", "DELETE"):
            cls._meta.database.execute_sql(
                AUTOUPDATE_TRIGGER.format(
                    action=action, name=cls._old_name, table=cls._db_table
                )
            )

    @staticmethod
    def expire_downstream(group):
        """Expire any activity parameters that depend on this group"""
        Group.update(fresh=False).where(
            Group.name
            << GroupDependency.select(GroupDependency.group).where(
                GroupDependency.depends == group
            )
        ).execute()


class ProjectParameter(ParameterBase):
    """Parameter set for a project. Group name is 'project'.

    Columns:

        * name: str, unique
        * formula: str, optional
        * amount: float, optional
        * data: object, optional. Used for any other metadata.

    Note that there is no magic for reading and writing to ``data`` (unlike ``Activity`` objects) - it must be used directly.

    """

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
        Group.get_or_create(name="project")[0].expire()
        super(ProjectParameter, self).save(*args, **kwargs)

    @staticmethod
    def load(group=None):
        """Return dictionary of parameter data with names as keys and ``.dict()`` as values."""

        def reformat(o):
            o = o.dict
            return (o.pop("name"), o)

        return dict([reformat(o) for o in ProjectParameter.select()])

    @staticmethod
    def static(ignored="project", only=None):
        """Get dictionary of ``{name: amount}`` for all project parameters.

        ``only`` restricts returned names to ones found in ``only``. ``ignored`` included for API compatibility with other ``recalculate`` methods."""
        result = dict(
            ProjectParameter.select(
                ProjectParameter.name, ProjectParameter.amount
            ).tuples()
        )
        if only is not None:
            result = {k: v for k, v in result.items() if k in only}
        return result

    @staticmethod
    def expired():
        """Return boolean - is this group expired?"""
        try:
            return not Group.get(name="project").fresh
        except Group.DoesNotExist:
            return False

    @staticmethod
    def recalculate(ignored=None):
        """Recalculate all parameters.

        ``ignored`` included for API compatibility with other ``recalculate`` methods - it will really be ignored."""
        if not ProjectParameter.expired():
            return
        data = ProjectParameter.load()
        if not data:
            return
        ParameterSet(data).evaluate_and_set_amount_field()
        with parameters.db.atomic() as _:
            for key, value in data.items():
                ProjectParameter.update(amount=value["amount"],).where(
                    ProjectParameter.name == key
                ).execute()
            Group.get_or_create(name="project")[0].freshen()
            ProjectParameter.expire_downstream("project")

    @staticmethod
    def dependency_chain():
        """ Determine if ```ProjectParameter`` parameters have dependencies
        within the group.

        Returns:

        .. code-block:: python

            [
                {
                    'kind': 'project',
                    'group': 'project',
                    'names': set of variables names
                }
            ]

        """
        data = ProjectParameter.load()
        if not data:
            return []

        # Parse all formulas, find missing variables
        needed = get_new_symbols(data.values())
        if not needed:
            return []

        missing = needed.difference(data)
        if missing:
            raise MissingName(
                "The following variables aren't defined:\n{}".format("|".join(missing))
            )

        return [{"kind": "project", "group": "project", "names": needed}]

    @staticmethod
    def is_dependency_within_group(name):
        own_group = next(iter(ProjectParameter.dependency_chain()), {})
        return True if name in own_group.get("names", set()) else False

    def is_deletable(self):
        """Perform a test to see if the current parameter can be deleted."""
        if ProjectParameter.is_dependency_within_group(self.name):
            return False
        # Test the database parameters
        if DatabaseParameter.is_dependent_on(self.name):
            return False
        # Test activity parameters
        if ActivityParameter.is_dependent_on(self.name, "project"):
            return False
        return True

    @classmethod
    def update_formula_parameter_name(cls, old, new):
        """ Performs an update of the formula of relevant parameters.

        NOTE: Make sure to wrap this in an .atomic() statement!
        """
        data = (
            alter_parameter_formula(p, old, new)
            for p in cls.select().where(cls.formula.contains(old))
        )
        cls.bulk_update(data, fields=[cls.formula], batch_size=50)
        Group.get_or_create(name="project")[0].expire()

    @property
    def dict(self):
        """Parameter data as a standardized dictionary"""
        obj = nonempty(
            {"name": self.name, "formula": self.formula, "amount": self.amount,}
        )
        obj.update(self.data)
        return obj


class DatabaseParameter(ParameterBase):
    """Parameter set for a database. Group name is the name of the database.

    Columns:

        * database: str
        * name: str, unique within a database
        * formula: str, optional
        * amount: float, optional
        * data: object, optional. Used for any other metadata.

    Note that there is no magic for reading and writing to ``data`` (unlike ``Activity`` objects) - it must be used directly.

    """

    database = TextField(index=True)
    name = TextField(index=True)
    formula = TextField(null=True)
    amount = FloatField(null=True)
    data = PickleField(default={})

    _old_name = "OLD.database"
    _new_name = "NEW.database"
    _db_table = "databaseparameter"

    class Meta:
        indexes = ((("database", "name"), True),)
        constraints = [Check("database != 'project'")]

    def __str__(self):
        return "Database parameter: {}:{}".format(self.database, self.name)

    @staticmethod
    def load(database):
        """Return dictionary of parameter data with names as keys and ``.dict()`` as values."""

        def reformat(o):
            o = o.dict
            return (o.pop("name"), o)

        return dict(
            [
                reformat(o)
                for o in DatabaseParameter.select().where(
                    DatabaseParameter.database == database
                )
            ]
        )

    @staticmethod
    def expired(database):
        """Return boolean - is this group expired?"""
        try:
            return not Group.get(name=database).fresh
        except Group.DoesNotExist:
            return False

    @staticmethod
    def static(database, only=None):
        """Return dictionary of {name: amount} for database group."""
        result = dict(
            DatabaseParameter.select(DatabaseParameter.name, DatabaseParameter.amount)
            .where(DatabaseParameter.database == database)
            .tuples()
        )
        if only is not None:
            result = {k: v for k, v in result.items() if k in only}
        return result

    @staticmethod
    def recalculate(database):
        """Recalculate all database parameters for ``database``, if expired."""
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
        found_symbols = {
            x[0] for x in ProjectParameter.select(ProjectParameter.name).tuples()
        }
        missing = new_symbols.difference(found_symbols)
        if missing:
            raise MissingName(
                "The following variables aren't defined:\n{}".format("|".join(missing))
            )

        # Add or delete `project` dependency if needed
        if new_symbols:
            GroupDependency.get_or_create(group=database, depends="project")
            # Load needed project variables as {'foo': 42} dict
            glo = ProjectParameter.static(only=new_symbols)
        else:
            GroupDependency.delete().where(
                GroupDependency.group == database, GroupDependency.depends == "project"
            ).execute()
            glo = None

        # Update database parameter values
        ParameterSet(data, glo).evaluate_and_set_amount_field()
        with parameters.db.atomic():
            for key, value in data.items():
                DatabaseParameter.update(amount=value["amount"],).where(
                    DatabaseParameter.name == key,
                    DatabaseParameter.database == database,
                ).execute()
            Group.get(name=database).freshen()
            DatabaseParameter.expire_downstream(database)

    @staticmethod
    def dependency_chain(group, include_self=False):
        """Find where each missing variable is defined in dependency chain.

        If ``include_self`` is True will include parameters within the group as possible dependencies

        Returns:

        .. code-block:: python

            [
                {
                    'kind': one of 'project', 'database', 'activity',
                    'group': group name,
                    'names': set of variables names
                }
            ]

        """
        data = DatabaseParameter.load(group)
        if not data:
            return []

        # Parse all formulas, find missing variables
        context = set(data) if not include_self else set()
        needed = get_new_symbols(data.values(), context=context)
        if not needed:
            return []

        names, chain = set(), []
        if include_self:
            included = needed.intersection(data)
            for name in included:
                names.add(name)
                needed.remove(name)
            if names:
                chain.append({"kind": "database", "group": group, "names": names})

        if needed:
            names = set()
            for name in ProjectParameter.static(only=needed):
                names.add(name)
                needed.remove(name)
            if names:
                chain.insert(0, {"kind": "project", "group": "project", "names": names})

        if needed:
            raise MissingName(
                "The following variables aren't defined:\n{}".format("|".join(needed))
            )

        return chain

    @staticmethod
    def is_dependency_within_group(name, database):
        own_group = next(
            (
                x
                for x in DatabaseParameter.dependency_chain(database, include_self=True)
                if x.get("group") == database
            ),
            {},
        )
        return True if name in own_group.get("names", set()) else False

    def save(self, *args, **kwargs):
        """Save this model instance"""
        Group.get_or_create(name=self.database)[0].expire()
        super(DatabaseParameter, self).save(*args, **kwargs)

    def is_deletable(self):
        """Perform a test to see if the current parameter can be deleted."""
        # Test if the current parameter is used by other database parameters
        if DatabaseParameter.is_dependency_within_group(self.name, self.database):
            return False
        # Then test all relevant activity parameters
        if ActivityParameter.is_dependent_on(self.name, self.database):
            return False
        return True

    @staticmethod
    def is_dependent_on(name):
        """ Test if any database parameters are dependent on the given
        project parameter name.
        """
        query = (
            GroupDependency.select(GroupDependency.group)
            .where(GroupDependency.depends == "project")
            .distinct()
        )

        for row in query.execute():
            chain = DatabaseParameter.dependency_chain(row.group)
            own_group = next((x for x in chain if x.get("group") == "project"), {})
            if name in own_group.get("names", set()):
                return True

        return False

    @classmethod
    def update_formula_project_parameter_name(cls, old, new):
        """ Performs an update of the formula of relevant parameters.

        This method specifically targets project parameters used in database
        formulas
        """
        data = (
            alter_parameter_formula(p, old, new)
            for p in (
                cls.select()
                .join(GroupDependency, on=(GroupDependency.group == cls.database))
                .where(cls.formula.contains(old))
            )
            if not DatabaseParameter.is_dependency_within_group(old, p.database)
        )
        dbs = set(
            p.database
            for p in (
                cls.select(cls.database)
                .join(GroupDependency, on=(GroupDependency.group == cls.database))
                .where(cls.formula.contains(old))
                .distinct()
            )
            if not DatabaseParameter.is_dependency_within_group(old, p.database)
        )
        cls.bulk_update(data, fields=[cls.formula], batch_size=50)
        for db in dbs:
            Group.get_or_create(name=db)[0].expire()

    @classmethod
    def update_formula_database_parameter_name(cls, old, new):
        """ Performs an update of the formula of relevant parameters.

        This method specifically targets database parameters used in database
        formulas
        """
        data = (
            alter_parameter_formula(p, old, new)
            for p in cls.select().where(cls.formula.contains(old))
            if DatabaseParameter.is_dependency_within_group(old, p.database)
        )
        dbs = set(
            p.database
            for p in (
                cls.select(cls.database).where(cls.formula.contains(old)).distinct()
            )
            if DatabaseParameter.is_dependency_within_group(old, p.database)
        )
        cls.bulk_update(data, fields=[cls.formula], batch_size=50)
        for db in dbs:
            Group.get_or_create(name=db)[0].expire()

    @property
    def dict(self):
        """Parameter data as a standardized dictionary"""
        obj = nonempty(
            {
                "database": self.database,
                "name": self.name,
                "formula": self.formula,
                "amount": self.amount,
            }
        )
        obj.update(self.data)
        return obj


class ActivityParameter(ParameterBase):
    """Parameter set for a group of activities.

    Columns:

        * group: str
        * database: str
        * code: str. Code and database define the linked activity for this parameter.
        * name: str, unique within a group
        * formula: str, optional
        * amount: float, optional
        * data: object, optional. Used for any other metadata.

    Activities can only have parameters in one group. Group names cannot be 'project' or the name of any existing database.

    Activity parameter groups can depend on other activity parameter groups, so that a formula in group "a" can depend on a variable in group "b". This dependency information is stored in ``Group.order`` - in our small example, we could define the following:

    .. code-block:: python

        a = Group.get(name="a")
        a.order = ["b", "c"]
        a.save()

    In this case, a variable not found in "a" would be searched for in "b" and then "c", in that order. Database and then project parameters are also implicitly included at the end of ``Group.order``.

    Note that there is no magic for reading and writing to ``data`` (unlike ``Activity`` objects) - it must be used directly.

    """

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
        indexes = [(("group", "name"), True)]
        constraints = [Check("""("group" != 'project') AND ("group" != database)""")]

    def __str__(self):
        return "Activity parameter: {}:{}".format(self.group, self.name)

    @staticmethod
    def load(group):
        """Return dictionary of parameter data with names as keys and ``.dict()`` as values."""

        def reformat(o):
            o = o.dict
            return (o.pop("name"), o)

        return dict(
            [
                reformat(o)
                for o in ActivityParameter.select().where(
                    ActivityParameter.group == group
                )
            ]
        )

    @staticmethod
    def static(group, only=None, full=False):
        """Get dictionary of ``{name: amount}`` for parameters defined in ``group``.

        ``only`` restricts returned names to ones found in ``only``. ``full`` returns all names, including those found in the dependency chain."""
        result = dict(
            ActivityParameter.select(ActivityParameter.name, ActivityParameter.amount)
            .where(ActivityParameter.group == group)
            .tuples()
        )
        if full:
            temp = ActivityParameter._static_dependencies(group)
            temp.update(result)
            result = temp
        if only is not None:
            result = {k: v for k, v in result.items() if k in only}
        return result

    @staticmethod
    def _static_dependencies(group):
        """Get dictionary of ``{name: amount}`` for all variables defined in dependency chain.

        Be careful! This could have variables which overlap with local variable names. Designed for internal use."""
        database = ActivityParameter.get(group=group).database

        chain = [ProjectParameter.static(), DatabaseParameter.static(database)] + [
            ActivityParameter.static(g) for g in Group.get(name=group).order[::-1]
        ]

        result = {}
        for dct in chain:
            result.update(dct)
        return result

    @staticmethod
    def insert_dummy(group, activity):
        code, database = activity[1], activity[0]
        if (
            not ActivityParameter.select()
            .where(
                ActivityParameter.group == group,
                ActivityParameter.code == code,
                ActivityParameter.database == database,
            )
            .count()
        ):
            ActivityParameter.create(
                group=group,
                name="__dummy_{}__".format(uuid.uuid4().hex),
                code=code,
                database=database,
                amount=0,
            )

    @staticmethod
    def expired(group):
        """Return boolean - is this group expired?"""
        try:
            return not Group.get(name=group).fresh
        except Group.DoesNotExist:
            return False

    @staticmethod
    def dependency_chain(group, include_self=False):
        """Find where each missing variable is defined in dependency chain.

        Will also load in all parameters needed to resolve the ``ParameterizedExchanges`` for this group.

        If ``include_self`` is True will include parameters within the group as possible dependencies

        Returns:

        .. code-block:: python

            [
                {
                    'kind': one of 'project', 'database', 'activity',
                    'group': group name,
                    'names': set of variables names
                }
            ]

        """
        data = ActivityParameter.load(group)
        if not data:
            return []

        # Parse all formulas, find missing variables
        context = set(data) if not include_self else None
        activity_needed = get_new_symbols(data.values(), context=context)
        exchanges_needed = get_new_symbols(
            ParameterizedExchange.load(group).values(), context=context
        )
        needed = activity_needed.union(exchanges_needed)

        if not needed:
            return []

        chain = []

        # Iteratively search through other activity params,
        # then db params, then project params
        for new_group in Group.get(name=group).order:
            names = set()
            for name in ActivityParameter.static(new_group, only=needed):
                names.add(name)
                needed.remove(name)
            if names:
                chain.append({"kind": "activity", "group": new_group, "names": names})

        if needed and include_self:
            names = set()
            included = needed.intersection(data)
            for name in included:
                names.add(name)
                needed.remove(name)
            if names:
                chain.append({"kind": "activity", "group": group, "names": names})

        if needed:
            database = ActivityParameter.get(group=group).database
            names = set()
            for name in DatabaseParameter.static(database, only=needed):
                names.add(name)
                needed.remove(name)
            if names:
                chain.append({"kind": "database", "group": database, "names": names})
        if needed:
            names = set()
            for name in ProjectParameter.static(only=needed):
                names.add(name)
                needed.remove(name)
            if names:
                chain.append({"kind": "project", "group": "project", "names": names})
        if needed:
            raise MissingName(
                "The following variables aren't defined:\n{}".format("|".join(needed))
            )

        return chain

    @staticmethod
    def is_dependency_within_group(name, group, include_order=False):
        """ Determine if the given parameter `name` is a dependency within
        the given activity `group`.

        The optional ``include_order`` parameter will include dependencies
        from groups found in the the ``Group``.`order` field.
        """
        chain = ActivityParameter.dependency_chain(group, include_self=True)
        own_group = next((x for x in chain if x.get("group") == group), {})
        names = own_group.get("names", set())
        if include_order:
            for new_group in Group.get(name=group).order:
                order_group = next(
                    (x for x in chain if x.get("group") == new_group), {}
                )
                names = names.union(order_group.get("names", set()))
        return True if name in names else False

    @staticmethod
    def recalculate(group):
        """Recalculate all values for activity parameters in this group, and update their underlying `Activity` and `Exchange` values."""
        # Start by traversing and updating the list of dependencies
        if not ActivityParameter.expired(group):
            return

        chain = ActivityParameter.dependency_chain(group)

        # Reset dependencies and dependency order
        if chain:
            obj = Group.get(name=group)
            obj.order = [o["group"] for o in chain if o["kind"] == "activity"]
            obj.save()
            GroupDependency.delete().where(GroupDependency.group == group).execute()
            GroupDependency.insert_many(
                [{"group": group, "depends": o["group"]} for o in chain]
            ).execute()

        # Update all upstream groups
        mapping = {
            "project": ProjectParameter,
            "database": DatabaseParameter,
            "activity": ActivityParameter,
        }

        # Not guaranteed to be the most efficient,
        # but definitely simplest for now.
        # Could be smarter here in the future
        # Shouldn't be any race conditions because check for
        # circular dependencies
        for row in chain[::-1]:
            mapping[row["kind"]].recalculate(row["group"])

        # Update activity parameter values
        data = ActivityParameter.load(group)
        static = {
            k: v
            for k, v in ActivityParameter._static_dependencies(group).items()
            if k not in data
        }
        ParameterSet(data, static).evaluate_and_set_amount_field()
        with parameters.db.atomic():
            for key, value in data.items():
                ActivityParameter.update(amount=value["amount"],).where(
                    ActivityParameter.name == key, ActivityParameter.group == group,
                ).execute()
            Group.get(name=group).freshen()
            ActivityParameter.expire_downstream(group)

        ActivityParameter.recalculate_exchanges(group)

    @staticmethod
    def recalculate_exchanges(group):
        """Recalculate formulas for all parameterized exchanges in group ``group``."""
        if ActivityParameter.expired(group):
            return ActivityParameter.recalculate(group)

        interpreter = Interpreter()
        for k, v in ActivityParameter.static(group, full=True).items():
            interpreter.symtable[k] = v
        # TODO: Remove uncertainty from exchanges?
        for obj in ParameterizedExchange.select().where(
            ParameterizedExchange.group == group
        ):
            exc = ExchangeDataset.get(id=obj.exchange)
            exc.data["amount"] = interpreter(obj.formula)
            exc.save()

        databases.set_dirty(ActivityParameter.get(group=group).database)

    def save(self, *args, **kwargs):
        """Save this model instance"""
        Group.get_or_create(name=self.group)[0].expire()
        super(ActivityParameter, self).save(*args, **kwargs)

    def is_deletable(self):
        """Perform a test to see if the current parameter can be deleted."""
        # First check own group
        if ActivityParameter.is_dependency_within_group(self.name, self.group):
            return False
        # Then test other relevant activity groups.
        if ActivityParameter.is_dependent_on(self.name, self.group):
            return False
        return True

    @staticmethod
    def is_dependent_on(name, group):
        """ Test if any activity parameters are dependent on the given
        parameter name from the given group.
        """
        query = (
            GroupDependency.select(GroupDependency.group)
            .where(GroupDependency.depends == group)
            .distinct()
        )

        for row in query.execute():
            chain = ActivityParameter.dependency_chain(row.group)
            own_group = next((x for x in chain if x.get("group") == group), {})
            if name in own_group.get("names", set()):
                return True

        return False

    @classmethod
    def update_formula_project_parameter_name(cls, old, new):
        """ Performs an update of the formula of relevant parameters.

        This method specifically targets project parameters used in activity
        formulas
        """
        data = (
            alter_parameter_formula(p, old, new)
            for p in (
                cls.select()
                .join(GroupDependency, on=(GroupDependency.group == cls.group))
                .where(
                    (GroupDependency.depends == "project") & (cls.formula.contains(old))
                )
            )
            if not ActivityParameter.is_dependency_within_group(old, p.group)
        )
        group_parameters = itertools.chain(
            (
                cls.select(cls.group)
                .join(GroupDependency, on=(GroupDependency.group == cls.group))
                .where(
                    (GroupDependency.depends == "project") & (cls.formula.contains(old))
                )
                .distinct()
            ),
            (
                ParameterizedExchange.select(ParameterizedExchange.group)
                .where(ParameterizedExchange.formula.contains(old))
                .distinct()
            ),
        )
        groups = set(
            p.group
            for p in group_parameters
            if not ActivityParameter.is_dependency_within_group(old, p.group)
        )
        exchanges = (
            alter_parameter_formula(p, old, new)
            for p in ParameterizedExchange.select().where(
                ParameterizedExchange.group << groups
            )
        )
        cls.bulk_update(data, fields=[cls.formula], batch_size=50)
        for param_exc in exchanges:
            param_exc.save()
        Group.update(fresh=False).where(Group.name << groups).execute()

    @classmethod
    def update_formula_database_parameter_name(cls, old, new):
        """ Performs an update of the formula of relevant parameters.

        This method specifically targets database parameters used in activity
        formulas
        """
        data = (
            alter_parameter_formula(p, old, new)
            for p in (
                cls.select()
                .join(GroupDependency, on=(GroupDependency.group == cls.group))
                .where(
                    (GroupDependency.depends == cls.database)
                    & (cls.formula.contains(old))
                )
            )
            if not ActivityParameter.is_dependency_within_group(old, p.group)
        )
        group_parameters = itertools.chain(
            (
                cls.select(cls.group)
                .join(GroupDependency, on=(GroupDependency.group == cls.group))
                .where(
                    (GroupDependency.depends == cls.database)
                    & (cls.formula.contains(old))
                )
                .distinct()
            ),
            (
                ParameterizedExchange.select(ParameterizedExchange.group)
                .where(ParameterizedExchange.formula.contains(old))
                .distinct()
            ),
        )
        groups = set(
            p.group
            for p in group_parameters
            if not ActivityParameter.is_dependency_within_group(old, p.group)
        )
        exchanges = (
            alter_parameter_formula(p, old, new)
            for p in ParameterizedExchange.select().where(
                ParameterizedExchange.group << groups
            )
        )
        cls.bulk_update(data, fields=[cls.formula], batch_size=50)
        for param_exc in exchanges:
            param_exc.save()
        Group.update(fresh=False).where(Group.name << groups).execute()

    @classmethod
    def update_formula_activity_parameter_name(cls, old, new, include_order=False):
        """ Performs an update of the formula of relevant parameters.

        This method specifically targets activity parameters used in activity
        formulas
        """
        data = (
            alter_parameter_formula(p, old, new)
            for p in cls.select().where(cls.formula.contains(old))
            if ActivityParameter.is_dependency_within_group(old, p.group, include_order)
        )
        group_parameters = itertools.chain(
            cls.select(cls.group).where(cls.formula.contains(old)).distinct(),
            (
                ParameterizedExchange.select(ParameterizedExchange.group)
                .where(ParameterizedExchange.formula.contains(old))
                .distinct()
            ),
        )
        groups = set(
            p.group
            for p in group_parameters
            if ActivityParameter.is_dependency_within_group(old, p.group, include_order)
        )
        exchanges = (
            alter_parameter_formula(p, old, new)
            for p in ParameterizedExchange.select().where(
                ParameterizedExchange.group << groups
            )
        )
        cls.bulk_update(data, fields=[cls.formula], batch_size=50)
        for param_exc in exchanges:
            param_exc.save()
        Group.update(fresh=False).where(Group.name << groups).execute()

    @classmethod
    def create_table(cls):
        super(ActivityParameter, cls).create_table()
        cls._meta.database.execute_sql(CROSSDATASE_UPDATE_TRIGGER)
        cls._meta.database.execute_sql(CROSSDATASE_INSERT_TRIGGER)
        cls._meta.database.execute_sql(CROSSGROUP_UPDATE_TRIGGER)
        cls._meta.database.execute_sql(CROSSGROUP_INSERT_TRIGGER)

    @property
    def dict(self):
        """Parameter data as a standardized dictionary"""
        obj = nonempty(
            {
                "database": self.database,
                "code": self.code,
                "name": self.name,
                "formula": self.formula,
                "amount": self.amount,
            }
        )
        obj.update(self.data)
        return obj


class ParameterizedExchange(Model):
    group = TextField()
    exchange = IntegerField(unique=True)
    formula = TextField()

    @classmethod
    def create_table(cls):
        super(ParameterizedExchange, cls).create_table()
        cls._meta.database.execute_sql(PE_UPDATE_TRIGGER)
        cls._meta.database.execute_sql(PE_INSERT_TRIGGER)

    def save(self, *args, **kwargs):
        Group.get_or_create(name=self.group)[0].expire()
        super(ParameterizedExchange, self).save(*args, **kwargs)
        # Push the changed formula to the Exchange.
        exc = ExchangeDataset.get_or_none(id=self.exchange)
        if exc and exc.data.get("formula") != self.formula:
            exc.data["formula"] = self.formula
            exc.save()

    @staticmethod
    def load(group):
        """Return dictionary of parameter data with names as keys and ``.dict()`` as values."""
        return {
            o.exchange: o.formula
            for o in ParameterizedExchange.select().where(
                ParameterizedExchange.group == group
            )
        }

    @staticmethod
    def recalculate(group):
        """Shortcut for ``ActivityParameter.recalculate_exchanges``."""
        return ActivityParameter.recalculate_exchanges(group)


class Group(Model):
    name = TextField(unique=True)
    fresh = BooleanField(default=True)
    updated = DateTimeField(default=datetime.datetime.now)
    order = PickleField(default=[])

    def expire(self):
        """Set ``fresh`` to ``False``"""
        self.fresh = False
        self.save()

    def freshen(self):
        """Set ``fresh`` to ``True``"""
        self.fresh = True
        self.save()

    def save(self, *args, **kwargs):
        """Save this model instance. Will remove 'project' and database names from ``order``."""
        self.purge_order()
        super(Group, self).save(*args, **kwargs)

    def purge_order(self):
        reserved = set(databases).union(set(["project"]))
        self.order = [x for x in self.order if x not in reserved]

    class Meta:
        table_name = "group_table"


class GroupDependency(Model):
    group = TextField()
    depends = TextField()

    class Meta:
        indexes = ((("group", "depends"), True),)
        constraints = [Check('"group" != depends')]

    def save(self, *args, **kwargs):
        if self.group == "project":
            raise ValueError("`project` group can't have dependencies")
        elif self.group in databases and self.depends != "project":
            raise ValueError("Database groups can only depend on `project`")
        super(GroupDependency, self).save(*args, **kwargs)

    @classmethod
    def create_table(cls):
        super(GroupDependency, cls).create_table()
        cls._meta.database.execute_sql(GD_UPDATE_TRIGGER)
        cls._meta.database.execute_sql(GD_INSERT_TRIGGER)


class ParameterManager(object):
    def __init__(self):
        self.db = SubstitutableDatabase(
            projects.dir / "parameters.db",
            [
                DatabaseParameter,
                ProjectParameter,
                ActivityParameter,
                ParameterizedExchange,
                Group,
                GroupDependency,
            ],
        )
        config.sqlite3_databases.append(("parameters.db", self.db))

    def add_to_group(self, group, activity):
        """Add `activity` to group.

        Creates ``group`` if needed.

        Will delete any existing ``ActivityParameter`` for this activity.

        Deletes `parameters` key from `Activity`."""
        Group.get_or_create(name=group)

        activity = get_activity((activity[0], activity[1]))
        if "parameters" not in activity:
            return

        # Avoid duplicate by deleting existing parameters
        ActivityParameter.delete().where(
            ActivityParameter.database == activity["database"],
            ActivityParameter.code == activity["code"],
        ).execute()

        def reformat(o):
            skipped = ("name", "amount", "formula")
            return [
                nonempty(
                    {
                        "group": group,
                        "database": o["database"],
                        "code": o["code"],
                        "name": p["name"],
                        "formula": p.get("formula"),
                        "amount": p.get("amount", 0),
                        "data": {k: v for k, v in p.items() if k not in skipped},
                    }
                )
                for p in o.get("parameters", [])
            ]

        # Get formatted parameters
        with self.db.atomic():
            for row in reformat(activity):
                ActivityParameter.create(**row)

        # Parameters are now "active", remove from `Activity`
        del activity["parameters"]
        activity.save()

        self.add_exchanges_to_group(group, activity)

        return (
            ActivityParameter.select()
            .where(
                ActivityParameter.database == activity["database"],
                ActivityParameter.code == activity["code"],
            )
            .count()
        )

    def remove_from_group(self, group, activity, restore_amounts=True):
        """Remove `activity` from `group`.

        Will delete any existing ``ActivityParameter`` and ``ParameterizedExchange`` for this activity.

        Restores `parameters` key to this `Activity`.
        By default, restores `amount` value of each parameterized exchange
        of the `Activity` to the original value. This can be avoided by using
        the ``restore_amounts`` parameter.

        """

        def drop_fields(dct):
            dct = {k: v for k, v in dct.items() if k not in ("database", "code")}
            return dct.pop("name"), dct

        activity = get_activity((activity[0], activity[1]))
        activity["parameters"] = dict(
            [
                drop_fields(o.dict)
                for o in ActivityParameter.select().where(
                    ActivityParameter.database == activity[0],
                    ActivityParameter.code == activity[1],
                )
            ]
        )

        with self.db.atomic():
            self.remove_exchanges_from_group(group, activity, restore_amounts)
            ActivityParameter.delete().where(
                ActivityParameter.database == activity[0],
                ActivityParameter.code == activity[1],
            ).execute()
            activity.save()

    def add_exchanges_to_group(self, group, activity):
        """ Add exchanges with formulas from ``activity`` to ``group``.

        Every exchange with a formula field will have its original `amount`
        value stored as `original_amount`. This original value can be
        restored when parameterization is removed from the activity with
        `remove_from_group`.

        """
        count = 0
        if (
            not ActivityParameter.select()
            .where(
                ActivityParameter.database == activity[0],
                ActivityParameter.code == activity[1],
            )
            .count()
        ):
            ActivityParameter.insert_dummy(group, activity)

        for exc in get_activity((activity[0], activity[1])).exchanges():
            if "formula" in exc:
                try:
                    obj = ParameterizedExchange.get(exchange=exc._document.id)
                except ParameterizedExchange.DoesNotExist:
                    obj = ParameterizedExchange(exchange=exc._document.id)
                obj.group = group
                obj.formula = exc["formula"]
                obj.save()
                if "original_amount" not in exc:
                    exc["original_amount"] = exc["amount"]
                    exc.save()
                count += 1

        return count

    def remove_exchanges_from_group(self, group, activity, restore_original=True):
        """ Takes a group and activity and removes all ``ParameterizedExchange``
        objects from the group.

        The ``restore_original`` parameter determines if the original amount
        values will be restored to those exchanges where a formula was used
        to alter the amount.

        """
        if restore_original:
            for exc in (ex for ex in activity.exchanges() if "original_amount" in ex):
                exc["amount"] = exc["original_amount"]
                del exc["original_amount"]
                exc.save()

        ParameterizedExchange.delete().where(
            ParameterizedExchange.group == group
        ).execute()

    def new_project_parameters(self, data, overwrite=True):
        """Efficiently and correctly enter multiple parameters.

        Will overwrite existing project parameters with the same name, unless ``overwrite`` is false, in which case a ``ValueError`` is raised.

        ``data`` should be a list of dictionaries:

        .. code-block:: python

            [{
                'name': name of variable (unique),
                'amount': numeric value of variable (optional),
                'formula': formula in Python as string (optional),
                optional keys like uncertainty, etc. (no limitations)
            }]

        """
        potentially_non_unique_names = [ds["name"] for ds in data]
        unique_names = list(set(potentially_non_unique_names))
        assert len(unique_names) == len(
            potentially_non_unique_names
        ), "Nonunique names: {}".format(
            [p for p in unique_names if potentially_non_unique_names.count(p) > 1]
        )

        def reformat(ds):
            return {
                "name": ds.pop("name"),
                "amount": ds.pop("amount", 0),
                "formula": ds.pop("formula", None),
                "data": ds,
            }

        data = [reformat(ds) for ds in data]
        new = {o["name"] for o in data}
        existing = {
            o[0] for o in ProjectParameter.select(ProjectParameter.name).tuples()
        }

        if new.intersection(existing) and not overwrite:
            raise ValueError(
                "The following parameters already exist:\n{}".format(
                    "|".join(new.intersection(existing))
                )
            )

        with self.db.atomic():
            # Remove existing values
            ProjectParameter.delete().where(
                ProjectParameter.name << tuple(new)
            ).execute()
            for idx in range(0, len(data), 100):
                ProjectParameter.insert_many(data[idx : idx + 100]).execute()
            Group.get_or_create(name="project")[0].expire()
            ProjectParameter.recalculate()

    def new_database_parameters(self, data, database, overwrite=True):
        """Efficiently and correctly enter multiple parameters. Deletes **all** existing database parameters for this database.

        Will overwrite existing database parameters with the same name, unless ``overwrite`` is false, in which case a ``ValueError`` is raised.

        ``database`` should be an existing database. ``data`` should be a list of dictionaries:

        .. code-block:: python

            [{
                'name': name of variable (unique),
                'amount': numeric value of variable (optional),
                'formula': formula in Python as string (optional),
                optional keys like uncertainty, etc. (no limitations)
            }]

        """
        assert database in databases, "Unknown database"

        potentially_non_unique_names = [ds["name"] for ds in data]
        unique_names = list(set(potentially_non_unique_names))
        assert len(unique_names) == len(
            potentially_non_unique_names
        ), "Nonunique names: {}".format(
            [p for p in unique_names if potentially_non_unique_names.count(p) > 1]
        )

        def reformat(ds):
            return {
                "database": database,
                "name": ds.pop("name"),
                "amount": ds.pop("amount", 0),
                "formula": ds.pop("formula", None),
                "data": ds,
            }

        data = [reformat(ds) for ds in data]
        new = {o["name"] for o in data}
        existing = {
            o[0]
            for o in DatabaseParameter.select(DatabaseParameter.name)
            .where(DatabaseParameter.database == database)
            .tuples()
        }

        if new.intersection(existing) and not overwrite:
            raise ValueError(
                "The following parameters already exist:\n{}".format(
                    "|".join(new.intersection(existing))
                )
            )

        with self.db.atomic():
            # Remove existing values
            DatabaseParameter.delete().where(
                DatabaseParameter.database == database,
                DatabaseParameter.name << tuple(new),
            ).execute()
            for idx in range(0, len(data), 100):
                DatabaseParameter.insert_many(data[idx : idx + 100]).execute()
            Group.get_or_create(name=database)[0].expire()
            DatabaseParameter.recalculate(database)

    def new_activity_parameters(self, data, group, overwrite=True):
        """Efficiently and correctly enter multiple parameters. Deletes **all** existing activity parameters for this group.

        Will overwrite existing parameters in the same group with the same name, unless ``overwrite`` is false, in which case a ``ValueError`` is raised.

        Input parameters must refer to a single, existing database.

        ``group`` is the group name; will be autocreated if necessary. ``data`` should be a list of dictionaries:

        .. code-block:: python

            [{
                'name': name of variable (unique),
                'database': activity database,
                'code': activity code,
                'amount': numeric value of variable (optional),
                'formula': formula in Python as string (optional),
                optional keys like uncertainty, etc. (no limitations)
            }]

        """
        database = {o["database"] for o in data}
        assert len(database) == 1, "Multiple databases"
        assert database.pop() in databases, "Unknown database"

        potentially_non_unique_names = [o["name"] for o in data]
        unique_names = list(set(potentially_non_unique_names))
        assert len(unique_names) == len(
            potentially_non_unique_names
        ), "Nonunique names: {}".format(
            [p for p in unique_names if potentially_non_unique_names.count(p) > 1]
        )

        Group.get_or_create(name=group)

        def reformat(ds):
            return {
                "group": group,
                "database": ds.pop("database"),
                "code": ds.pop("code"),
                "name": ds.pop("name"),
                "formula": ds.pop("formula", None),
                "amount": ds.pop("amount", 0),
                "data": ds,
            }

        data = [reformat(ds) for ds in data]
        new = {o["name"] for o in data}
        existing = {
            o[0]
            for o in ActivityParameter.select(ActivityParameter.name)
            .where(ActivityParameter.group == group)
            .tuples()
        }

        if new.intersection(existing) and not overwrite:
            raise ValueError(
                "The following parameters already exist:\n{}".format(
                    "|".join(new.intersection(existing))
                )
            )

        with self.db.atomic():
            # Remove existing values
            ActivityParameter.delete().where(
                ActivityParameter.group == group, ActivityParameter.name << new
            ).execute()
            for idx in range(0, len(data), 100):
                ActivityParameter.insert_many(data[idx : idx + 100]).execute()
            Group.get_or_create(name=group)[0].expire()
            ActivityParameter.recalculate(group)

    def rename_project_parameter(self, parameter, new_name, update_dependencies=False):
        """ Given a parameter and a new name, safely update the parameter.

        Will raise a TypeError if the given parameter is of the incorrect type.
        Will raise a ValueError if other parameters depend on the given one
        and ``update_dependencies`` is False.

        """
        if not isinstance(parameter, ProjectParameter):
            raise TypeError("Incorrect parameter type for this method.")
        if parameter.name == new_name:
            return

        project = ProjectParameter.is_dependency_within_group(parameter.name)
        database = DatabaseParameter.is_dependent_on(parameter.name)
        activity = ActivityParameter.is_dependent_on(parameter.name, "project")

        if not update_dependencies and any([project, database, activity]):
            raise ValueError(
                "Parameter '{}' is used in other (downstream) formulas".format(
                    parameter.name
                )
            )

        with self.db.atomic():
            if project:
                ProjectParameter.update_formula_parameter_name(parameter.name, new_name)
            if database:
                DatabaseParameter.update_formula_project_parameter_name(
                    parameter.name, new_name
                )
            if activity:
                ActivityParameter.update_formula_project_parameter_name(
                    parameter.name, new_name
                )
            parameter.name = new_name
            parameter.save()
            self.recalculate()

    def rename_database_parameter(self, parameter, new_name, update_dependencies=False):
        """ Given a parameter and a new name, safely update the parameter.

        Will raise a TypeError if the given parameter is of the incorrect type.
        Will raise a ValueError if other parameters depend on the given one
        and ``update_dependencies`` is False.

        """
        if not isinstance(parameter, DatabaseParameter):
            raise TypeError("Incorrect parameter type for this method.")
        if parameter.name == new_name:
            return

        database = DatabaseParameter.is_dependency_within_group(
            parameter.name, parameter.database
        )
        activity = ActivityParameter.is_dependent_on(parameter.name, parameter.database)

        if not update_dependencies and any([database, activity]):
            raise ValueError(
                "Parameter '{}' is used in other (downstream) formulas".format(
                    parameter.name
                )
            )

        with self.db.atomic():
            if database:
                DatabaseParameter.update_formula_database_parameter_name(
                    parameter.name, new_name
                )
            if activity:
                ActivityParameter.update_formula_database_parameter_name(
                    parameter.name, new_name
                )
            parameter.name = new_name
            parameter.save()
            self.recalculate()

    def rename_activity_parameter(self, parameter, new_name, update_dependencies=False):
        """ Given a parameter and a new name, safely update the parameter.

        Will raise a TypeError if the given parameter is of the incorrect type.
        Will raise a ValueError if other parameters depend on the given one
        and ``update_dependencies`` is False.

        """
        if not isinstance(parameter, ActivityParameter):
            raise TypeError("Incorrect parameter type for this method.")
        if parameter.name == new_name:
            return

        activity = any(
            [
                ActivityParameter.is_dependency_within_group(
                    parameter.name, parameter.group, include_order=True
                ),
                ActivityParameter.is_dependent_on(parameter.name, parameter.group),
            ]
        )

        if not update_dependencies and activity:
            raise ValueError(
                "Parameter '{}' is used in other (downstream) formulas".format(
                    parameter.name
                )
            )

        with self.db.atomic():
            if activity:
                ActivityParameter.update_formula_activity_parameter_name(
                    parameter.name, new_name, include_order=True
                )
            parameter.name = new_name
            parameter.save()
            self.recalculate()

    def recalculate(self):
        """Recalculate all expired project, database, and activity parameters, as well as exchanges."""
        if ProjectParameter.expired():
            ProjectParameter.recalculate()
        for db in databases:
            if DatabaseParameter.expired(db):
                DatabaseParameter.recalculate(db)
        for obj in Group.select().where(Group.fresh == False):
            # Shouldn't be possible? Maybe concurrent access?
            if obj.name in databases or obj.name == "project":
                continue
            ActivityParameter.recalculate(obj.name)
            ActivityParameter.recalculate_exchanges(obj.name)

    def __len__(self):
        return (
            DatabaseParameter.select().count()
            + ProjectParameter.select().count()
            + ActivityParameter.select().count()
        )

    def __repr__(self):
        return "Parameters manager with {} objects".format(len(self))


parameters = ParameterManager()


def get_new_symbols(data, context=None):
    interpreter = asteval.Interpreter()
    BUILTIN_SYMBOLS = set(interpreter.symtable).union(set(context or set()))
    found = set()
    for ds in data:
        if isinstance(ds, str):
            formula = ds
        elif "formula" in ds:
            formula = ds["formula"]
        else:
            continue

        nf = asteval.NameFinder()
        nf.generic_visit(interpreter.parse(formula))
        found.update(set(nf.names))
    return found.difference(BUILTIN_SYMBOLS)


def alter_parameter_formula(parameter, old, new):
    """ Replace the `old` part with `new` in the formula field and return
    the parameter itself.
    """
    if hasattr(parameter, "formula"):
        parameter.formula = re.sub(r"\b{}\b".format(old), new, parameter.formula)
    return parameter
