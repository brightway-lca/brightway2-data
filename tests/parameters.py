from bw2data.tests import bw2test
from bw2data import parameters, Database, get_activity
from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    Group,
    GroupDependency,
    ParameterizedExchange,
    parameters,
    ProjectParameter,
)
from bw2parameters.errors import MissingName
from peewee import IntegrityError
import pytest
import time
import re


# Regex to search for UUID: https://stackoverflow.com/a/18359032
uuid4hex = re.compile(
    r"[0-9a-f]{8}[0-9a-f]{4}4[0-9a-f]{3}[89ab][0-9a-f]{3}[0-9a-f]{12}", re.I
)

######################
### Project parameters
######################


@bw2test
def test_project_parameters():
    assert not len(parameters)
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    assert obj.name == "foo"
    assert obj.amount == 3.14
    assert obj.data == {"uncertainty type": 0}
    assert str(obj)
    assert isinstance(str(obj), str)


@bw2test
def test_project_parameter_autocreate_group():
    assert not Group.select().count()
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    assert Group.get(name="project")
    assert not Group.get(name="project").fresh


@bw2test
def test_expire_downstream():
    Group.create(fresh=True, name="A")
    Group.create(fresh=True, name="B")
    GroupDependency.create(group="B", depends="A")
    assert Group.get(name="A").fresh
    assert Group.get(name="B").fresh
    ProjectParameter.expire_downstream("A")
    assert not Group.get(name="B").fresh


@bw2test
def test_project_parameters_ordering():
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    with pytest.raises(TypeError):
        obj < 0
    assert not (obj < obj)
    another = ProjectParameter.create(name="bar", formula="2 * foo",)
    assert another < obj


@bw2test
def test_project_parameters_dict():
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    expected = {
        "name": "foo",
        "amount": 3.14,
        "uncertainty type": 0,
    }
    assert obj.dict == expected


@bw2test
def test_project_parameters_load():
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    another = ProjectParameter.create(name="bar", formula="2 * foo",)
    expected = {
        "foo": {"amount": 3.14, "uncertainty type": 0},
        "bar": {"formula": "2 * foo"},
    }
    assert ProjectParameter.load() == expected
    assert ProjectParameter.load("project") == expected
    assert ProjectParameter.load("foo") == expected


@bw2test
def test_project_parameters_static():
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    another = ProjectParameter.create(name="bar", formula="2 * foo",)
    assert ProjectParameter.static() == {"foo": 3.14, "bar": None}
    assert ProjectParameter.static(only=["foo"]) == {"foo": 3.14}
    ProjectParameter.recalculate()
    assert ProjectParameter.static() == {"foo": 3.14, "bar": 2 * 3.14}
    assert ProjectParameter.static(only=["bar"]) == {"bar": 2 * 3.14}


@bw2test
def test_project_parameters_expired():
    assert not ProjectParameter.expired()
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    assert ProjectParameter.expired()
    ProjectParameter.recalculate()
    assert not ProjectParameter.expired()


@bw2test
def test_project_parameters_recalculate():
    ProjectParameter.recalculate()
    Group.create(name="project")
    ProjectParameter.recalculate()
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    another = ProjectParameter.create(name="bar", formula="2 * foo",)
    ProjectParameter.recalculate()
    obj = ProjectParameter.get(name="bar")
    assert obj.amount == 2 * 3.14


@bw2test
def test_project_parameters_expire_downstream():
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    Group.create(name="bar")
    GroupDependency.create(group="bar", depends="project")
    assert Group.get(name="bar").fresh
    ProjectParameter.recalculate()
    assert not Group.get(name="bar").fresh


@bw2test
def test_project_autoupdate_triggers():
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    first = Group.get(name="project").updated
    time.sleep(1.1)
    another = ProjectParameter.create(name="bar", formula="2 * foo",)
    second = Group.get(name="project").updated
    assert first != second
    time.sleep(1.1)
    ProjectParameter.update(amount=7).execute()
    third = Group.get(name="project").updated
    assert second != third
    time.sleep(1.1)
    ProjectParameter.get(name="foo").delete_instance()
    fourth = Group.get(name="project").updated
    assert fourth != third


@bw2test
def test_project_name_uniqueness():
    obj = ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    with pytest.raises(IntegrityError):
        ProjectParameter.create(
            name="foo", amount=7,
        )


@bw2test
def test_project_parameter_dependency_chain():
    ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    ProjectParameter.create(name="bar", amount=6.28, formula="foo * 2")
    expected = [
        {"kind": "project", "group": "project", "names": set(["foo"])},
    ]
    assert ProjectParameter.dependency_chain() == expected


@bw2test
def test_project_parameter_dependency_chain_missing():
    ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    ProjectParameter.create(name="baz", amount=8, formula="foo * bar")
    with pytest.raises(MissingName):
        ProjectParameter.dependency_chain()


@bw2test
def test_project_parameter_depend_within_group():
    ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    ProjectParameter.create(name="baz", amount=8, formula="foo * 2")
    assert ProjectParameter.is_dependency_within_group("foo")
    assert not ProjectParameter.is_dependency_within_group("baz")


@bw2test
def test_project_parameter_is_deletable():
    """ Project parameters can be deleted if they are no dependencies.
    """
    ProjectParameter.create(name="foo", amount=3.14)
    assert ProjectParameter.get(name="foo").is_deletable()


@bw2test
def test_project_parameter_is_not_deletable_project():
    ProjectParameter.create(name="foo", amount=3.14)
    ProjectParameter.create(name="bar", amount=1, formula="foo * 2")
    assert not ProjectParameter.get(name="foo").is_deletable()


@bw2test
def test_project_parameter_is_not_deletable_database():
    ProjectParameter.create(name="foo", amount=3.14)
    Database("B").register()
    DatabaseParameter.create(database="B", name="bar", amount=1, formula="foo * 5")
    # Recalculate to build GroupDependencies.
    parameters.recalculate()
    assert not ProjectParameter.get(name="foo").is_deletable()


@bw2test
def test_project_parameter_is_not_deletable_activity():
    ProjectParameter.create(name="foo", amount=3.14)
    Database("B").register()
    ActivityParameter.create(
        group="baz", database="B", code="first", name="bar", amount=0, formula="foo + 4"
    )
    # Recalculate to build GroupDependencies.
    parameters.recalculate()
    assert not ProjectParameter.get(name="foo").is_deletable()


@bw2test
def test_project_parameter_formula_update():
    """ Update formulas only where the name of the parameter is an exact match.
    """
    ProjectParameter.create(name="foo", amount=3.14)
    ProjectParameter.create(name="foobar", amount=6.28)
    ProjectParameter.create(name="bar", amount=1, formula="foo + 2")
    ProjectParameter.create(name="baz", amount=1, formula="foobar * 3")
    assert (
        ProjectParameter.select()
        .where(ProjectParameter.formula.contains("foo"))
        .count()
        == 2
    )
    ProjectParameter.update_formula_parameter_name("foo", "efficiency")
    assert ProjectParameter.get(name="bar").formula == "efficiency + 2"
    assert ProjectParameter.get(name="baz").formula == "foobar * 3"


#######################
### Database parameters
#######################


@bw2test
def test_create_database_parameters():
    assert not len(parameters)
    obj = DatabaseParameter.create(database="bar", name="foo", amount=3.14,)
    assert obj.name == "foo"
    assert obj.database == "bar"
    assert obj.amount == 3.14
    assert str(obj)
    assert isinstance(str(obj), str)
    assert len(parameters)


@bw2test
def test_database_parameters_group_autocreated():
    assert not Group.select().count()
    obj = DatabaseParameter.create(database="bar", name="foo", amount=3.14,)
    assert Group.get(name="bar")
    assert not Group.get(name="bar").fresh


@bw2test
def test_database_parameters_expired():
    assert not DatabaseParameter.expired("bar")
    DatabaseParameter.create(
        database="bar", name="foo", amount=3.14,
    )
    assert DatabaseParameter.expired("bar")


@bw2test
def test_database_parameters_dict():
    obj = DatabaseParameter.create(database="bar", name="foo", amount=3.14,)
    expected = {
        "database": "bar",
        "name": "foo",
        "amount": 3.14,
    }
    assert obj.dict == expected


@bw2test
def test_database_parameters_load():
    DatabaseParameter.create(
        database="bar", name="foo", amount=3.14,
    )
    DatabaseParameter.create(database="bar", name="baz", formula="foo + baz")
    expected = {
        "foo": {"database": "bar", "amount": 3.14},
        "baz": {"database": "bar", "formula": "foo + baz"},
    }
    assert DatabaseParameter.load("bar") == expected


@bw2test
def test_database_parameters_static():
    DatabaseParameter.create(
        database="bar", name="foo", amount=3.14,
    )
    DatabaseParameter.create(database="bar", name="baz", amount=7, formula="foo + baz")
    expected = {"foo": 3.14, "baz": 7}
    assert DatabaseParameter.static("bar") == expected
    assert DatabaseParameter.static("bar", only=["baz"]) == {"baz": 7}


@bw2test
def test_database_parameters_check():
    with pytest.raises(IntegrityError):
        DatabaseParameter.create(
            database="project", name="foo", amount=3.14,
        )


@bw2test
def test_database_autoupdate_triggers():
    obj = DatabaseParameter.create(database="A", name="foo", amount=3.14,)
    first = Group.get(name="A").updated
    time.sleep(1.1)
    another = DatabaseParameter.create(database="A", name="bar", formula="2 * foo",)
    second = Group.get(name="A").updated
    assert first != second
    time.sleep(1.1)
    DatabaseParameter.update(amount=7).execute()
    third = Group.get(name="A").updated
    assert second != third
    time.sleep(1.1)
    DatabaseParameter.get(name="foo").delete_instance()
    fourth = Group.get(name="A").updated
    assert fourth != third


@bw2test
def test_database_uniqueness_constraint():
    DatabaseParameter.create(
        database="A", name="foo", amount=3.14, data={"uncertainty type": 0}
    )
    with pytest.raises(IntegrityError):
        DatabaseParameter.create(
            database="A", name="foo", amount=7,
        )


@bw2test
def test_database_parameter_cross_database_constraint():
    """Database parameters cannot use parameters on other databases."""
    Database("B").register()
    Database("C").register()
    DatabaseParameter.create(
        database="B", name="car", amount=8,
    )
    DatabaseParameter.create(
        database="C", name="plane", formula="car ** 5",
    )
    with pytest.raises(MissingName):
        DatabaseParameter.recalculate("C")


@bw2test
def test_update_database_parameters():
    assert not Group.select().count()
    assert not GroupDependency.select().count()

    DatabaseParameter.create(
        database="A", name="B", amount=5,
    )
    o = DatabaseParameter.create(database="A", name="C", formula="B * 2 + foo",)
    Group.create(name="Zed")
    GroupDependency.create(group="Zed", depends="A")
    assert Group.get(name="A")
    with pytest.raises(MissingName):
        DatabaseParameter.recalculate("A")
    o.formula = "B * 2"
    o.save()
    DatabaseParameter.recalculate("A")
    assert Group.get(name="A").fresh
    assert DatabaseParameter.get(name="C").amount == 10
    assert not Group.get(name="Zed").fresh

    o.formula = "B * 2 + foo + bar"
    o.save()
    ProjectParameter.create(name="foo", amount=3.14, data={"uncertainty type": 0})
    ProjectParameter.create(
        name="bar", formula="2 * foo",
    )
    assert Group.get(name="project")
    Database("A").register()

    obj = DatabaseParameter.get(name="C")
    assert obj.amount != 3.14 * 3 + 10
    with pytest.raises(GroupDependency.DoesNotExist):
        GroupDependency.get(group="A", depends="project")

    DatabaseParameter.recalculate("A")
    assert GroupDependency.get(group="A", depends="project")
    assert Group.get(name="A")
    assert Group.get(name="project")
    obj = DatabaseParameter.get(name="C")
    assert obj.amount == 3.14 * 3 + 10


@bw2test
def test_database_parameter_dependency_chain():
    Database("B").register()
    DatabaseParameter.create(
        database="B", name="car", formula="2 ** fly", amount=8,
    )
    DatabaseParameter.create(
        database="B", name="bike", formula="car - hike", amount=2,
    )
    ProjectParameter.create(
        name="hike", formula="2 * 2 * 2", amount=6,
    )
    ProjectParameter.create(
        name="fly", formula="3", amount=3,
    )
    expected = [
        {"kind": "project", "group": "project", "names": set(["fly", "hike"])},
    ]
    assert DatabaseParameter.dependency_chain("B") == expected
    assert DatabaseParameter.dependency_chain("missing") == []


@bw2test
def test_database_parameter_dependency_chain_missing():
    Database("B").register()
    DatabaseParameter.create(
        database="B", name="car", formula="2 ** fly", amount=8,
    )
    ProjectParameter.create(
        name="hike", formula="2 * 2 * 2", amount=6,
    )
    with pytest.raises(MissingName):
        DatabaseParameter.dependency_chain("B")


@bw2test
def test_database_parameter_dependency_chain_include_self():
    Database("B").register()
    DatabaseParameter.create(
        database="B", name="car", formula="2 ** fly", amount=8,
    )
    DatabaseParameter.create(
        database="B", name="truck", formula="car * 5",
    )
    ProjectParameter.create(
        name="fly", formula="3", amount=3,
    )
    expected = [
        {"kind": "project", "group": "project", "names": set(["fly"])},
        {"kind": "database", "group": "B", "names": set(["car"])},
    ]
    # Method now also includes required names within group
    assert DatabaseParameter.dependency_chain("B", include_self=True) == expected


@bw2test
def test_database_parameter_depend_within_group():
    Database("B").register()
    Database("C").register()
    DatabaseParameter.create(
        database="B", name="car", formula="2 ** fly", amount=8,
    )
    DatabaseParameter.create(
        database="B", name="truck", amount=2, formula="car * 5",
    )
    DatabaseParameter.create(
        database="C", name="fly", amount=7,
    )
    DatabaseParameter.create(
        database="C", name="parade", amount=1, formula="fly * 2.7",
    )
    ProjectParameter.create(
        name="fly", formula="3", amount=3,
    )
    parameters.recalculate()
    assert DatabaseParameter.is_dependency_within_group("car", "B")
    assert not DatabaseParameter.is_dependency_within_group("truck", "B")
    assert DatabaseParameter.is_dependency_within_group("fly", "C")


@bw2test
def test_database_parameter_is_deletable():
    """ Database parameters can be deleted if they are no dependencies.
    """
    Database("B").register()
    DatabaseParameter.create(database="B", name="car", amount=8)
    assert DatabaseParameter.get(name="car").is_deletable()


@bw2test
def test_database_parameter_is_not_deletable_database():
    Database("B").register()
    DatabaseParameter.create(database="B", name="car", amount=8)
    DatabaseParameter.create(database="B", name="truck", formula="car * 5", amount=4)
    assert not DatabaseParameter.get(name="car").is_deletable()


@bw2test
def test_database_parameter_is_not_deletable_activity():
    Database("B").register()
    DatabaseParameter.create(database="B", name="car", amount=8)
    ActivityParameter.create(
        group="cars",
        database="B",
        code="first",
        name="bar",
        amount=0,
        formula="car + 4",
    )
    # Build GroupDependencies
    parameters.recalculate()
    assert not DatabaseParameter.get(name="car").is_deletable()


@bw2test
def test_database_parameter_is_dependent_on():
    """ Databases parameters can be dependent on project parameters.
    """
    Database("B").register()
    ProjectParameter.create(name="foo", amount=2)
    ProjectParameter.create(name="bar", amount=5)
    DatabaseParameter.create(database="B", name="baz", amount=1, formula="foo + 2")
    parameters.recalculate()
    assert DatabaseParameter.is_dependent_on("foo")
    assert not DatabaseParameter.is_dependent_on("bar")


@bw2test
def test_database_parameter_formula_update_project():
    """ Update formulas of database parameters, only update the formulas
    where the actual ProjectParameter is referenced.
    """
    ProjectParameter.create(name="foo", amount=2)
    ProjectParameter.create(name="tracks", amount=14)
    Database("B").register()
    Database("C").register()
    DatabaseParameter.create(database="B", name="bar", amount=1, formula="foo + 2")
    DatabaseParameter.create(database="C", name="bing", amount=1, formula="foo + 2")
    DatabaseParameter.create(database="C", name="foo", amount=8, formula="tracks * 2")
    parameters.recalculate()
    assert DatabaseParameter.get(name="bar").formula == "foo + 2"
    assert DatabaseParameter.get(name="bing").formula == "foo + 2"
    DatabaseParameter.update_formula_project_parameter_name("foo", "baz")
    assert DatabaseParameter.get(name="bar").formula == "baz + 2"
    assert DatabaseParameter.get(name="bing").formula == "foo + 2"


@bw2test
def test_database_parameter_formula_update_database():
    """ Update formulas of database parameters, only update the formulas
    where the actual DatabaseParameter is referenced.
    """
    ProjectParameter.create(name="foo", amount=2)
    Database("B").register()
    Database("C").register()
    DatabaseParameter.create(database="B", name="bar", amount=1, formula="foo + 2")
    DatabaseParameter.create(database="C", name="bing", amount=1, formula="foo + 2")
    DatabaseParameter.create(
        database="C", name="foo", amount=8,
    )
    parameters.recalculate()
    assert DatabaseParameter.get(name="bar").formula == "foo + 2"
    assert DatabaseParameter.get(name="bing").formula == "foo + 2"
    DatabaseParameter.update_formula_database_parameter_name("foo", "baz")
    assert DatabaseParameter.get(name="bar").formula == "foo + 2"
    assert DatabaseParameter.get(name="bing").formula == "baz + 2"


###########################
### Parameterized exchanges
###########################


@bw2test
def test_create_parameterized_exchange_missing_group():
    with pytest.raises(IntegrityError):
        obj = ParameterizedExchange.create(group="A", exchange=42, formula="foo + bar")


@bw2test
def test_create_parameterized_exchange():
    assert not ParameterizedExchange.select().count()
    ActivityParameter.insert_dummy("A", ("b", "c"))
    obj = ParameterizedExchange.create(group="A", exchange=42, formula="foo + bar")
    assert obj.group == "A"
    assert obj.exchange == 42
    assert obj.formula == "foo + bar"
    assert ParameterizedExchange.select().count()


@bw2test
def test_create_parameterized_exchange_nonunique():
    ActivityParameter.insert_dummy("A", ("b", "c"))
    ParameterizedExchange.create(group="A", exchange=42, formula="foo + bar")
    with pytest.raises(IntegrityError):
        ParameterizedExchange.create(group="B", exchange=42, formula="2 + 3")


#######################
### Activity parameters
#######################


@pytest.fixture
@bw2test
def chain():
    Database("B").register()
    Database("K").register()
    Group.create(name="G", order=["A"])
    ActivityParameter.create(
        group="A", database="B", code="C", name="D", formula="2 ** 3", amount=1,
    )
    ActivityParameter.create(
        group="A", database="B", code="E", name="F", formula="foo + bar + D", amount=2,
    )
    ActivityParameter.create(
        group="G", database="K", code="H", name="J", formula="F + D * 2", amount=3,
    )
    DatabaseParameter.create(
        database="B", name="foo", formula="2 ** 2", amount=5,
    )
    ProjectParameter.create(
        name="bar", formula="2 * 2 * 2", amount=6,
    )


@bw2test
def test_create_activity_parameter():
    assert not ActivityParameter.select().count()
    obj = ActivityParameter.create(
        group="A", database="B", code="C", name="D", amount=3.14
    )
    assert obj.group == "A"
    assert obj.database == "B"
    assert obj.code == "C"
    assert obj.name == "D"
    assert obj.amount == 3.14
    assert str(obj)
    assert isinstance(str(obj), str)
    assert ActivityParameter.select().count()
    assert len(parameters)


@bw2test
def test_activity_parameters_group_autocreated():
    assert not Group.select().count()
    ActivityParameter.create(group="A", database="B", code="C", name="D", amount=3.14)
    assert Group.get(name="A")
    assert not Group.get(name="A").fresh


@bw2test
def test_activity_parameter_expired():
    assert not ActivityParameter.expired("A")
    ActivityParameter.create(group="A", database="B", code="C", name="D", amount=3.14)
    assert ActivityParameter.expired("A")
    Group.get(name="A").freshen()
    assert not ActivityParameter.expired("A")


@bw2test
def test_activity_parameter_dict():
    a = ActivityParameter.create(
        group="A", database="B", code="C", name="D", amount=3.14
    )
    expected = {"database": "B", "code": "C", "name": "D", "amount": 3.14}
    assert a.dict == expected
    b = ActivityParameter.create(
        group="A",
        database="B",
        code="E",
        name="F",
        amount=7,
        data={"foo": "bar"},
        formula="7 * 1",
    )
    expected = {
        "database": "B",
        "code": "E",
        "name": "F",
        "amount": 7,
        "foo": "bar",
        "formula": "7 * 1",
    }
    assert b.dict == expected


@bw2test
def test_activity_parameter_load():
    ActivityParameter.create(
        group="A",
        database="B",
        code="E",
        name="F",
        amount=7,
        data={"foo": "bar"},
        formula="7 * 1",
    )
    expected = {
        "F": {
            "database": "B",
            "code": "E",
            "amount": 7,
            "foo": "bar",
            "formula": "7 * 1",
        }
    }
    assert ActivityParameter.load("A") == expected


def test_activity_parameter_static(chain):
    expected = {"D": 1, "F": 2}
    assert ActivityParameter.static("A") == expected
    expected = {}
    assert ActivityParameter.static("A", only=[]) == expected
    expected = {"D": 1}
    assert ActivityParameter.static("A", only=["D"]) == expected
    expected = {"D": 1, "F": 2, "foo": 5, "bar": 6}
    assert ActivityParameter.static("A", full=True) == expected
    expected = {"foo": 5, "bar": 6}
    assert ActivityParameter.static("A", full=True, only=["foo", "bar"]) == expected


@bw2test
def test_activity_parameter_recalculate_shortcut():
    assert not ActivityParameter.recalculate("A")
    ActivityParameter.create(group="A", database="B", code="C", name="D", amount=3.14)
    Group.get(name="A").freshen()
    assert not ActivityParameter.recalculate("A")


def test_activity_parameter_dependency_chain(chain):
    expected = [{"kind": "activity", "group": "A", "names": set(["D", "F"])}]
    assert ActivityParameter.dependency_chain("G") == expected
    expected = [
        {"kind": "database", "group": "B", "names": set(["foo"])},
        {"kind": "project", "group": "project", "names": set(["bar"])},
    ]
    assert ActivityParameter.dependency_chain("A") == expected


def test_activity_parameter_dependency_chain_missing(chain):
    """ Use unknown parameter 'K' in formula to test for MissingName error.
    """
    ActivityParameter.create(
        group="G", database="K", code="L", name="M", formula="foo + bar / K", amount=7,
    )
    with pytest.raises(MissingName):
        ActivityParameter.dependency_chain("G")


def test_activity_parameter_dependency_chain_includes_exchanges(chain):
    ProjectParameter.create(name="something_new", amount=10)
    db = Database("K")
    a = db.new_activity(code="something something danger zone", name="An activity")
    a.save()
    a.new_exchange(
        amount=0, input=a, type="production", formula="something_new + 4 - J"
    ).save()
    parameters.add_exchanges_to_group("G", a)

    expected = [
        {"kind": "activity", "group": "A", "names": {"D", "F"}},
        {"group": "project", "kind": "project", "names": {"something_new"}},
    ]
    assert ActivityParameter.dependency_chain("G") == expected


def test_activity_parameter_dependency_chain_include_self(chain):
    """ Out of the parameters 'D' and 'F' in group 'A', only 'D' counts
    as a dependency for group 'A'.

    This means that 'F' can be freely deleted, after which 'D' is no longer
    a dependency for group 'A' (as 'D' was a dependency of 'F') and can now
    also be deleted.
    """
    expected = [
        {"kind": "database", "group": "B", "names": set(["foo"])},
        {"kind": "project", "group": "project", "names": set(["bar"])},
    ]
    assert ActivityParameter.dependency_chain("A") == expected
    expected = [
        {"kind": "activity", "group": "A", "names": set(["D"])},
        {"kind": "database", "group": "B", "names": set(["foo"])},
        {"kind": "project", "group": "project", "names": set(["bar"])},
    ]
    assert ActivityParameter.dependency_chain("A", include_self=True) == expected


def test_activity_parameter_dependency_chain_include_self_exchanges(chain):
    """ Out of the parameters 'J' and 'H' in group 'G', only 'H' counts
    as a dependency as 'J' is not used by either 'H' or by any exchanges.
    """
    ActivityParameter.create(
        group="G", database="K", code="L", name="H", amount=7,
    )
    db = Database("K")
    a = db.new_activity(code="not a robot", name="actually an activity")
    a.save()
    a.new_exchange(amount=0, input=a, type="production", formula="15 / H").save()
    parameters.add_exchanges_to_group("G", a)

    expected = [
        {"kind": "activity", "group": "A", "names": set(["D", "F"])},
    ]
    assert ActivityParameter.dependency_chain("G") == expected
    expected = [
        {"kind": "activity", "group": "A", "names": set(["D", "F"])},
        {"kind": "activity", "group": "G", "names": set(["H"])},
    ]
    assert ActivityParameter.dependency_chain("G", include_self=True) == expected


def test_activity_parameter_depend_within_group(chain):
    """ When considering only dependencies within the given group. 'D' is
    a dependency within the group 'A', while 'F' is not.
    """
    assert ActivityParameter.is_dependency_within_group("D", "A")
    assert not ActivityParameter.is_dependency_within_group("F", "A")


def test_activity_parameter_depend_within_group_include(chain):
    """ The 'J' parameter in group 'G' depends on the 'F' parameter in group
    'A'. 'F' doesn't exist within the 'G' group but is instead linked to the
    'J' parameter through the 'G' group order.
    """
    parameters.recalculate()
    assert ActivityParameter.is_dependent_on("F", "A")
    assert not ActivityParameter.is_dependency_within_group("F", "G")
    assert ActivityParameter.is_dependency_within_group("F", "G", include_order=True)


@bw2test
def test_activity_parameter_dummy():
    assert not ActivityParameter.select().count()
    ActivityParameter.insert_dummy("A", ("B", "C"))
    assert ActivityParameter.select().count() == 1
    a = ActivityParameter.get()
    assert a.name.startswith("__dummy_") and uuid4hex.search(a.name)
    assert a.database == "B"
    assert a.amount == 0

    ActivityParameter.insert_dummy("A", ("B", "C"))
    assert ActivityParameter.select().count() == 1


@bw2test
def test_activity_parameter_multiple_dummies():
    assert not ActivityParameter.select().count()
    ActivityParameter.insert_dummy("A", ("B", "C"))
    ActivityParameter.insert_dummy("A", ("B", "D"))
    assert ActivityParameter.select().count() == 2
    assert all(
        ap.name.startswith("__dummy_") and uuid4hex.search(ap.name)
        for ap in ActivityParameter.select()
    )


def test_activity_parameter_static_dependencies(chain):
    expected = {"foo": 5, "bar": 6}
    assert ActivityParameter._static_dependencies("A") == expected
    expected = {"bar": 6, "D": 1, "F": 2}
    assert ActivityParameter._static_dependencies("G") == expected


@bw2test
def test_activity_parameter_recalculate_exchanges():
    db = Database("example")
    db.register()
    assert not len(parameters)
    assert not len(db)

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(
        amount=0, input=b, type="technosphere", formula="foo * bar + 4"
    ).save()

    project_data = [
        {"name": "foo", "formula": "green / 7",},
        {"name": "green", "amount": 7},
    ]
    parameters.new_project_parameters(project_data)

    database_data = [
        {"name": "red", "formula": "(foo + blue ** 2) / 5",},
        {"name": "blue", "amount": 12},
    ]
    parameters.new_database_parameters(database_data, "example")

    activity_data = [
        {
            "name": "reference_me",
            "formula": "sqrt(red - 20)",
            "database": "example",
            "code": "B",
        },
        {
            "name": "bar",
            "formula": "reference_me + 2",
            "database": "example",
            "code": "A",
        },
    ]
    parameters.new_activity_parameters(activity_data, "my group")

    parameters.add_exchanges_to_group("my group", a)
    ActivityParameter.recalculate_exchanges("my group")

    for exc in a.exchanges():
        # (((1 + 12 ** 2) / 5 - 20) ** 0.5 + 2) + 4
        assert exc.amount == 9


@bw2test
def test_pe_no_activities_parameter_group_error():
    db = Database("example")
    db.register()
    assert not len(parameters)
    assert not len(db)

    a = db.new_activity(code="A", name="An activity")
    a.save()
    a.new_exchange(amount=0, input=a, type="production").save()

    for exc in a.exchanges():
        obj = ParameterizedExchange(
            exchange=exc._document.id, group="my group", formula="1 + 1",
        )
        with pytest.raises(IntegrityError):
            obj.save()


@bw2test
def test_recalculate_exchanges_no_activities_parameters():
    db = Database("example")
    db.register()
    assert not len(parameters)
    assert not len(db)

    a = db.new_activity(code="A", name="An activity")
    a.save()
    a.new_exchange(amount=0, input=a, type="production", formula="foo + 4").save()

    project_data = [
        {"name": "foo", "formula": "green / 7",},
        {"name": "green", "amount": 7},
    ]
    parameters.new_project_parameters(project_data)

    assert ActivityParameter.select().count() == 0
    parameters.add_exchanges_to_group("my group", a)
    ActivityParameter.recalculate_exchanges("my group")

    for exc in a.exchanges():
        assert exc.amount == 5
        assert exc.get("formula")

    assert ActivityParameter.select().count() == 1
    a = ActivityParameter.get()
    assert a.name.startswith("__dummy_") and uuid4hex.search(a.name)


@bw2test
def test_activity_parameter_recalculate():
    Database("B").register()
    ActivityParameter.create(
        group="A", database="B", code="C", name="D", formula="2 ** 3"
    )
    ActivityParameter.create(
        group="A", database="B", code="E", name="F", formula="2 * D"
    )
    assert not Group.get(name="A").fresh
    ActivityParameter.recalculate("A")
    assert ActivityParameter.get(name="D").amount == 8
    assert ActivityParameter.get(name="F").amount == 16
    assert Group.get(name="A").fresh

    Database("K").register()
    ActivityParameter.create(
        group="G", database="K", code="H", name="J", formula="F + D * 2"
    )
    ActivityParameter.create(
        group="G", database="K", code="E", name="F", amount=3,
    )
    assert not Group.get(name="G").fresh
    with pytest.raises(MissingName):
        ActivityParameter.recalculate("G")

    assert not Group.get(name="G").fresh
    g = Group.get(name="G")
    g.order = ["A"]
    g.save()
    ActivityParameter.recalculate("G")
    assert Group.get(name="G").fresh
    assert ActivityParameter.get(name="J").amount == 19
    assert ActivityParameter.get(name="F", database="K").amount == 3

    DatabaseParameter.create(
        database="B", name="foo", formula="2 ** 2",
    )
    ProjectParameter.create(
        name="bar", formula="2 * 2 * 2",
    )
    a = ActivityParameter.get(database="B", code="E")
    a.formula = "foo + bar + D"
    a.save()
    assert not Group.get(name="A").fresh
    ActivityParameter.recalculate("A")
    assert ActivityParameter.get(database="B", code="E").amount == 4 + 8 + 8
    assert Group.get(name="A").fresh


def test_activity_parameter_is_deletable(chain):
    """ An activity parameter is deletable if it is not a dependency of another
    activity parameter.
    """
    # Ensure that all GroupDependencies exist.
    Group.get(name="G").expire()
    parameters.recalculate()

    # Is not used by any other activity parameter
    assert ActivityParameter.get(name="J", group="G").is_deletable()
    # Is used by the 'F' activity parameter from the same group.
    assert not ActivityParameter.get(name="D", group="A").is_deletable()
    # Is used by the 'J' activity parameter from group 'G'
    assert not ActivityParameter.get(name="F", group="A").is_deletable()


def test_activity_parameter_is_dependent_on(chain):
    """ An activity parameter can be dependent on any other type of parameter.
    """
    # Ensure that GroupDependencies exist.
    parameters.recalculate()

    # Some activity parameter is using the "bar" project parameter
    assert ActivityParameter.is_dependent_on("bar", "project")
    # Some activity parameter is using the "foo" database parameter
    assert ActivityParameter.is_dependent_on("foo", "B")
    # Some activity parameter is using the "F" activity parameter
    assert ActivityParameter.is_dependent_on("F", "A")
    # No activity parameter is dependent on the "J" activity parameter
    assert not ActivityParameter.is_dependent_on("J", "G")


def test_activity_parameter_formula_update_project(chain):
    ActivityParameter.create(
        group="G", database="K", code="AA", name="bar", amount=2,
    )
    ActivityParameter.create(
        group="G", database="K", code="BB", name="baz", amount=5, formula="bar * 6"
    )
    parameters.recalculate()
    assert ActivityParameter.get(name="F", group="A").formula == "foo + bar + D"
    assert ActivityParameter.get(name="baz", group="G").formula == "bar * 6"
    ActivityParameter.update_formula_project_parameter_name("bar", "banana")
    assert ActivityParameter.get(name="F", group="A").formula == "foo + banana + D"
    assert ActivityParameter.get(name="baz", group="G").formula == "bar * 6"


def test_activity_parameter_formula_update_database(chain):
    ActivityParameter.create(
        group="G", database="K", code="AA", name="foo", amount=2,
    )
    ActivityParameter.create(
        group="G", database="K", code="BB", name="baz", amount=5, formula="foo * 6"
    )
    parameters.recalculate()
    assert ActivityParameter.get(name="F", group="A").formula == "foo + bar + D"
    assert ActivityParameter.get(name="baz", group="G").formula == "foo * 6"
    ActivityParameter.update_formula_database_parameter_name("foo", "mango")
    assert ActivityParameter.get(name="F", group="A").formula == "mango + bar + D"
    assert ActivityParameter.get(name="baz", group="G").formula == "foo * 6"


def test_activity_parameter_formula_update_activity(chain):
    parameters.recalculate()
    assert ActivityParameter.get(name="F", group="A").formula == "foo + bar + D"
    assert ActivityParameter.get(name="J", group="G").formula == "F + D * 2"
    ActivityParameter.update_formula_activity_parameter_name("D", "dingo")
    assert ActivityParameter.get(name="F", group="A").formula == "foo + bar + dingo"
    assert ActivityParameter.get(name="J", group="G").formula == "F + D * 2"


def test_activity_parameter_formula_update_activity_include(chain):
    parameters.recalculate()
    assert ActivityParameter.get(name="F", group="A").formula == "foo + bar + D"
    assert ActivityParameter.get(name="J", group="G").formula == "F + D * 2"
    ActivityParameter.update_formula_activity_parameter_name(
        "D", "dingo", include_order=True
    )
    assert ActivityParameter.get(name="F", group="A").formula == "foo + bar + dingo"
    assert ActivityParameter.get(name="J", group="G").formula == "F + dingo * 2"


@bw2test
def test_activity_parameter_crossdatabase_triggers():
    ActivityParameter.create(group="A", database="B", name="C", code="D")
    with pytest.raises(IntegrityError):
        ActivityParameter.create(group="A", database="E", name="F", code="G")
    with pytest.raises(IntegrityError):
        a = ActivityParameter.get(name="C")
        a.database = "E"
        a.save()
    with pytest.raises(IntegrityError):
        ActivityParameter.update(database="C").execute()


@bw2test
def test_activity_parameter_crossgroup_triggers():
    ActivityParameter.create(
        group="A", database="B", name="C", code="D", amount=11,
    )
    with pytest.raises(IntegrityError):
        ActivityParameter.create(
            group="E", database="B", name="C", code="D", amount=1,
        )
    ActivityParameter.create(
        group="E", database="B", name="C", code="F", amount=1,
    )


@bw2test
def test_activity_parameter_autoupdate_triggers():
    obj = ActivityParameter.create(
        group="A", database="B", name="C", code="D", amount=11,
    )
    first = Group.get(name="A").updated
    time.sleep(1.1)
    another = ActivityParameter.create(
        group="A", database="B", code="E", name="F", formula="2 * foo",
    )
    second = Group.get(name="A").updated
    assert first != second
    time.sleep(1.1)
    ActivityParameter.update(amount=7).execute()
    third = Group.get(name="A").updated
    assert second != third
    time.sleep(1.1)
    ActivityParameter.get(name="F").delete_instance()
    fourth = Group.get(name="A").updated
    assert fourth != third


@bw2test
def test_activity_parameter_checks_uniqueness_constraints():
    ActivityParameter.create(
        group="A", database="B", name="C", code="D", amount=11,
    )
    with pytest.raises(IntegrityError):
        ActivityParameter.create(
            group="A", database="B", name="C", code="G", amount=111,
        )


@bw2test
def test_activity_parameter_checks():
    with pytest.raises(IntegrityError):
        ActivityParameter.create(group="project", database="E", name="F", code="G")
    with pytest.raises(IntegrityError):
        ActivityParameter.create(group="E", database="E", name="F", code="G")


##########
### Groups
##########


@bw2test
def test_group():
    o = Group.create(name="foo")
    assert o.fresh
    o.expire()
    assert not o.fresh
    o = Group.get(name="foo")
    assert not o.fresh
    o.freshen()
    assert o.fresh
    o = Group.get(name="foo")
    assert o.fresh
    with pytest.raises(IntegrityError):
        Group.create(name="foo")
    Group.create(name="bar")


@bw2test
def test_group_purging():
    Database("A").register()
    Database("B").register()
    o = Group.create(name="one", order=["C", "project", "B", "D", "A"])
    expected = ["C", "D"]
    assert o.updated
    assert o.fresh
    assert o.order == expected
    assert Group.get(name="one").order == expected


######################
### Group dependencies
######################


@bw2test
def test_group_dependency():
    d = GroupDependency.create(group="foo", depends="bar")
    assert d.group == "foo"
    assert d.depends == "bar"


@bw2test
def test_group_dependency_save_checks():
    with pytest.raises(ValueError):
        GroupDependency.create(group="project", depends="foo")
    Database("A").register()
    GroupDependency.create(group="A", depends="project")
    with pytest.raises(ValueError):
        GroupDependency.create(group="A", depends="foo")


@bw2test
def test_group_dependency_constraints():
    GroupDependency.create(group="foo", depends="bar")
    with pytest.raises(IntegrityError):
        GroupDependency.create(group="foo", depends="bar")
    with pytest.raises(IntegrityError):
        GroupDependency.create(group="foo", depends="foo")


@bw2test
def test_group_dependency_circular():
    GroupDependency.create(group="foo", depends="bar")
    with pytest.raises(IntegrityError):
        GroupDependency.create(group="bar", depends="foo")


@bw2test
def test_group_dependency_override():
    """ GroupDependency can be overridden by having a parameter with the same
    name within the group.
    """
    ProjectParameter.create(name="foo", amount=2)
    Database("B").register()
    DatabaseParameter.create(database="B", name="bar", amount=1, formula="foo * 5")
    parameters.recalculate()
    assert (
        GroupDependency.select().where(GroupDependency.depends == "project").count()
        == 1
    )
    assert DatabaseParameter.get(name="bar").amount == 10
    DatabaseParameter.create(
        database="B", name="foo", amount=8,
    )
    parameters.recalculate()
    assert (
        GroupDependency.select().where(GroupDependency.depends == "project").count()
        == 0
    )
    assert DatabaseParameter.get(name="bar").amount == 40


######################
### Parameters manager
######################


@bw2test
def test_parameters_new_project_parameters_uniqueness():
    with pytest.raises(AssertionError):
        parameters.new_project_parameters([{"name": "foo"}, {"name": "foo"}])


@bw2test
def test_parameters_new_project_parameters():
    assert not len(parameters)
    ProjectParameter.create(name="foo", amount=17)
    ProjectParameter.create(name="baz", amount=10)
    assert len(parameters) == 2
    assert ProjectParameter.get(name="foo").amount == 17
    data = [
        {"name": "foo", "amount": 4},
        {"name": "bar", "formula": "foo + 3"},
    ]
    parameters.new_project_parameters(data)
    assert len(parameters) == 3
    assert ProjectParameter.get(name="foo").amount == 4
    assert ProjectParameter.get(name="bar").amount == 7
    assert ProjectParameter.get(name="baz").amount == 10
    assert Group.get(name="project").fresh


@bw2test
def test_parameters_new_project_parameters_no_overwrite():
    ProjectParameter.create(name="foo", amount=17)
    data = [
        {"name": "foo", "amount": 4},
        {"name": "bar", "formula": "foo + 3"},
    ]
    with pytest.raises(ValueError):
        parameters.new_project_parameters(data, overwrite=False)


@bw2test
def test_parameters_repr():
    assert repr(parameters) == "Parameters manager with 0 objects"


@bw2test
def test_parameters_recalculate():
    Database("B").register()
    ActivityParameter.create(
        group="A", database="B", code="C", name="D", formula="2 ** 3"
    )
    ActivityParameter.create(
        group="A", database="B", code="E", name="F", formula="foo + bar + D"
    )
    DatabaseParameter.create(
        database="B", name="foo", formula="2 ** 2",
    )
    ProjectParameter.create(
        name="bar", formula="2 * 2 * 2",
    )
    parameters.recalculate()
    assert ProjectParameter.get(name="bar").amount == 8
    assert DatabaseParameter.get(name="foo").amount == 4
    assert ActivityParameter.get(name="F").amount == 20
    assert ActivityParameter.get(name="D").amount == 8


@bw2test
def test_parameters_new_database_parameters():
    with pytest.raises(AssertionError):
        parameters.new_database_parameters([], "another")
    Database("another").register()
    with pytest.raises(AssertionError):
        parameters.new_database_parameters(
            [{"name": "foo"}, {"name": "foo"}], "another"
        )
    DatabaseParameter.create(name="foo", database="another", amount=0)
    DatabaseParameter.create(name="baz", database="another", amount=21)
    assert len(parameters) == 2
    assert DatabaseParameter.get(name="foo").amount == 0
    data = [
        {"name": "foo", "amount": 4},
        {"name": "bar", "formula": "foo + 3"},
    ]
    parameters.new_database_parameters(data, "another")
    assert len(parameters) == 3
    assert DatabaseParameter.get(name="foo").amount == 4
    assert DatabaseParameter.get(name="bar").amount == 7
    assert DatabaseParameter.get(name="baz").amount == 21
    assert Group.get(name="another").fresh


@bw2test
def test_parameters_new_database_parameters_no_overwrite():
    Database("another").register()
    DatabaseParameter.create(name="foo", database="another", amount=0)
    with pytest.raises(ValueError):
        parameters.new_database_parameters(
            [{"name": "foo", "amount": 4}], "another", overwrite=False
        )


@bw2test
def test_parameters_new_activity_parameters_errors():
    with pytest.raises(AssertionError):
        parameters.new_activity_parameters([], "example")
    with pytest.raises(AssertionError):
        parameters.new_activity_parameters(
            [{"database": 1}, {"database": 2}], "example"
        )

    with pytest.raises(AssertionError):
        parameters.new_activity_parameters([{"database": "unknown"}], "example")

    Database("A").register()
    with pytest.raises(AssertionError):
        parameters.new_activity_parameters(
            [{"database": "A", "name": "foo"}, {"database": "A", "name": "foo"}],
            "example",
        )


@bw2test
def test_parameters_new_activity_parameters():
    assert not len(parameters)
    assert not Group.select().count()
    Database("A").register()
    ActivityParameter.create(
        group="another", database="A", name="baz", code="D", amount=49
    )
    ActivityParameter.create(
        group="another", database="A", name="foo", code="E", amount=101
    )
    assert len(parameters) == 2
    assert ActivityParameter.get(name="foo").amount == 101
    assert ActivityParameter.get(name="baz").amount == 49
    data = [
        {"database": "A", "code": "B", "name": "foo", "amount": 4},
        {
            "database": "A",
            "code": "C",
            "name": "bar",
            "formula": "foo + 3",
            "uncertainty type": 0,
        },
    ]
    parameters.new_activity_parameters(data, "another")
    assert len(parameters) == 3
    assert ActivityParameter.get(name="foo").amount == 4
    assert ActivityParameter.get(name="foo").code == "B"
    assert ActivityParameter.get(name="baz").amount == 49
    a = ActivityParameter.get(code="C")
    assert a.database == "A"
    assert a.name == "bar"
    assert a.formula == "foo + 3"
    assert a.data == {"uncertainty type": 0}
    assert a.amount == 7
    assert ActivityParameter.get(name="foo").amount == 4
    assert Group.get(name="another").fresh


@bw2test
def test_parameters_new_activity_parameters_no_overlap():
    Database("A").register()
    ActivityParameter.create(
        group="another", database="A", name="foo", code="D", amount=49
    )
    data = [
        {"database": "A", "code": "B", "name": "foo", "amount": 4},
        {
            "database": "A",
            "code": "C",
            "name": "bar",
            "formula": "foo + 3",
            "uncertainty type": 0,
        },
    ]
    with pytest.raises(ValueError):
        parameters.new_activity_parameters(data, "another", overwrite=False)


@bw2test
def test_parameters_rename_project_parameter():
    """ Project parameters can be renamed. """
    param = ProjectParameter.create(name="foo", amount=7,)
    assert ProjectParameter.select().where(ProjectParameter.name == "foo").count() == 1
    parameters.rename_project_parameter(param, "foobar")
    with pytest.raises(ProjectParameter.DoesNotExist):
        ProjectParameter.get(name="foo")
    assert (
        ProjectParameter.select().where(ProjectParameter.name == "foobar").count() == 1
    )


@bw2test
def test_parameters_rename_project_parameter_incorrect_type():
    Database("B").register()
    param = DatabaseParameter.create(database="B", name="foo", amount=5,)
    with pytest.raises(TypeError):
        parameters.rename_project_parameter(param, "bar")


@bw2test
def test_parameters_rename_project_parameter_dependencies():
    """ Updating downstream parameters will update all relevant formulas
    to use the new name for the parameter.
    """
    param = ProjectParameter.create(name="foo", amount=7,)
    ProjectParameter.create(name="bar", amount=1, formula="foo * 2")
    assert ProjectParameter.is_dependency_within_group("foo")
    parameters.rename_project_parameter(param, "baz", update_dependencies=True)
    assert ProjectParameter.get(name="bar").formula == "baz * 2"


@bw2test
def test_parameters_rename_project_parameter_dependencies_fail():
    """ An exception is raised if rename is attempted without updating
    downstream if other parameters depend on that parameter.
    """
    param = ProjectParameter.create(name="foo", amount=7,)
    ProjectParameter.create(name="bar", amount=1, formula="foo * 2")
    with pytest.raises(ValueError):
        parameters.rename_project_parameter(param, "baz")


def test_parameters_rename_project_parameter_dependencies_full(chain):
    """ Updating downstream parameters will update all relevant formulas
    to use the new name for the parameter.

    Parameter amounts do no change as only the name is altered.
    """
    ProjectParameter.create(name="double_bar", amount=12, formula="bar * 2")
    DatabaseParameter.create(database="B", name="bing", amount=2, formula="bar ** 5")
    parameters.recalculate()
    assert ProjectParameter.is_dependency_within_group("bar")
    assert DatabaseParameter.is_dependent_on("bar")
    assert ActivityParameter.is_dependent_on("bar", "project")
    assert ProjectParameter.get(name="double_bar").amount == 16
    assert DatabaseParameter.get(name="bing", database="B").amount == 32768
    assert ActivityParameter.get(name="F", group="A").amount == 20

    param = ProjectParameter.get(name="bar")
    parameters.rename_project_parameter(param, "new_bar", update_dependencies=True)

    assert ProjectParameter.get(name="double_bar").formula == "new_bar * 2"
    assert DatabaseParameter.get(name="bing", database="B").formula == "new_bar ** 5"
    assert ActivityParameter.get(name="F", group="A").formula == "foo + new_bar + D"
    assert ProjectParameter.get(name="double_bar").amount == 16
    assert DatabaseParameter.get(name="bing", database="B").amount == 32768
    assert ActivityParameter.get(name="F", group="A").amount == 20


@bw2test
def test_parameters_rename_database_parameter():
    Database("B").register()
    param = DatabaseParameter.create(database="B", name="foo", amount=5,)
    assert (
        DatabaseParameter.select().where(DatabaseParameter.name == "foo").count() == 1
    )
    parameters.rename_database_parameter(param, "bar")
    with pytest.raises(DatabaseParameter.DoesNotExist):
        DatabaseParameter.get(name="foo")
    assert (
        DatabaseParameter.select().where(DatabaseParameter.name == "bar").count() == 1
    )


def test_parameters_rename_database_parameter_dependencies(chain):
    DatabaseParameter.create(database="B", name="baz", amount=1, formula="foo + 2")
    parameters.recalculate()
    param = DatabaseParameter.get(name="foo")
    parameters.rename_database_parameter(param, "foobar", True)
    assert DatabaseParameter.get(name="baz").formula == "foobar + 2"
    assert ActivityParameter.get(name="F", group="A").formula == "foobar + bar + D"


def test_parameters_rename_activity_parameter(chain):
    parameters.recalculate()
    param = ActivityParameter.get(name="J", group="G")
    parameters.rename_activity_parameter(param, "John")
    with pytest.raises(ActivityParameter.DoesNotExist):
        ActivityParameter.get(name="J", group="G")
    assert (
        ActivityParameter.select()
        .where(ActivityParameter.name == "John", ActivityParameter.group == "G")
        .count()
        == 1
    )


def test_parameters_rename_activity_parameter_dependencies(chain):
    parameters.recalculate()
    param = ActivityParameter.get(name="D", group="A")
    parameters.rename_activity_parameter(param, "Dirk", True)
    assert ActivityParameter.get(name="F", group="A").formula == "foo + bar + Dirk"
    assert ActivityParameter.get(name="J", group="G").formula == "F + Dirk * 2"


@bw2test
def test_parameters_rename_activity_parameter_group_exchange():
    """ Rename 'D' from group 'A' updates ParameterizedExchange and
    underlying exchange.
    """
    db = Database("B")
    db.register()
    ActivityParameter.create(
        group="A", database="B", code="C", name="D", formula="2 ** 3", amount=1,
    )
    a = db.new_activity(code="newcode", name="new activity")
    a.save()
    a.new_exchange(amount=1, input=a, type="production", formula="D + 2").save()
    parameters.add_exchanges_to_group("A", a)
    ActivityParameter.recalculate_exchanges("A")

    param = ActivityParameter.get(name="D", group="A")
    parameters.rename_activity_parameter(param, "Correct", True)
    assert ParameterizedExchange.get(group="A").formula == "Correct + 2"
    exc = next(iter(a.production()))
    assert exc.amount == 10
    assert exc.get("formula") == "Correct + 2"


@bw2test
def test_parameters_rename_activity_parameter_order_exchange():
    """ Rename 'D' from group 'A' updates ParameterizedExchange and
    underlying exchange in group 'G'
    """
    db = Database("K")
    db.register()
    ActivityParameter.create(
        group="A", database="K", code="C", name="D", formula="2 ** 3", amount=1,
    )
    a = db.new_activity(code="newcode", name="new activity")
    a.save()
    a.new_exchange(amount=1, input=a, type="production", formula="D + 2").save()
    Group.create(name="G", order=["A"], fresh=False)
    parameters.add_exchanges_to_group("G", a)
    ActivityParameter.recalculate_exchanges("G")

    param = ActivityParameter.get(name="D", group="A")
    parameters.rename_activity_parameter(param, "Correct", update_dependencies=True)

    assert ParameterizedExchange.get(group="G").formula == "Correct + 2"
    exc = next(iter(a.production()))
    assert exc.amount == 10
    assert exc.get("formula") == "Correct + 2"


@bw2test
def test_parameters_add_to_group_empty():
    db = Database("example")
    db.register()
    assert not len(parameters)
    assert not len(db)
    assert not Group.select().count()
    a = db.new_activity(code="A", name="An activity",)
    a.save()
    assert parameters.add_to_group("my group", a) is None
    assert Group.get(name="my group")
    assert not len(parameters)


@bw2test
def test_parameters_add_to_group():
    db = Database("example")
    db.register()
    assert not len(parameters)
    assert not len(db)
    assert not Group.select().count()

    ActivityParameter.create(
        group="my group", database="example", name="bye bye", code="A", amount=1,
    )

    a = db.new_activity(
        code="A",
        name="An activity",
        parameters=[
            {"amount": 4, "name": "one", "foo": "bar"},
            {"amount": 42, "name": "two", "formula": "this + that"},
        ],
    )
    a.save()
    assert "parameters" in get_activity(("example", "A"))

    assert parameters.add_to_group("my group", a) == 2
    assert Group.get(name="my group")
    assert (
        not ActivityParameter.select()
        .where(ActivityParameter.name == "bye bye")
        .count()
    )
    expected = (
        ("one", 4, None, {"foo": "bar"}),
        ("two", 42, "this + that", {}),
    )
    for ap in ActivityParameter.select():
        assert (ap.name, ap.amount, ap.formula, ap.data) in expected
    assert "parameters" not in get_activity(("example", "A"))


@bw2test
def test_parameters_remove_from_group():
    db = Database("example")
    db.register()
    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(amount=0, input=b, type="technosphere", formula="bar + 4").save()
    activity_data = [
        {
            "name": "reference_me",
            "formula": "sqrt(25)",
            "database": "example",
            "code": "B",
        },
        {
            "name": "bar",
            "formula": "reference_me + 2",
            "database": "example",
            "code": "A",
        },
    ]
    parameters.new_activity_parameters(activity_data, "my group")
    parameters.add_exchanges_to_group("my group", a)
    assert not get_activity(("example", "A")).get("parameters")
    assert ActivityParameter.select().count() == 2
    assert ParameterizedExchange.select().count() == 1

    parameters.remove_from_group("my group", a)
    assert ActivityParameter.select().count() == 1
    assert not ParameterizedExchange.select().count()
    assert get_activity(("example", "A"))["parameters"]


@bw2test
def test_parameters_save_restore_exchange_amount():
    """ The original amount of the exchange is restored when it is no
    longer parameterized.
    """
    db = Database("example")
    db.register()
    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(amount=5, input=b, type="technosphere", formula="bing + 5").save()

    activity_data = [
        {"name": "bing", "amount": "7", "database": "example", "code": "A",}
    ]
    parameters.new_activity_parameters(activity_data, "calculate")
    parameters.add_exchanges_to_group("calculate", a)
    # The original amount and current amount is 5
    for exc in a.exchanges():
        assert exc["amount"] == 5
        assert "original_amount" in exc and exc["original_amount"] == 5

    ActivityParameter.recalculate_exchanges("calculate")
    # Parameterization has caused the amount to change.
    for exc in a.exchanges():
        assert exc["amount"] == 12
        assert "original_amount" in exc

    # Remove parameterization from the activity, restoring the original amount
    parameters.remove_from_group("calculate", a)
    for exc in a.exchanges():
        assert exc["amount"] == 5
        assert "original_amount" not in exc


@bw2test
def test_parameters_save_keep_changed_exchange_amount():
    db = Database("example")
    db.register()
    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(amount=5, input=b, type="technosphere", formula="bing + 5").save()

    activity_data = [
        {"name": "bing", "amount": "7", "database": "example", "code": "A",}
    ]
    parameters.new_activity_parameters(activity_data, "calculate")
    parameters.add_exchanges_to_group("calculate", a)
    # The original amount and current amount is 5
    for exc in a.exchanges():
        assert exc["amount"] == 5
        assert "original_amount" in exc and exc["original_amount"] == 5

    ActivityParameter.recalculate_exchanges("calculate")
    # Parameterization has caused the amount to change.
    for exc in a.exchanges():
        assert exc["amount"] == 12
        assert "original_amount" in exc

    # Remove parameterization from the activity, keeping the changed amount
    parameters.remove_from_group("calculate", a, restore_amounts=False)
    for exc in a.exchanges():
        assert exc["amount"] == 12
        assert "original_amount" in exc
