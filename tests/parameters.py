# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import bw2test
from bw2data import parameters, projects, Database
from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    GroupDependency,
    Group,
    ProjectParameter,
)
from bw2parameters.errors import MissingName
from peewee import IntegrityError
import pytest
import time


######################
### Project parameters
######################

@bw2test
def test_project_parameters():
    assert not len(parameters)
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    assert obj.name == "foo"
    assert obj.amount == 3.14
    assert obj.data == {'uncertainty type': 0}
    assert str(obj)
    assert isinstance(str(obj), str)
    with pytest.raises(TypeError):
        obj < 0
    assert Group.get(name='project')
    assert not Group.get(name='project').fresh

@bw2test
def test_project_parameters_ordering():
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    assert not (obj < obj)
    another = ProjectParameter.create(
        name="bar",
        formula="2 * foo",
    )
    assert another < obj

@bw2test
def test_project_parameters_dict():
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    expected = {
        'name': 'foo',
        'amount': 3.14,
        'uncertainty type': 0,
    }
    assert obj.dict == expected


@bw2test
def test_project_parameters_load():
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    another = ProjectParameter.create(
        name="bar",
        formula="2 * foo",
    )
    expected = {
        'foo': {'amount': 3.14, 'uncertainty type': 0},
        'bar': {'formula': '2 * foo'}
    }
    assert ProjectParameter.load() == expected

@bw2test
def test_project_parameters_static():
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    another = ProjectParameter.create(
        name="bar",
        formula="2 * foo",
    )
    assert ProjectParameter.static() == {'foo': 3.14, 'bar': None}
    assert ProjectParameter.static(['foo']) == {'foo': 3.14}
    ProjectParameter.recalculate()
    assert ProjectParameter.static() == {'foo': 3.14, 'bar': 2 * 3.14}
    assert ProjectParameter.static(['bar']) == {'bar': 2 * 3.14}

@bw2test
def test_project_parameters_expired():
    assert not ProjectParameter.expired()
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    assert ProjectParameter.expired()
    ProjectParameter.recalculate()
    assert not ProjectParameter.expired()

@bw2test
def test_project_parameters_recalculate():
    ProjectParameter.recalculate()
    Group.create(name="project")
    ProjectParameter.recalculate()
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    another = ProjectParameter.create(
        name="bar",
        formula="2 * foo",
    )
    ProjectParameter.recalculate()
    obj = ProjectParameter.get(name="bar")
    assert obj.amount == 2 * 3.14

@bw2test
def test_project_parameters_expire_downstream():
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    Group.create(name="bar")
    GroupDependency.create(group="bar", depends="project")
    assert Group.get(name="bar").fresh
    ProjectParameter.recalculate()
    assert not Group.get(name="bar").fresh

@bw2test
def test_project_triggers():
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    first = Group.get(name="project").updated
    time.sleep(1.1)
    another = ProjectParameter.create(
        name="bar",
        formula="2 * foo",
    )
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
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    with pytest.raises(IntegrityError):
        ProjectParameter.create(
            name="foo",
            amount=7,
        )

#######################
### Database parameters
#######################

@bw2test
def test_create_database_parameters():
    assert not len(parameters)
    obj = DatabaseParameter.create(
        database='bar',
        name="foo",
        amount=3.14,
    )
    assert obj.name == "foo"
    assert obj.database == "bar"
    assert obj.amount == 3.14
    assert str(obj)
    assert isinstance(str(obj), str)
    assert len(parameters)
    assert Group.get(name='bar')
    assert not Group.get(name='bar').fresh

@bw2test
def test_database_parameters_expired():
    assert not DatabaseParameter.expired('bar')
    DatabaseParameter.create(
        database='bar',
        name="foo",
        amount=3.14,
    )
    assert DatabaseParameter.expired('bar')

@bw2test
def test_database_parameters_dict():
    obj = DatabaseParameter.create(
        database='bar',
        name="foo",
        amount=3.14,
    )
    expected = {
        'database': 'bar',
        'name': 'foo',
        'amount': 3.14,
    }
    assert obj.dict == expected

@bw2test
def test_database_parameters_load():
    DatabaseParameter.create(
        database='bar',
        name="foo",
        amount=3.14,
    )
    DatabaseParameter.create(
        database='bar',
        name="baz",
        formula="foo + baz"
    )
    expected = {
        'foo': {'database': 'bar', 'amount': 3.14},
        'baz': {'database': 'bar', 'formula': "foo + baz"}
    }
    assert DatabaseParameter.load("bar") == expected

@bw2test
def test_database_parameters_static():
    DatabaseParameter.create(
        database='bar',
        name="foo",
        amount=3.14,
    )
    DatabaseParameter.create(
        database='bar',
        name="baz",
        amount=7,
        formula="foo + baz"
    )
    expected = {
        'foo': 3.14,
        'baz': 7
    }
    assert DatabaseParameter.static("bar") == expected
    assert DatabaseParameter.static("bar", only=['baz']) == {'baz': 7}

@bw2test
def test_database_parameters_check():
    with pytest.raises(IntegrityError):
        DatabaseParameter.create(
            database='project',
            name="foo",
            amount=3.14,
        )

@bw2test
def test_database_triggers():
    obj = DatabaseParameter.create(
        database="A",
        name="foo",
        amount=3.14,
    )
    first = Group.get(name="A").updated
    time.sleep(1.1)
    another = DatabaseParameter.create(
        database="A",
        name="bar",
        formula="2 * foo",
    )
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
        database="A",
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    with pytest.raises(IntegrityError):
        DatabaseParameter.create(
            database="A",
            name="foo",
            amount=7,
        )

@bw2test
def test_update_database_parameters():
    assert not Group.select().count()
    assert not GroupDependency.select().count()

    DatabaseParameter.create(
        database='A',
        name="B",
        amount=5,
    )
    o = DatabaseParameter.create(
        database='A',
        name="C",
        formula="B * 2 + foo",
    )
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
    ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    ProjectParameter.create(
        name="bar",
        formula="2 * foo",
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

#######################
### Activity parameters
#######################

@bw2test
def test_create_activity_parameter():
    assert not ActivityParameter.select().count()
    obj = ActivityParameter.create(
        group="A",
        database="B",
        code="C",
        name="D",
        amount=3.14
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
    assert Group.get(name='A')
    assert not Group.get(name='A').fresh

@bw2test
def test_activity_parameter_expired():
    pass

@bw2test
def test_activity_parameter_dict():
    pass

@bw2test
def test_activity_parameter_load():
    pass

@bw2test
def test_activity_parameter_static():
    pass

@bw2test
def test_activity_parameter_recalculate():
    pass

@bw2test
def test_activity_parameter_crossdatabase_triggers():
    ActivityParameter.create(
        group="A",
        database="B",
        name="C",
        code="D"
    )
    with pytest.raises(IntegrityError):
        ActivityParameter.create(
            group="A",
            database="E",
            name="F",
            code="G"
        )
    with pytest.raises(IntegrityError):
        a = ActivityParameter.get(name="C")
        a.database = "E"
        a.save()
    with pytest.raises(IntegrityError):
        ActivityParameter.update(database="C").execute()

@bw2test
def test_activity_parameter_checks_uniqueness_constraints():
    pass

@bw2test
def test_activity_parameter_checks():
    with pytest.raises(IntegrityError):
        ActivityParameter.create(
            group="project",
            database="E",
            name="F",
            code="G"
        )
    with pytest.raises(IntegrityError):
        ActivityParameter.create(
            group="E",
            database="E",
            name="F",
            code="G"
        )

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
    o = Group.create(
        name="one",
        order=["C", "project", "B", "D", "A"]
    )
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

######################
### Parameters manager
######################
