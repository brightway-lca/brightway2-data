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
from peewee import IntegrityError
import pytest
import time


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
    assert repr(obj)
    assert isinstance(str(obj), str)
    assert not (obj < obj)
    with pytest.raises(TypeError):
        obj < 0
    assert Group.get(name='project')
    assert not Group.get(name='project').fresh
    expected = {
        'name': 'foo',
        'amount': 3.14,
        'uncertainty type': 0,
    }
    assert obj.dict == expected
    another = ProjectParameter.create(
        name="bar",
        formula="2 * foo",
    )
    assert another < obj

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
def test_create_database_parameters():
    assert not len(parameters)
    DatabaseParameter.create(
        database='bar',
        name="foo",
        amount=3.14,
    )
    assert len(parameters)
    assert DatabaseParameter.expired('bar')

@bw2test
def test_update_database_parameters():
    assert not Group.select().count()
    assert not GroupDependency.select().count()

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
    DatabaseParameter.create(
        database='A',
        name="B",
        amount=5,
    )
    DatabaseParameter.create(
        database='A',
        name="C",
        formula="foo + bar + B",
    )

    obj = DatabaseParameter.get(name="C")
    assert obj.amount != 3.14 * 3 + 5
    assert Group.get(name="A")
    with pytest.raises(GroupDependency.DoesNotExist):
        GroupDependency.get(group="A", depends="project")

    DatabaseParameter.recalculate("A")
    assert GroupDependency.get(group="A", depends="project")
    assert Group.get(name="A")
    assert Group.get(name="project")
    obj = DatabaseParameter.get(name="C")
    assert obj.amount == 3.14 * 3 + 5

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
def test_group_dependency():
    d = GroupDependency.create(group="foo", depends="bar")
    with pytest.raises(IntegrityError):
        GroupDependency.create(group="foo", depends="bar")
    GroupDependency.create(group="foo", depends="baz")
    with pytest.raises(ValueError):
        GroupDependency.create(group="project", depends="foo")
    Database("A").register()
    GroupDependency.create(group="A", depends="project")
    with pytest.raises(ValueError):
        GroupDependency.create(group="A", depends="foo")
