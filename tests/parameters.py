# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import bw2test
from bw2data import parameters, projects
from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    GroupDependency,
    ParameterGroup,
    ProjectParameter,
    WarningLabel,
)
import pytest


@bw2test
def test_create_project_parameters():
    assert not len(parameters)
    assert not ProjectParameter.expired()
    ProjectParameter.create(
        name="foo",
        amount=3.14,
    )
    assert len(parameters)
    assert ProjectParameter.expired()

@bw2test
def test_compare_project_parameter_error():
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
    )
    with pytest.raises(TypeError):
        obj < 0

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
def test_project_parameters_recalculate():
    obj = ProjectParameter.create(
        name="foo",
        amount=3.14,
        data={'uncertainty type': 0}
    )
    another = ProjectParameter.create(
        name="bar",
        formula="2 * foo",
    )
    assert ProjectParameter.expired()
    ProjectParameter.recalculate()
    obj = ProjectParameter.get(name="bar")
    assert obj.amount == 2 * 3.14
    assert not ProjectParameter.expired()

@bw2test
def test_create_database_parameters():
    assert not len(parameters)
    assert not list(DatabaseParameter.expired())
    DatabaseParameter.create(
        database='bar',
        name="foo",
        amount=3.14,
    )
    assert len(parameters)
    assert list(DatabaseParameter.expired()) == ['bar']

@bw2test
def test_update_database_parameters():
    assert not ParameterGroup.select().count()
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
    assert ParameterGroup.get(name="project")
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
    assert WarningLabel.select().count() == 2
    assert ParameterGroup.get(name="db_A")
    with pytest.raises(GroupDependency.DoesNotExist):
        GroupDependency.get(group="db_A", depends="project")

    DatabaseParameter.recalculate("A")
    assert GroupDependency.get(group="db_A", depends="project")
    assert ParameterGroup.get(name="db_A")
    assert ParameterGroup.get(name="project")
    obj = DatabaseParameter.get(name="C")
    assert obj.amount == 3.14 * 3 + 5
    assert not WarningLabel.select().count()
