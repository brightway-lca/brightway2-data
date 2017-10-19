# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import bw2test
from bw2data import parameters, projects
from bw2data.parameters import DatabaseParameter, ParameterGroup, ProjectParameter, WarningLabel
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
    assert isinstance(repr(obj), str)
    assert isinstance(str(obj), str)
    expected = {
        'name': 'foo',
        'formula': None,
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
