# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import bw2test
from bw2data.database import DatabaseChooser
from bw2data import mapping, geomapping, databases, methods, projects
import pytest


@pytest.fixture
@bw2test
def activity():
    database = DatabaseChooser("db")
    database.write({
        ("db", "a"): {
            'exchanges': [{
                'input': ("db", "a"),
                'amount': 2,
                'type': 'production',
            }, {
                'input': ("db", "b"),
                'amount': 3,
                'type': 'technosphere',
            }, {
                'input': ("db", "c"),
                'amount': 4,
                'type': 'biosphere',
            }],
            'name': 'a'
        },
        ("db", "b"): {'name': 'b'},
        ("db", "c"): {'name': 'c', 'type': 'biosphere'},
        ("db", "d"): {
            'name': 'd',
            'exchanges': [{
                'input': ("db", "a"),
                'amount': 5,
                'type': 'technosphere'
            }]
        },
    })
    return database.get("a")

def test_setup_clean(activity):
    assert len(databases) == 1
    assert list(methods) == []
    assert len(mapping) == 4
    assert len(geomapping) == 1  # GLO
    assert "GLO" in geomapping
    assert len(projects) == 1  # Default project
    assert "default" in projects

def test_production(activity):
    assert len(list(activity.production())) == 1
    assert len(activity.production()) == 1
    exc = list(activity.production())[0]
    assert exc['amount'] == 2

def test_biosphere(activity):
    assert len(list(activity.biosphere())) == 1
    assert len(activity.biosphere()) == 1
    exc = list(activity.biosphere())[0]
    assert exc['amount'] == 4

def test_technosphere(activity):
    assert len(list(activity.technosphere())) == 1
    assert len(activity.technosphere()) == 1
    exc = list(activity.technosphere())[0]
    assert exc['amount'] == 3

def test_upstream(activity):
    assert len(list(activity.upstream())) == 1
    assert len(activity.upstream()) == 1
    exc = list(activity.upstream())[0]
    assert exc['amount'] == 5
