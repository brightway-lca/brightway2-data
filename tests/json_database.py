# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import bw2test
from .fixtures import food2, biosphere as biosphere_data
from bw2data import config, databases, projects
from bw2data.backends.json import JSONDatabase, SynchronousJSONDict
from bw2data.backends.json.proxies import Activity
from bw2data.serialization import JsonWrapper, JsonSanitizer
import json
import pytest
import os
import shutil
import unittest


### JSON database tests

@pytest.fixture
@bw2test
def biosphere():
    d = JSONDatabase("biosphere")
    d.write(biosphere_data)
    return d

def test_get(biosphere):
    activity = biosphere.get('1')
    assert isinstance(activity, Activity)
    assert activity['name'] == 'an emission'

def test_iter(biosphere):
    activity = next(iter(biosphere))
    assert isinstance(activity, Activity)
    assert activity['name'] in ('an emission', 'another emission')

def test_get_random(biosphere):
    activity = biosphere.random()
    assert isinstance(activity, Activity)
    assert activity['name'] in ('an emission', 'another emission')

def test_load_write(biosphere):
    jd = JSONDatabase("foo")
    jd.register()
    key = ("foobar", "spaghetti")
    data = {
        key: {
            'categories': ['stuff', 'meals'],
            'code': 1,
            'exchanges': [],
            'location': 'CA',
            'name': 'early lunch',
            'type': 'process',
            'unit': 'kg'
        }
    }
    jd.write(data)
    loaded = jd.load()
    data[key]["key"] = key
    assert isinstance(loaded, SynchronousJSONDict)
    assert loaded[key] == data[key]

@bw2test
def test_register_creates_directory():
    assert not os.path.exists(os.path.join(
        projects.dir, "intermediate", "foo"
    ))
    JSONDatabase("foo").register()
    assert os.path.exists(os.path.join(
        projects.dir,
        "intermediate",
        "foo.acbd18db4cc2f85cedef654fccc4a4d8"
    ))
    assert os.path.isdir(os.path.join(
        projects.dir,
        "intermediate",
        "foo.acbd18db4cc2f85cedef654fccc4a4d8"
    ))

@bw2test
def test_write_sets_number_metadata():
    db = JSONDatabase("foo")
    db.write({("foo", str(x)): {} for x in range(10)})
    assert databases["foo"]["number"] == 10

def test_load_as_dict(biosphere):
    d = JSONDatabase("food")
    d.register()
    d.write(food2, process=False)
    data = d.load(as_dict=True)
    assert isinstance(data, dict)
    data = d.load()
    assert not isinstance(data, dict)

def test_db_is_json_serializable(biosphere):
    d = JSONDatabase("food")
    d.register()
    d.write(food2, process=False)
    data = d.load(as_dict=True)
    JsonWrapper.dumps(JsonSanitizer.sanitize(data))

def test_database_len(biosphere):
    assert len(biosphere) == 2

@bw2test
def test_change_project():
    d = JSONDatabase("biosphere")
    d.write(biosphere_data)
    assert ("biosphere", '1') in d
    assert projects._json_backend_cache
    projects.set_current("something else")
    print("Switched projects")
    print("Config.cache:\n", config.cache)
    d = JSONDatabase("biosphere")
    assert not projects._json_backend_cache
    assert not len(d)
    assert ("biosphere", '1') not in d


### Synchronous JSON dict

@pytest.fixture
@bw2test
def sync_json_setup():
    fp = projects.request_directory("futball")
    js = SynchronousJSONDict(fp, "futball")
    return fp, js

def test_write_on_modification(sync_json_setup):
    fp, js = sync_json_setup
    js[("futball", "brazil")] = {"foot": "ball"}
    assert ("brazil.6e5fa4d9c48ca921c0a2ce1e64c9ae6f.json"
            in os.listdir(fp))
    with open(os.path.join(
            fp,
            "brazil.6e5fa4d9c48ca921c0a2ce1e64c9ae6f.json"
            )) as f:
        data = json.load(f)
    assert data == {'foot': 'ball', 'key': ['futball', 'brazil']}

def test_non_dict_raises_error(sync_json_setup):
    fp, js = sync_json_setup
    with pytest.raises(AssertionError):
        js[("futball", "brazil")] = "foot"

def test_load_makes_tuples(sync_json_setup):
    fp, js = sync_json_setup
    ds = {'exchanges': [{'input': ('foo', 'bar')}]}
    js[('futball', 'brazil')] = ds
    expected = {'key': ('futball', 'brazil'), 'exchanges': [{'input': ('foo', 'bar')}]}
    assert js[('futball', 'brazil')] == expected
    # Force load from file
    js.cache = {}
    assert js[('futball', 'brazil')] == expected
    assert [1] != (1,)

def test_set_get(sync_json_setup):
    fp, js = sync_json_setup
    ds = {'foo': 'bar'}
    js[('futball', 'spain')] = ds
    assert js[('futball', 'spain')]['foo'] == 'bar'
    # Force load from file
    js.cache = {}
    assert js[('futball', 'spain')]['foo'] == 'bar'

def test_delete(sync_json_setup):
    fp, js = sync_json_setup
    key = ('futball', 'germany')
    js[key] = {}
    assert key in js
    del js[key]
    assert key not in js
    with pytest.raises(KeyError):
        js[key]
    with pytest.raises(KeyError):
        del js[key]

def test_iter_keys_values_items(sync_json_setup):
    fp, js = sync_json_setup
    keys = {('x', str(x)) for x in [1,2,3,4]}
    for key in keys:
        js[key] = {}
    for key in js:
        assert key in keys

def test_keys(sync_json_setup):
    fp, js = sync_json_setup
    keys = {('x', str(x)) for x in [1,2,3,4]}
    for key in keys:
        js[key] = {}
    assert sorted(keys) == sorted(js.keys())
    # Force load from file
    js.cache = {}
    assert sorted(keys) == sorted(js.keys())

def test_values(sync_json_setup):
    fp, js = sync_json_setup
    keys = {('x', str(x)) for x in [1,2,3,4]}
    for key in keys:
        js[key] = {}
    found = [dict(o) for o in js.values()]
    for obj in [{'key': key} for key in keys]:
        assert obj in found
    # Force load from file
    js.cache = {}
    found = [dict(o) for o in js.values()]
    for obj in [{'key': key} for key in keys]:
        assert obj in found

def test_items(sync_json_setup):
    fp, js = sync_json_setup
    keys = {('x', str(x)) for x in [1,2,3,4]}
    for key in keys:
        js[key] = {}
    for k, v in js.items():
        assert k in keys
        assert list(v.keys()) == ['key']
    # Force load from file
    js.cache = {}
    for k, v in js.items():
        assert k in keys
        assert list(v.keys()) == ['key']

def test_len(sync_json_setup):
    fp, js = sync_json_setup
    keys = {('x', str(x)) for x in [1,2,3,4]}
    for key in keys:
        js[key] = {}
    assert len(js) == 4

def test_contains(sync_json_setup):
    fp, js = sync_json_setup
    keys = {('x', str(x)) for x in [1,2,3,4]}
    for key in keys:
        js[key] = {}
    for key in keys:
        assert key in js

def test_keys_must_be_strings(sync_json_setup):
    fp, js = sync_json_setup
    with pytest.raises(TypeError):
        js[('x', 1)] = {}

def test_filename_illegal_characters(sync_json_setup):
    fp, js = sync_json_setup
    js[('futball', '!')] = {}
    assert js[('futball', '!')] == {'key': ('futball', '!')}
