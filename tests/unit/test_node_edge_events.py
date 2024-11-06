import json

from snowflake import SnowflakeGenerator as sfg

from bw2data import revisions, get_node
from bw2data.backends.schema import ActivityDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.tests import bw2test


@bw2test
def test_node_revision_expected_format_create():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()

    assert projects.dataset.revision is not None
    with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
        revision = json.load(f)

    expected = {
        "data": [
            {
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "new_type": "dict",
                            "new_value": {
                                "code": "A",
                                "database": "db",
                                "location": "GLO",
                                "name": "A",
                            },
                            "old_type": "NoneType",
                        }
                    }
                },
                "id": node.id,
                "type": "lci_node",
            }
        ],
        "metadata": {
            "authors": "Anonymous",
            "description": "No description",
            "parent_revision": None,
            "revision": projects.dataset.revision,
            "title": "Untitled revision",
        },
    }

    assert revision == expected


@bw2test
def test_node_revision_apply_create():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    database = DatabaseChooser("db")
    database.register()

    revision_id = next(sfg(0))
    revision = {
        "data": [
            {
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "new_type": "dict",
                            "new_value": {
                                "code": "A",
                                "database": "db",
                                "location": "GLO",
                                "name": "A",
                            },
                            "old_type": "NoneType",
                        }
                    }
                },
                "id": 1,
                "type": "lci_node",
            }
        ],
        "metadata": {
            "authors": "Anonymous",
            "description": "No description",
            "parent_revision": None,
            "revision": revision_id,
            "title": "Untitled revision",
        },
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id
    node = get_node(code="A")
    assert node['name'] == 'A'
    assert len(database) == 1


@bw2test
def test_node_revision_expected_format_delete():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()

    parent = projects.dataset.revision
    assert parent is not None
    node.delete()

    with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
        revision = json.load(f)

    expected = {
        "data": [
            {
                "change_type": "delete",
                "delta": {
                    "type_changes": {
                        "root": {
                            "new_type": "NoneType",
                            "new_value": None,
                            "old_type": "dict",
                        }
                    }
                },
                "id": node.id,
                "type": "lci_node",
            }
        ],
        "metadata": {
            "authors": "Anonymous",
            "description": "No description",
            "parent_revision": parent,
            "revision": projects.dataset.revision,
            "title": "Untitled revision",
        },
    }

    assert revision == expected


@bw2test
def test_node_revision_apply_delete():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    assert len(database) == 1

    revision_id = next(sfg(0))

    revision = {
        "data": [
            {
                "change_type": "delete",
                "delta": {
                    "type_changes": {
                        "root": {
                            "new_type": "NoneType",
                            "new_value": None,
                            "old_type": "dict",
                        }
                    }
                },
                "id": node.id,
                "type": "lci_node",
            }
        ],
        "metadata": {
            "authors": "Anonymous",
            "description": "No description",
            "parent_revision": projects.dataset.revision,
            "revision": revision_id,
            "title": "Untitled revision",
        },
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id
    assert len(database) == 0


@bw2test
def test_node_revision_expected_format_update():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()

    parent = projects.dataset.revision
    assert parent is not None
    node['name'] = 'B'
    node.save()

    with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
        revision = json.load(f)

    expected = {
        "data": [
            {
                "change_type": "update",
                "delta": {'values_changed': {"root['name']": {'new_value': 'B'}}},
                "id": node.id,
                "type": "lci_node",
            }
        ],
        "metadata": {
            "authors": "Anonymous",
            "description": "No description",
            "parent_revision": parent,
            "revision": projects.dataset.revision,
            "title": "Untitled revision",
        },
    }

    assert revision == expected


@bw2test
def test_node_revision_apply_update():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()

    revision_id = next(sfg(0))

    revision = {
        "data": [
            {
                "change_type": "update",
                "delta": {'values_changed': {"root['name']": {'new_value': 'B'}}},
                "id": node.id,
                "type": "lci_node",
            }
        ],
        "metadata": {
            "authors": "Anonymous",
            "description": "No description",
            "parent_revision": projects.dataset.revision,
            "revision": revision_id,
            "title": "Untitled revision",
        },
    }

    from pprint import pprint
    pprint(revision)

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id
    assert len(database) == 1
    node = get_node(code="A")
    assert node['name'] == 'B'


@bw2test
def test_writes_activity_evt_on_db_write():
    """
        key=("db", "foo"),
        name="foo",
        type="production",
        location="bar",
    )
    """
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    previous_revision = projects.dataset.revision
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "foo"): {
                "exchanges": [
                    {
                        "input": ("db", "foo"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "baz",
            },
        }
    )
    activity = database.get("foo")
    assert activity["name"] == "baz"
    activity["foo"] = "bar"
    activity.save()
    act = DatabaseChooser("db").get("foo")
    assert act["foo"] == "bar"
    assert projects.dataset.revision != previous_revision


@bw2test
def test_writes_exchange_evt_on_save():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    previous_revision = projects.dataset.revision
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "foo"): {
                "exchanges": [
                    {
                        "input": ("db", "foo"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "bar",
            },
        }
    )
    activity = database.get("foo")
    new_exchange = activity.new_edge(
        input=("db", "foo"), amount=0.5, type="production", unit="kilogram"
    )
    new_exchange.save()
    assert projects.dataset.revision != previous_revision
