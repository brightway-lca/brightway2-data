import json

import pytest

from bw2data import get_node
from bw2data.backends.schema import ExchangeDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test


@bw2test
def test_edge_revision_expected_format_create():
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo").save()

    assert projects.dataset.revision is not None
    with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
        revision = json.load(f)

    for edge in node.exchanges():
        continue

    expected = {
        "metadata": {
            "parent_revision": None,
            "revision": projects.dataset.revision,
            "authors": "Anonymous",
            "title": "Untitled revision",
            "description": "No description",
        },
        "data": [
            {
                "type": "lci_edge",
                "id": edge._document.id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "data": {
                                    "output": ["db", "A"],
                                    "input": ["db", "B"],
                                    "type": "technosphere",
                                    "amount": 0.1,
                                    "arbitrary": "foo",
                                },
                                "input_database": "db",
                                "input_code": "B",
                                "output_database": "db",
                                "output_code": "A",
                                "type": "technosphere",
                            },
                        }
                    }
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_edge_revision_apply_create(num_revisions):
    projects.set_current("activity-event")
    assert projects.dataset.revision is None

    database = DatabaseChooser("db")
    database.register()

    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()

    revision_id = next(snowflake_id_generator)
    edge_id = next(snowflake_id_generator)
    revision = {
        "metadata": {
            "parent_revision": projects.dataset.revision,
            "revision": revision_id,
            "authors": "Anonymous",
            "title": "Untitled revision",
            "description": "No description",
        },
        "data": [
            {
                "type": "lci_edge",
                "id": edge_id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "data": {
                                    "output": ["db", "A"],
                                    "input": ["db", "B"],
                                    "type": "technosphere",
                                    "amount": 0.1,
                                    "arbitrary": "foo",
                                },
                                "input_database": "db",
                                "input_code": "B",
                                "output_database": "db",
                                "output_code": "A",
                                "type": "technosphere",
                            },
                        }
                    }
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)
    assert len(get_node(code="A").exchanges()) == 1
    for edge in get_node(code="A").exchanges():
        assert edge["arbitrary"] == "foo"
        assert edge._document.id == edge_id
        assert edge["amount"] == 0.1
        assert edge.output["code"] == "A"
        assert edge.input["code"] == "B"
    assert len(database) == 2


@bw2test
def test_edge_revision_expected_format_delete():
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()

    node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo").save()

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    for edge in node.edges():
        edge.delete()

    assert projects.dataset.revision is not None
    with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
        revision = json.load(f)

    expected = {
        "metadata": {
            "parent_revision": None,
            "revision": projects.dataset.revision,
            "authors": "Anonymous",
            "title": "Untitled revision",
            "description": "No description",
        },
        "data": [
            {
                "type": "lci_edge",
                "id": edge._document.id,
                "change_type": "delete",
                "delta": {
                    "type_changes": {
                        "root": {"old_type": "dict", "new_type": "NoneType", "new_value": None}
                    }
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_edge_revision_apply_delete(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()

    node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo").save()
    for edge in node.edges():
        pass

    assert ExchangeDataset.select().count() == 1
    assert len(get_node(code="A").exchanges()) == 1

    revision_id = next(snowflake_id_generator)
    revision = {
        "metadata": {
            "parent_revision": None,
            "revision": revision_id,
            "authors": "Anonymous",
            "title": "Untitled revision",
            "description": "No description",
        },
        "data": [
            {
                "type": "lci_edge",
                "id": edge._document.id,
                "change_type": "delete",
                "delta": {
                    "type_changes": {
                        "root": {"old_type": "dict", "new_type": "NoneType", "new_value": None}
                    }
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)
    assert ExchangeDataset.select().count() == 0
    assert len(get_node(code="A").exchanges()) == 0


@bw2test
def test_edge_revision_expected_format_modify(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    node.new_edge(input=other, type="technosphere", amount=0.1).save()

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    for edge in node.edges():
        edge["amount"] = 42
        edge["arbitrary"] = "foo"
        edge.save()

    assert num_revisions(projects) == 1
    assert projects.dataset.revision is not None
    with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
        revision = json.load(f)

    expected = {
        "metadata": {
            "parent_revision": None,
            "revision": projects.dataset.revision,
            "authors": "Anonymous",
            "title": "Untitled revision",
            "description": "No description",
        },
        "data": [
            {
                "type": "lci_edge",
                "id": edge._document.id,
                "change_type": "update",
                "delta": {
                    "dictionary_item_added": {
                        "root['data']['arbitrary']": "foo",
                    },
                    "type_changes": {
                        "root['data']['amount']": {
                            "old_type": "float",
                            "new_type": "int",
                            "new_value": 42,
                        }
                    },
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_edge_revision_apply_modify(num_revisions):
    projects.set_current("activity-event")
    assert projects.dataset.revision is None

    database = DatabaseChooser("db")
    database.register()

    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    node.new_edge(input=other, type="technosphere", amount=0.1).save()

    for edge in node.edges():
        pass

    revision_id = next(snowflake_id_generator)
    revision = {
        "metadata": {
            "parent_revision": None,
            "revision": revision_id,
            "authors": "Anonymous",
            "title": "Untitled revision",
            "description": "No description",
        },
        "data": [
            {
                "type": "lci_edge",
                "id": edge._document.id,
                "change_type": "update",
                "delta": {
                    "dictionary_item_added": {
                        "root['data']['arbitrary']": "foo",
                    },
                    "type_changes": {
                        "root['data']['amount']": {
                            "old_type": "float",
                            "new_type": "int",
                            "new_value": 42,
                        }
                    },
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)
    assert len(get_node(code="A").exchanges()) == 1
    for edge in get_node(code="A").exchanges():
        assert edge["arbitrary"] == "foo"
        assert edge["amount"] == 42
        assert edge.output["code"] == "A"
        assert edge.input["code"] == "B"
    assert len(database) == 2


@bw2test
def test_edge_mass_delete():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()

    database = DatabaseChooser("db")
    database.register()
    DatabaseChooser("db2").register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    node.new_edge(input=other, type="technosphere", amount=0.1).save()
    node.new_edge(input=other, type="production", amount=1.0).save()

    with pytest.raises(NotImplementedError):
        node.exchanges().delete()
