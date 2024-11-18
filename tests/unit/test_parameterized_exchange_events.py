import json

from bw2data.database import DatabaseChooser
from bw2data.parameters import ActivityParameter, ParameterizedExchange
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test


@bw2test
def test_parameterized_exchange_revision_expected_format_create(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    edge = node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo")
    edge.save()
    ActivityParameter.insert_dummy("test-group", (node["database"], node["code"]))

    assert not ParameterizedExchange.select().count()
    assert projects.dataset.revision is None

    projects.dataset.set_sourced()

    pe = ParameterizedExchange.create(
        group="test-group",
        exchange=edge.id,
        formula="1 * 2 + 3",
    )
    assert pe.id > 1e6
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
                "type": "parameterized_exchange",
                "id": pe.id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "id": pe.id,
                                "group": "test-group",
                                "formula": "1 * 2 + 3",
                                "exchange": edge.id,
                            },
                        }
                    }
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_parameterized_exchange_revision_apply_create(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    edge = node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo")
    edge.save()
    ActivityParameter.insert_dummy("test-group", (node["database"], node["code"]))

    assert projects.dataset.revision is None

    revision_id = next(snowflake_id_generator)
    pe_id = next(snowflake_id_generator)
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
                "type": "parameterized_exchange",
                "id": pe_id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "id": pe_id,
                                "group": "test-group",
                                "formula": "1 * 2 + 3",
                                "exchange": edge.id,
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

    assert ParameterizedExchange.select().count() == 1
    pe = ParameterizedExchange.get(id=pe_id)
    assert pe.group == "test-group"
    assert pe.formula == "1 * 2 + 3"
    assert pe.exchange == edge.id


@bw2test
def test_parameterized_exchange_revision_expected_format_update(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    edge = node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo")
    edge.save()
    ActivityParameter.insert_dummy("test-group", (node["database"], node["code"]))
    pe = ParameterizedExchange.create(
        group="test-group",
        exchange=edge.id,
        formula="1 * 2 + 3",
    )

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    pe.formula = "7 / 3.141"
    pe.save()

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
                "type": "parameterized_exchange",
                "id": pe.id,
                "change_type": "update",
                "delta": {"values_changed": {"root['formula']": {"new_value": "7 / 3.141"}}},
            }
        ],
    }

    assert revision == expected


@bw2test
def test_parameterized_exchange_revision_apply_update(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    edge = node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo")
    edge.save()
    ActivityParameter.insert_dummy("test-group", (node["database"], node["code"]))
    pe = ParameterizedExchange.create(
        group="test-group",
        exchange=edge.id,
        formula="1 * 2 + 3",
    )

    assert projects.dataset.revision is None

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
                "type": "parameterized_exchange",
                "id": pe.id,
                "change_type": "update",
                "delta": {"values_changed": {"root['formula']": {"new_value": "7 / 3.141"}}},
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)

    assert ParameterizedExchange.select().count() == 1
    dp = ParameterizedExchange.get(id=pe.id)
    assert dp.formula == "7 / 3.141"
    assert pe.group == "test-group"
    assert pe.exchange == edge.id


@bw2test
def test_parameterized_exchange_revision_expected_format_delete(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    edge = node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo")
    edge.save()
    ActivityParameter.insert_dummy("test-group", (node["database"], node["code"]))
    pe = ParameterizedExchange.create(
        group="test-group",
        exchange=edge.id,
        formula="1 * 2 + 3",
    )

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    pe.delete_instance()

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
                "type": "parameterized_exchange",
                "id": pe.id,
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
def test_parameterized_exchange_revision_apply_delete(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    edge = node.new_edge(input=other, type="technosphere", amount=0.1, arbitrary="foo")
    edge.save()
    ActivityParameter.insert_dummy("test-group", (node["database"], node["code"]))
    pe = ParameterizedExchange.create(
        group="test-group",
        exchange=edge.id,
        formula="1 * 2 + 3",
    )

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
                "type": "parameterized_exchange",
                "id": pe.id,
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
    assert not ParameterizedExchange.select().count()
