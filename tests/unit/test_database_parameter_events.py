import json

from bw2data.database import DatabaseChooser
from bw2data.parameters import DatabaseParameter
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test


@bw2test
def test_database_parameter_revision_expected_format_create(num_revisions):
    projects.set_current("activity-event")

    assert not DatabaseParameter.select().count()
    assert projects.dataset.revision is None

    DatabaseChooser("test-example").register()

    projects.dataset.set_sourced()

    dp = DatabaseParameter.create(
        database="test-example", name="example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"}
    )
    assert dp.id > 1e6
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
                "type": "database_parameter",
                "id": dp.id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "id": dp.id,
                                "database": "test-example",
                                "name": "example",
                                "formula": "1 * 2 + 3",
                                "amount": 5,
                                "data": {"foo": "bar"},
                            },
                        }
                    }
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_database_parameter_revision_apply_create(num_revisions):
    projects.set_current("activity-event")
    DatabaseChooser("test-example").register()
    assert projects.dataset.revision is None

    revision_id = next(snowflake_id_generator)
    dp_id = next(snowflake_id_generator)
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
                "type": "database_parameter",
                "id": dp_id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "id": dp_id,
                                "database": "test-example",
                                "name": "example",
                                "formula": "1 * 2 + 3",
                                "amount": 5,
                                "data": {"foo": "bar"},
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

    assert DatabaseParameter.select().count() == 1
    dp = DatabaseParameter.get(id=dp_id)
    assert dp.data == {"foo": "bar"}
    assert dp.database == "test-example"
    assert dp.amount == 5
    assert dp.formula == "1 * 2 + 3"
    assert dp.name == "example"


@bw2test
def test_database_parameter_revision_expected_format_update(num_revisions):
    projects.set_current("activity-event")

    DatabaseChooser("test-example").register()
    dp = DatabaseParameter.create(
        name="example", database="test-example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"}
    )

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    dp.name = "another"
    dp.amount = 7
    dp.save()

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
                "type": "database_parameter",
                "id": dp.id,
                "change_type": "update",
                "delta": {
                    "type_changes": {
                        "root['amount']": {"old_type": "float", "new_type": "int", "new_value": 7}
                    },
                    "values_changed": {"root['name']": {"new_value": "another"}},
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_database_parameter_revision_apply_update(num_revisions):
    projects.set_current("activity-event")

    DatabaseChooser("test-example").register()
    dp = DatabaseParameter.create(
        name="example", database="test-example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"}
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
                "type": "database_parameter",
                "id": dp.id,
                "change_type": "update",
                "delta": {
                    "type_changes": {
                        "root['amount']": {"old_type": "float", "new_type": "int", "new_value": 7}
                    },
                    "values_changed": {"root['name']": {"new_value": "another"}},
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)

    assert DatabaseParameter.select().count() == 1
    dp = DatabaseParameter.get(id=dp.id)
    assert dp.data == {"foo": "bar"}
    assert dp.amount == 7
    assert dp.formula == "1 * 2 + 3"
    assert dp.name == "another"


@bw2test
def test_database_parameter_revision_expected_format_delete(num_revisions):
    projects.set_current("activity-event")

    DatabaseChooser("test-example").register()
    dp = DatabaseParameter.create(
        name="example", database="test-example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"}
    )

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    dp.delete_instance()

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
                "type": "database_parameter",
                "id": dp.id,
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
def test_database_parameter_revision_apply_delete(num_revisions):
    projects.set_current("activity-event")

    DatabaseChooser("test-example").register()
    dp = DatabaseParameter.create(
        name="example", database="test-example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"}
    )
    assert DatabaseParameter.select().count() == 1
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
                "type": "database_parameter",
                "id": dp.id,
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
    assert not DatabaseParameter.select().count()


@bw2test
def test_database_parameter_revision_expected_format_recalculate(num_revisions):
    projects.set_current("activity-event")

    # Needed to have a parameter which could be obsolete - otherwise `recalculate` just
    # no-op exits
    DatabaseChooser("test-example").register()
    DatabaseParameter.create(
        name="example", database="test-example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"}
    )

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    DatabaseParameter.recalculate("test-example")

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
                "type": "database_parameter",
                "id": "test-example",
                "change_type": "database_parameter_recalculate",
                "delta": {},
            }
        ],
    }

    assert revision == expected


@bw2test
def test_database_parameter_revision_apply_recalculate(num_revisions, monkeypatch):
    def fake_recalculate(database, signal=True):
        assert database == "test-example"
        assert not signal

    monkeypatch.setattr(DatabaseParameter, "recalculate", fake_recalculate)

    projects.set_current("activity-event")
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
                "type": "database_parameter",
                "id": "test-example",
                "change_type": "database_parameter_recalculate",
                "delta": {},
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)


@bw2test
def test_database_parameter_revision_expected_format_update_formula_project_parameter_name(
    num_revisions,
):
    projects.set_current("activity-event")
    DatabaseChooser("test-example").register()

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    DatabaseParameter.update_formula_project_parameter_name(old="one2three", new="123")

    # from pprint import pprint
    # pprint([
    #     json.load(open(fp))
    #     for fp in (projects.dataset.dir / "revisions").iterdir()
    #     if fp.stem.lower() != "head" and fp.is_file()
    # ])

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
                "type": "database_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "database_parameter_update_formula_project_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123"},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_database_parameter_revision_apply_update_formula_project_parameter_name(
    num_revisions, monkeypatch
):
    def fake_update(old, new, signal=True):
        assert old == "one2three"
        assert new == "123"
        assert not signal

    monkeypatch.setattr(DatabaseParameter, "update_formula_project_parameter_name", fake_update)

    projects.set_current("activity-event")
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
                "type": "database_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "database_parameter_update_formula_project_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123"},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)


@bw2test
def test_database_parameter_revision_expected_format_update_formula_database_parameter_name(
    num_revisions,
):
    projects.set_current("activity-event")
    DatabaseChooser("test-example").register()

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    DatabaseParameter.update_formula_database_parameter_name(old="one2three", new="123")

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
                "type": "database_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "database_parameter_update_formula_database_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123"},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_database_parameter_revision_apply_update_formula_database_parameter_name(
    num_revisions, monkeypatch
):
    def fake_update(old, new, signal=True):
        assert old == "one2three"
        assert new == "123"
        assert not signal

    monkeypatch.setattr(DatabaseParameter, "update_formula_database_parameter_name", fake_update)

    projects.set_current("activity-event")
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
                "type": "database_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "database_parameter_update_formula_database_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123"},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)
