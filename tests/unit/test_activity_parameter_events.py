import json

from bw2data.database import DatabaseChooser
from bw2data.parameters import ActivityParameter, Group
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test


@bw2test
def test_activity_parameter_revision_expected_format_create(num_revisions, monkeypatch):
    def no_signal_save(self, *args, **kwargs):
        kwargs["signal"] = False
        return super(Group, self).save(*args, **kwargs)

    monkeypatch.setattr(Group, "save", no_signal_save)

    projects.set_current("activity-event")

    assert not ActivityParameter.select().count()
    assert projects.dataset.revision is None

    DatabaseChooser("test-database").register()

    projects.dataset.set_sourced()

    dp = ActivityParameter.create(
        database="test-database",
        code="test-code",
        group="test-group",
        name="example",
        formula="1 * 2 + 3",
        amount=5,
        data={"foo": "bar"},
    )

    from pprint import pprint

    pprint(
        [
            json.load(open(fp))
            for fp in (projects.dataset.dir / "revisions").iterdir()
            if fp.stem.lower() != "head" and fp.is_file()
        ]
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
                "type": "activity_parameter",
                "id": dp.id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "id": dp.id,
                                "database": "test-database",
                                "group": "test-group",
                                "code": "test-code",
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
def test_activity_parameter_revision_apply_create(num_revisions, monkeypatch):
    def no_signal_save(self, *args, **kwargs):
        kwargs["signal"] = False
        return super(Group, self).save(*args, **kwargs)

    monkeypatch.setattr(Group, "save", no_signal_save)

    projects.set_current("activity-event")
    DatabaseChooser("test-database").register()
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
                "type": "activity_parameter",
                "id": dp_id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "id": dp_id,
                                "database": "test-database",
                                "group": "test-group",
                                "code": "test-code",
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

    assert ActivityParameter.select().count() == 1
    dp = ActivityParameter.get(id=dp_id)
    assert dp.data == {"foo": "bar"}
    assert dp.database == "test-database"
    assert dp.code == "test-code"
    assert dp.group == "test-group"
    assert dp.amount == 5
    assert dp.formula == "1 * 2 + 3"
    assert dp.name == "example"


@bw2test
def test_activity_parameter_revision_expected_format_update(num_revisions):
    projects.set_current("activity-event")

    DatabaseChooser("test-database").register()
    dp = ActivityParameter.create(
        database="test-database",
        code="test-code",
        group="test-group",
        name="example",
        formula="1 * 2 + 3",
        amount=5,
        data={"foo": "bar"},
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
                "type": "activity_parameter",
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
def test_activity_parameter_revision_apply_update(num_revisions):
    projects.set_current("activity-event")

    DatabaseChooser("test-database").register()
    dp = ActivityParameter.create(
        database="test-database",
        code="test-code",
        group="test-group",
        name="example",
        formula="1 * 2 + 3",
        amount=5,
        data={"foo": "bar"},
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
                "type": "activity_parameter",
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

    assert ActivityParameter.select().count() == 1
    dp = ActivityParameter.get(id=dp.id)
    assert dp.data == {"foo": "bar"}
    assert dp.amount == 7
    assert dp.formula == "1 * 2 + 3"
    assert dp.name == "another"


@bw2test
def test_activity_parameter_revision_expected_format_delete(num_revisions):
    projects.set_current("activity-event")

    DatabaseChooser("test-database").register()
    dp = ActivityParameter.create(
        database="test-database",
        code="test-code",
        group="test-group",
        name="example",
        formula="1 * 2 + 3",
        amount=5,
        data={"foo": "bar"},
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
                "type": "activity_parameter",
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
def test_activity_parameter_revision_apply_delete(num_revisions):
    projects.set_current("activity-event")

    DatabaseChooser("test-database").register()
    dp = ActivityParameter.create(
        database="test-database",
        code="test-code",
        group="test-group",
        name="example",
        formula="1 * 2 + 3",
        amount=5,
        data={"foo": "bar"},
    )
    assert ActivityParameter.select().count() == 1
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
                "type": "activity_parameter",
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
    assert not ActivityParameter.select().count()


@bw2test
def test_activity_parameter_revision_expected_format_recalculate(num_revisions):
    projects.set_current("activity-event")

    # Needed to have a parameter which could be obsolete - otherwise `recalculate` just
    # no-op exits
    DatabaseChooser("test-database").register()
    ActivityParameter.create(
        database="test-database",
        code="test-code",
        group="test-group",
        name="example",
        formula="1 * 2 + 3",
        amount=5,
        data={"foo": "bar"},
    )

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    ActivityParameter.recalculate("test-group")

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
                "type": "activity_parameter",
                "id": "test-group",
                "change_type": "activity_parameter_recalculate",
                "delta": {},
            }
        ],
    }

    assert revision == expected


@bw2test
def test_activity_parameter_revision_apply_recalculate(num_revisions, monkeypatch):
    def fake_recalculate(group, signal=True):
        assert group == "test-group"
        assert not signal

    monkeypatch.setattr(ActivityParameter, "recalculate", fake_recalculate)

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
                "type": "activity_parameter",
                "id": "test-group",
                "change_type": "activity_parameter_recalculate",
                "delta": {},
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)


@bw2test
def test_activity_parameter_revision_expected_format_recalculate_exchanges(num_revisions):
    projects.set_current("activity-event")

    # Needed to have a parameter which could be obsolete - otherwise `recalculate` just
    # no-op exits
    DatabaseChooser("test-database").register()
    ActivityParameter.create(
        database="test-database",
        code="test-code",
        group="test-group",
        name="example",
        formula="1 * 2 + 3",
        amount=5,
        data={"foo": "bar"},
    )
    ActivityParameter.recalculate("test-group")

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    ActivityParameter.recalculate_exchanges("test-group")

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
                "type": "activity_parameter",
                "id": "test-group",
                "change_type": "activity_parameter_recalculate_exchanges",
                "delta": {},
            }
        ],
    }

    assert revision == expected


@bw2test
def test_activity_parameter_revision_apply_recalculate_exchanges(num_revisions, monkeypatch):
    def fake_recalculate(group, signal=True):
        assert group == "test-group"
        assert not signal

    monkeypatch.setattr(ActivityParameter, "recalculate_exchanges", fake_recalculate)

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
                "type": "activity_parameter",
                "id": "test-group",
                "change_type": "activity_parameter_recalculate_exchanges",
                "delta": {},
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)


@bw2test
def test_activity_parameter_revision_expected_format_update_formula_project_parameter_name(
    num_revisions,
):
    projects.set_current("activity-event")
    DatabaseChooser("test-database").register()

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    ActivityParameter.update_formula_project_parameter_name(old="one2three", new="123")

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
                "type": "activity_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "activity_parameter_update_formula_project_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123"},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_activity_parameter_revision_apply_update_formula_project_parameter_name(
    num_revisions, monkeypatch
):
    def fake_update(old, new, signal=True):
        assert old == "one2three"
        assert new == "123"
        assert not signal

    monkeypatch.setattr(ActivityParameter, "update_formula_project_parameter_name", fake_update)

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
                "type": "activity_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "activity_parameter_update_formula_project_parameter_name",
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
def test_activity_parameter_revision_expected_format_update_formula_database_parameter_name(
    num_revisions,
):
    projects.set_current("activity-event")
    DatabaseChooser("test-database").register()

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    ActivityParameter.update_formula_database_parameter_name(old="one2three", new="123")

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
                "type": "activity_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "activity_parameter_update_formula_database_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123"},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_activity_parameter_revision_apply_update_formula_database_parameter_name(
    num_revisions, monkeypatch
):
    def fake_update(old, new, signal=True):
        assert old == "one2three"
        assert new == "123"
        assert not signal

    monkeypatch.setattr(ActivityParameter, "update_formula_database_parameter_name", fake_update)

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
                "type": "activity_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "activity_parameter_update_formula_database_parameter_name",
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
def test_activity_parameter_revision_expected_format_update_formula_activity_parameter_name(
    num_revisions,
):
    projects.set_current("activity-event")
    DatabaseChooser("test-database").register()

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    ActivityParameter.update_formula_activity_parameter_name(old="one2three", new="123")

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
                "type": "activity_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "activity_parameter_update_formula_activity_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123", "root['include_order']": False},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_activity_parameter_revision_apply_update_formula_activity_parameter_name(
    num_revisions, monkeypatch
):
    def fake_update(old, new, include_order, signal=True):
        assert old == "one2three"
        assert new == "123"
        assert not signal

    monkeypatch.setattr(ActivityParameter, "update_formula_activity_parameter_name", fake_update)

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
                "type": "activity_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "activity_parameter_update_formula_activity_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123", "root['include_order']": False},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)
