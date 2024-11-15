import json

from bw2data.database import DatabaseChooser
from bw2data.parameters import ProjectParameter
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test


@bw2test
def test_project_parameter_revision_expected_format_create(num_revisions):
    projects.set_current("activity-event")

    assert not ProjectParameter.select().count()
    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    pp = ProjectParameter.create(name="example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"})
    assert pp.id > 1e6
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
                "type": "project_parameter",
                "id": pp.id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "id": pp.id,
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
def test_project_parameter_revision_apply_create(num_revisions):
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    revision_id = next(snowflake_id_generator)
    pp_id = next(snowflake_id_generator)
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
                "type": "project_parameter",
                "id": pp_id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {
                                "id": pp_id,
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

    assert ProjectParameter.select().count() == 1
    pp = ProjectParameter.get(id=pp_id)
    assert pp.data == {"foo": "bar"}
    assert pp.amount == 5
    assert pp.formula == "1 * 2 + 3"
    assert pp.name == "example"


@bw2test
def test_project_parameter_revision_expected_format_update(num_revisions):
    projects.set_current("activity-event")

    pp = ProjectParameter.create(name="example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"})

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    pp.name = "another"
    pp.amount = 7
    pp.save()

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
                "type": "project_parameter",
                "id": pp.id,
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
def test_project_parameter_revision_apply_update(num_revisions):
    projects.set_current("activity-event")
    pp = ProjectParameter.create(name="example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"})

    projects.dataset.set_sourced()
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
                "type": "project_parameter",
                "id": pp.id,
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

    assert ProjectParameter.select().count() == 1
    pp = ProjectParameter.get(id=pp.id)
    assert pp.data == {"foo": "bar"}
    assert pp.amount == 7
    assert pp.formula == "1 * 2 + 3"
    assert pp.name == "another"


@bw2test
def test_project_parameter_revision_expected_format_delete(num_revisions):
    projects.set_current("activity-event")

    pp = ProjectParameter.create(name="example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"})

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    pp.delete_instance()

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
                "type": "project_parameter",
                "id": pp.id,
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
def test_project_parameter_revision_apply_delete(num_revisions):
    projects.set_current("activity-event")
    pp = ProjectParameter.create(name="example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"})
    assert ProjectParameter.select().count() == 1
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
                "type": "project_parameter",
                "id": pp.id,
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
    assert not ProjectParameter.select().count()


@bw2test
def test_project_parameter_revision_expected_format_recalculate(num_revisions):
    projects.set_current("activity-event")

    # Needed to have a parameter which could be obsolete - otherwise `recalculate` just
    # no-op exits
    ProjectParameter.create(name="example", formula="1 * 2 + 3", amount=5, data={"foo": "bar"})

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    ProjectParameter.recalculate()

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
                "type": "project_parameter",
                "id": "__recalculate_dummy__",
                "change_type": "project_parameter_recalculate",
                "delta": {},
            }
        ],
    }

    assert revision == expected


@bw2test
def test_project_parameter_revision_apply_recalculate(num_revisions, monkeypatch):
    def fake_recalculate(ignored=None, signal=True):
        assert not signal

    monkeypatch.setattr(ProjectParameter, "recalculate", fake_recalculate)

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
                "type": "project_parameter",
                "id": "__recalculate_dummy__",
                "change_type": "project_parameter_recalculate",
                "delta": {},
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)


@bw2test
def test_project_parameter_revision_expected_format_update_formula_parameter_name(num_revisions):
    projects.set_current("activity-event")

    assert projects.dataset.revision is None
    projects.dataset.set_sourced()

    ProjectParameter.update_formula_parameter_name(old="one2three", new="123")

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
                "type": "project_parameter",
                "id": "__update_formula_parameter_name_dummy__",
                "change_type": "project_parameter_update_formula_parameter_name",
                "delta": {
                    "dictionary_item_added": {"root['new']": "123"},
                    "dictionary_item_removed": {"root['old']": "one2three"},
                },
            }
        ],
    }

    assert revision == expected
