import json

from bw2data.parameters import Group
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test


@bw2test
def test_group_revision_expected_format_create(num_revisions):
    projects.set_current("activity-event")

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    group = Group.create(name="A", order=[])

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
                "type": "group",
                "id": group.id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {"id": group.id, "name": "A", "order": []},
                        }
                    }
                },
            }
        ],
    }

    assert revision == expected
    assert num_revisions(projects) == 1


@bw2test
def test_group_revision_apply_create(num_revisions):
    projects.set_current("activity-event")

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    revision_id = next(snowflake_id_generator)
    group_id = next(snowflake_id_generator)
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
                "type": "group",
                "id": group_id,
                "change_type": "create",
                "delta": {
                    "type_changes": {
                        "root": {
                            "old_type": "NoneType",
                            "new_type": "dict",
                            "new_value": {"id": group_id, "name": "A", "order": []},
                        }
                    }
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id
    assert not num_revisions(projects)

    group = Group.get(Group.id == group_id)
    assert group.name == "A"
    assert group.order == []


@bw2test
def test_group_revision_expected_format_delete(num_revisions):
    projects.set_current("activity-event")

    group = Group.create(name="A", order=[])

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    group.delete_instance()

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
                "type": "group",
                "id": group.id,
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
    assert num_revisions(projects) == 1


@bw2test
def test_group_revision_apply_delete(num_revisions):
    projects.set_current("activity-event")

    group = Group.create(name="A", order=[])

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
                "type": "group",
                "id": group.id,
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
    assert not Group.select().count()
    assert not num_revisions(projects)


@bw2test
def test_group_revision_expected_format_update(num_revisions):
    projects.set_current("activity-event")

    group = Group.create(name="A", order=[])

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    group.order = ["foo", "bar"]
    group.save()

    parent = projects.dataset.revision
    assert parent is not None

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
                "type": "group",
                "id": group.id,
                "change_type": "update",
                "delta": {
                    "iterable_item_added": {"root['order'][0]": "foo", "root['order'][1]": "bar"}
                },
            }
        ],
    }

    assert revision == expected
    assert num_revisions(projects) == 1


@bw2test
def test_group_revision_apply_update(num_revisions):
    projects.set_current("activity-event")

    group = Group.create(name="A", order=[])

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
                "type": "group",
                "id": group.id,
                "change_type": "update",
                "delta": {
                    "iterable_item_added": {"root['order'][0]": "foo", "root['order'][1]": "bar"}
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id
    group = Group.get(Group.id == group.id)
    assert group.order == ["foo", "bar"]
    assert not num_revisions(projects)
