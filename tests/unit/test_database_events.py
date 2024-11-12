# Test database .delete()
# Test database delete
# Test database write
# Test database copy
# Test database rename
# Test no signal on `write` with unsourced database


import json

import pytest

from bw2data import databases
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test


@bw2test
def test_database_metadata_revision_expected_format_update():
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    database.metadata["foo"] = True
    database.metadata["other"] = 7
    databases.flush()

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
                "type": "lci_database",
                "id": None,
                "change_type": "database_metadata_change",
                "delta": {
                    "dictionary_item_added": {"root['db']['foo']": True, "root['db']['other']": 7}
                },
            }
        ],
    }

    assert revision == expected


@bw2test
def test_database_metadata_revision_apply_update():
    projects.set_current("activity-event")
    assert projects.dataset.revision is None

    database = DatabaseChooser("db")
    database.register()

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
                "type": "lci_database",
                "id": None,
                "change_type": "database_metadata_change",
                "delta": {
                    "dictionary_item_added": {"root['db']['foo']": True, "root['db']['other']": 7}
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    revision_files = [
        fp
        for fp in (projects.dataset.dir / "revisions").iterdir()
        if fp.stem.lower() != "head" and fp.is_file()
    ]
    assert not revision_files

    database = DatabaseChooser("db")
    assert database.metadata["foo"] is True
    assert database.metadata["other"] == 7

    assert databases["db"]["foo"] is True
    assert databases["db"]["other"] == 7


@bw2test
def test_database_metadata_no_revision_ignored_changes(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None
    assert not num_revisions(projects)
    assert not database.metadata.get("dirty")

    databases.set_dirty(database.name)
    assert database.metadata["dirty"]

    assert projects.dataset.revision is None
    assert not num_revisions(projects)

    databases.set_modified(database.name)

    assert projects.dataset.revision is None
    assert not num_revisions(projects)


@bw2test
def test_database_metadata_revision_expected_format_create():
    projects.set_current("activity-event")

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    database = DatabaseChooser("db")
    database.register(foo="bar", other=7)

    assert projects.dataset.revision is not None
    revisions = sorted(
        [
            (int(fp.stem), json.load(open(fp)))
            for fp in sorted((projects.dataset.dir / "revisions").iterdir())
            if fp.is_file()
            if fp.stem.lower() != "head"
        ]
    )

    expected = [
        {
            "metadata": {
                "parent_revision": None,
                "revision": revisions[0][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_database",
                    "id": None,
                    "change_type": "database_metadata_change",
                    "delta": {
                        "dictionary_item_added": {
                            "root['db']": {
                                "foo": "bar",
                                "other": 7,
                                "depends": [],
                                "backend": "sqlite",
                            }
                        }
                    },
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": revisions[0][0],
                "revision": revisions[1][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_database",
                    "id": None,
                    "change_type": "database_metadata_change",
                    "delta": {"dictionary_item_added": {"root['db']['geocollections']": []}},
                }
            ],
        },
    ]

    assert [x[1] for x in revisions] == expected
    assert projects.dataset.revision == revisions[1][0]


# @bw2test
# def test_database_metadata_revision_apply_create():
#     projects.set_current("activity-event")
#     assert projects.dataset.revision is None

#     database = DatabaseChooser("db")
#     database.register()

#     revision_id = next(snowflake_id_generator)
#     revision = {
#         "metadata": {
#             "parent_revision": None,
#             "revision": revision_id,
#             "authors": "Anonymous",
#             "title": "Untitled revision",
#             "description": "No description",
#         },
#         "data": [
#             {
#                 "type": "lci_database",
#                 "id": None,
#                 "change_type": "database_metadata_change",
#                 "delta": {
#                     "dictionary_item_added": {"root['db']['foo']": True, "root['db']['other']": 7}
#                 },
#             }
#         ],
#     }

#     projects.dataset.apply_revision(revision)
#     assert projects.dataset.revision == revision_id

#     revision_files = [
#         fp
#         for fp in (projects.dataset.dir / "revisions").iterdir()
#         if fp.stem.lower() != "head" and fp.is_file()
#     ]
#     assert not revision_files

#     database = DatabaseChooser("db")
#     assert database.metadata["foo"] is True
#     assert database.metadata["other"] == 7

#     assert databases["db"]["foo"] is True
#     assert databases["db"]["other"] == 7
