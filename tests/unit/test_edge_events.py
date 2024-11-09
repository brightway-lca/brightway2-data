import json

import pytest

from bw2data import get_node
from bw2data.backends.schema import ExchangeDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test

# test_edge_deletion
# test_edge_modification


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
def test_edge_revision_apply_create():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
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
            "parent_revision": None,
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

    revision_files = [
        fp
        for fp in (projects.dataset.dir / "revisions").iterdir()
        if fp.stem.lower() != "head" and fp.is_file()
    ]
    assert not revision_files

    assert len(get_node(code="A").exchanges()) == 1
    for edge in get_node(code="A").exchanges():
        assert edge["arbitrary"] == "foo"
        assert edge._document.id == edge_id
        assert edge["amount"] == 0.1
        assert edge.output["code"] == "A"
        assert edge.input["code"] == "B"
    assert len(database) == 2


# @bw2test
# def test_node_revision_expected_format_delete():
#     projects.set_current("activity-event")
#     projects.dataset.set_sourced()

#     database = DatabaseChooser("db")
#     database.register()
#     node = database.new_node(code="A", name="A")
#     node.save()

#     parent = projects.dataset.revision
#     assert parent is not None
#     node.delete()

#     with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
#         revision = json.load(f)

#     expected = {
#         "data": [
#             {
#                 "change_type": "delete",
#                 "delta": {
#                     "type_changes": {
#                         "root": {
#                             "new_type": "NoneType",
#                             "new_value": None,
#                             "old_type": "dict",
#                         }
#                     }
#                 },
#                 "id": node.id,
#                 "type": "lci_node",
#             }
#         ],
#         "metadata": {
#             "authors": "Anonymous",
#             "description": "No description",
#             "parent_revision": parent,
#             "revision": projects.dataset.revision,
#             "title": "Untitled revision",
#         },
#     }

#     assert revision == expected


# @bw2test
# def test_node_revision_apply_delete():
#     projects.set_current("activity-event")
#     projects.dataset.set_sourced()

#     database = DatabaseChooser("db")
#     database.register()
#     node = database.new_node(code="A", name="A")
#     node.save()
#     assert len(database) == 1

#     revision_id = next(snowflake_id_generator)

#     revision = {
#         "data": [
#             {
#                 "change_type": "delete",
#                 "delta": {
#                     "type_changes": {
#                         "root": {
#                             "new_type": "NoneType",
#                             "new_value": None,
#                             "old_type": "dict",
#                         }
#                     }
#                 },
#                 "id": node.id,
#                 "type": "lci_node",
#             }
#         ],
#         "metadata": {
#             "authors": "Anonymous",
#             "description": "No description",
#             "parent_revision": projects.dataset.revision,
#             "revision": revision_id,
#             "title": "Untitled revision",
#         },
#     }

#     projects.dataset.apply_revision(revision)
#     assert projects.dataset.revision == revision_id
#     assert len(database) == 0

#     revision_files = [
#         fp
#         for fp in (projects.dataset.dir / "revisions").iterdir()
#         if fp.stem.lower() != "head" and fp.is_file()
#     ]
#     assert len(revision_files) == 1


# @bw2test
# def test_node_revision_expected_format_update():
#     projects.set_current("activity-event")
#     projects.dataset.set_sourced()

#     database = DatabaseChooser("db")
#     database.register()
#     node = database.new_node(code="A", name="A")
#     node.save()

#     parent = projects.dataset.revision
#     assert parent is not None
#     node["name"] = "B"
#     node.save()

#     with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
#         revision = json.load(f)

#     expected = {
#         "data": [
#             {
#                 "change_type": "update",
#                 "delta": {"values_changed": {"root['name']": {"new_value": "B"}}},
#                 "id": node.id,
#                 "type": "lci_node",
#             }
#         ],
#         "metadata": {
#             "authors": "Anonymous",
#             "description": "No description",
#             "parent_revision": parent,
#             "revision": projects.dataset.revision,
#             "title": "Untitled revision",
#         },
#     }

#     assert revision == expected


# @bw2test
# def test_node_revision_apply_update():
#     projects.set_current("activity-event")
#     projects.dataset.set_sourced()

#     database = DatabaseChooser("db")
#     database.register()
#     node = database.new_node(code="A", name="A", location="kalamazoo")
#     node.save()

#     revision_id = next(snowflake_id_generator)

#     revision = {
#         "data": [
#             {
#                 "change_type": "update",
#                 "delta": {"values_changed": {"root['name']": {"new_value": "B"}}},
#                 "id": node.id,
#                 "type": "lci_node",
#             }
#         ],
#         "metadata": {
#             "authors": "Anonymous",
#             "description": "No description",
#             "parent_revision": projects.dataset.revision,
#             "revision": revision_id,
#             "title": "Untitled revision",
#         },
#     }

#     projects.dataset.apply_revision(revision)
#     assert projects.dataset.revision == revision_id
#     assert len(database) == 1
#     node = get_node(code="A")
#     assert node["name"] == "B"

#     revision_files = [
#         fp
#         for fp in (projects.dataset.dir / "revisions").iterdir()
#         if fp.stem.lower() != "head" and fp.is_file()
#     ]
#     assert len(revision_files) == 1


# @bw2test
# def test_node_revision_expected_format_activity_database_change():
#     projects.set_current("activity-event")
#     projects.dataset.set_sourced()

#     database = DatabaseChooser("db")
#     database.register()
#     DatabaseChooser("db2").register()
#     node = database.new_node(code="A", name="A")
#     node.save()
#     other = database.new_node(code="B", name="B2", type="product")
#     other.save()
#     node.new_edge(input=other, type="technosphere", amount=0.1).save()
#     node.new_edge(input=other, type="production", amount=1.0).save()

#     parent = projects.dataset.revision
#     assert parent is not None
#     node["database"] = "db2"

#     with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
#         revision = json.load(f)

#     expected = {
#         "data": [
#             {
#                 "change_type": "activity_database_change",
#                 "delta": {"values_changed": {"root['database']": {"new_value": "db2"}}},
#                 "id": node.id,
#                 "type": "lci_node",
#             }
#         ],
#         "metadata": {
#             "authors": "Anonymous",
#             "description": "No description",
#             "parent_revision": parent,
#             "revision": projects.dataset.revision,
#             "title": "Untitled revision",
#         },
#     }

#     assert revision == expected


# @bw2test
# def test_node_revision_apply_activity_database_change():
#     projects.set_current("activity-event")
#     projects.dataset.set_sourced()

#     database = DatabaseChooser("db")
#     database.register()
#     DatabaseChooser("db2").register()
#     node = database.new_node(code="A", name="A")
#     node.save()
#     other = database.new_node(code="B", name="B2", type="product")
#     other.save()
#     assert len(database) == 2
#     node.new_edge(input=other, type="technosphere", amount=0.1).save()
#     node.new_edge(input=node, type="production", amount=1.0).save()
#     assert len(node.exchanges()) == 2

#     revision_id = next(snowflake_id_generator)

#     num_revisions_before = len(
#         [
#             fp
#             for fp in (projects.dataset.dir / "revisions").iterdir()
#             if fp.stem.lower() != "head" and fp.is_file()
#         ]
#     )

#     revision = {
#         "data": [
#             {
#                 "change_type": "activity_database_change",
#                 "delta": {"values_changed": {"root['database']": {"new_value": "db2"}}},
#                 "id": node.id,
#                 "type": "lci_node",
#             }
#         ],
#         "metadata": {
#             "authors": "Anonymous",
#             "description": "No description",
#             "parent_revision": projects.dataset.revision,
#             "revision": revision_id,
#             "title": "Untitled revision",
#         },
#     }

#     projects.dataset.apply_revision(revision)
#     assert projects.dataset.revision == revision_id
#     assert len(DatabaseChooser("db")) == 1
#     assert len(DatabaseChooser("db2")) == 1
#     node = get_node(code="A")
#     assert node["database"] == "db2"
#     assert len(node.exchanges()) == 2
#     assert ExchangeDataset.select().count() == 2
#     for exc in node.production():
#         assert exc.input == node
#         assert exc.output == node

#     num_revisions_after = len(
#         [
#             fp
#             for fp in (projects.dataset.dir / "revisions").iterdir()
#             if fp.stem.lower() != "head" and fp.is_file()
#         ]
#     )
#     assert num_revisions_after == num_revisions_before


# @bw2test
# def test_node_revision_expected_format_activity_code_change():
#     projects.set_current("activity-event")
#     projects.dataset.set_sourced()

#     database = DatabaseChooser("db")
#     database.register()
#     node = database.new_node(code="A", name="A")
#     node.save()
#     other = database.new_node(code="B", name="B2", type="product")
#     other.save()
#     node.new_edge(input=other, type="technosphere", amount=0.1).save()
#     node.new_edge(input=other, type="production", amount=1.0).save()

#     parent = projects.dataset.revision
#     assert parent is not None
#     node["code"] = "bar"

#     with open(projects.dataset.dir / "revisions" / f"{projects.dataset.revision}.rev", "r") as f:
#         revision = json.load(f)

#     expected = {
#         "data": [
#             {
#                 "change_type": "activity_code_change",
#                 "delta": {"values_changed": {"root['code']": {"new_value": "bar"}}},
#                 "id": node.id,
#                 "type": "lci_node",
#             }
#         ],
#         "metadata": {
#             "authors": "Anonymous",
#             "description": "No description",
#             "parent_revision": parent,
#             "revision": projects.dataset.revision,
#             "title": "Untitled revision",
#         },
#     }

#     assert revision == expected


# @bw2test
# def test_node_revision_apply_activity_code_change():
#     projects.set_current("activity-event")
#     projects.dataset.set_sourced()

#     database = DatabaseChooser("db")
#     database.register()
#     node = database.new_node(code="A", name="A")
#     node.save()
#     other = database.new_node(code="B", name="B2", type="product")
#     other.save()
#     assert len(database) == 2
#     node.new_edge(input=other, type="technosphere", amount=0.1).save()
#     node.new_edge(input=node, type="production", amount=1.0).save()
#     assert len(node.exchanges()) == 2

#     revision_id = next(snowflake_id_generator)

#     num_revisions_before = len(
#         [
#             fp
#             for fp in (projects.dataset.dir / "revisions").iterdir()
#             if fp.stem.lower() != "head" and fp.is_file()
#         ]
#     )

#     revision = {
#         "data": [
#             {
#                 "change_type": "activity_code_change",
#                 "delta": {"values_changed": {"root['code']": {"new_value": "bar"}}},
#                 "id": node.id,
#                 "type": "lci_node",
#             }
#         ],
#         "metadata": {
#             "authors": "Anonymous",
#             "description": "No description",
#             "parent_revision": projects.dataset.revision,
#             "revision": revision_id,
#             "title": "Untitled revision",
#         },
#     }

#     projects.dataset.apply_revision(revision)
#     assert projects.dataset.revision == revision_id
#     assert len(DatabaseChooser("db")) == 2
#     node = get_node(code="bar")
#     assert node["database"] == "db"
#     assert len(node.exchanges()) == 2
#     assert ExchangeDataset.select().count() == 2
#     for exc in node.production():
#         assert exc.input == node
#         assert exc.output == node

#     num_revisions_after = len(
#         [
#             fp
#             for fp in (projects.dataset.dir / "revisions").iterdir()
#             if fp.stem.lower() != "head" and fp.is_file()
#         ]
#     )
#     assert num_revisions_after == num_revisions_before


# @bw2test
# def test_node_revision_expected_format_activity_copy():
#     projects.set_current("activity-event")

#     database = DatabaseChooser("db")
#     database.register()
#     DatabaseChooser("db2").register()
#     node = database.new_node(code="A", name="A")
#     node.save()
#     other = database.new_node(code="B", name="B2", type="product")
#     other.save()
#     node.new_edge(input=other, type="technosphere", amount=0.1).save()
#     node.new_edge(input=other, type="production", amount=1.0).save()

#     projects.dataset.set_sourced()

#     node.copy(code="foo")
#     foo_node = get_node(code="foo")
#     for prod_exc in foo_node.production():
#         pass
#     for tech_exc in foo_node.technosphere():
#         pass

#     revisions = [
#         (int(fp.stem), json.load(open(fp)))
#         for fp in sorted((projects.dataset.dir / "revisions").iterdir())
#         if fp.is_file()
#         if fp.stem.lower() != "head"
#     ]

#     expected = [
#         {
#             "data": [
#                 {
#                     "change_type": "create",
#                     "delta": {
#                         "type_changes": {
#                             "root": {
#                                 "new_type": "dict",
#                                 "new_value": {
#                                     "code": "foo",
#                                     "database": "db",
#                                     "location": "GLO",
#                                     "name": "A",
#                                 },
#                                 "old_type": "NoneType",
#                             }
#                         }
#                     },
#                     "id": foo_node.id,
#                     "type": "lci_node",
#                 }
#             ],
#             "metadata": {
#                 "authors": "Anonymous",
#                 "description": "No description",
#                 "parent_revision": None,
#                 "revision": revisions[0][0],
#                 "title": "Untitled revision",
#             },
#         },
#         {
#             "data": [
#                 {
#                     "change_type": "create",
#                     "delta": {
#                         "type_changes": {
#                             "root": {
#                                 "new_type": "dict",
#                                 "new_value": {
#                                     "data": {
#                                         "amount": 0.1,
#                                         "input": ["db", "B"],
#                                         "output": ["db", "foo"],
#                                         "type": "technosphere",
#                                     },
#                                     "input_code": "B",
#                                     "input_database": "db",
#                                     "output_code": "foo",
#                                     "output_database": "db",
#                                     "type": "technosphere",
#                                 },
#                                 "old_type": "NoneType",
#                             }
#                         }
#                     },
#                     "id": tech_exc._document.id,
#                     "type": "lci_edge",
#                 }
#             ],
#             "metadata": {
#                 "authors": "Anonymous",
#                 "description": "No description",
#                 "parent_revision": revisions[0][0],
#                 "revision": revisions[1][0],
#                 "title": "Untitled revision",
#             },
#         },
#         {
#             "data": [
#                 {
#                     "change_type": "create",
#                     "delta": {
#                         "type_changes": {
#                             "root": {
#                                 "new_type": "dict",
#                                 "new_value": {
#                                     "data": {
#                                         "amount": 1.0,
#                                         "input": ["db", "B"],
#                                         "output": ["db", "foo"],
#                                         "type": "production",
#                                     },
#                                     "input_code": "B",
#                                     "input_database": "db",
#                                     "output_code": "foo",
#                                     "output_database": "db",
#                                     "type": "production",
#                                 },
#                                 "old_type": "NoneType",
#                             }
#                         }
#                     },
#                     "id": prod_exc._document.id,
#                     "type": "lci_edge",
#                 }
#             ],
#             "metadata": {
#                 "authors": "Anonymous",
#                 "description": "No description",
#                 "parent_revision": revisions[1][0],
#                 "revision": revisions[2][0],
#                 "title": "Untitled revision",
#             },
#         },
#     ]

#     assert [x[1] for x in revisions] == expected


# @bw2test
# def test_node_revision_apply_activity_copy():
#     projects.set_current("activity-event")

#     database = DatabaseChooser("db")
#     database.register()
#     DatabaseChooser("db2").register()
#     node = database.new_node(code="A", name="A")
#     node.save()
#     other = database.new_node(code="B", name="B2", type="product")
#     other.save()
#     node.new_edge(input=other, type="technosphere", amount=0.1).save()
#     node.new_edge(input=node, type="production", amount=1.0).save()

#     projects.dataset.set_sourced()

#     revision_id_1 = next(snowflake_id_generator)
#     revision_id_2 = next(snowflake_id_generator)
#     revision_id_3 = next(snowflake_id_generator)

#     num_revisions_before = len(
#         [
#             fp
#             for fp in (projects.dataset.dir / "revisions").iterdir()
#             if fp.stem.lower() != "head" and fp.is_file()
#         ]
#     )

#     revisions = [
#         {
#             "metadata": {
#                 "parent_revision": None,
#                 "revision": revision_id_1,
#                 "authors": "Anonymous",
#                 "title": "Untitled revision",
#                 "description": "No description",
#             },
#             "data": [
#                 {
#                     "type": "lci_node",
#                     "id": 3,
#                     "change_type": "create",
#                     "delta": {
#                         "type_changes": {
#                             "root": {
#                                 "old_type": "NoneType",
#                                 "new_type": "dict",
#                                 "new_value": {
#                                     "database": "db",
#                                     "code": "foo",
#                                     "location": "GLO",
#                                     "name": "A",
#                                 },
#                             }
#                         }
#                     },
#                 }
#             ],
#         },
#         {
#             "metadata": {
#                 "parent_revision": revision_id_1,
#                 "revision": revision_id_2,
#                 "authors": "Anonymous",
#                 "title": "Untitled revision",
#                 "description": "No description",
#             },
#             "data": [
#                 {
#                     "type": "lci_edge",
#                     "id": 3,
#                     "change_type": "create",
#                     "delta": {
#                         "type_changes": {
#                             "root": {
#                                 "old_type": "NoneType",
#                                 "new_type": "dict",
#                                 "new_value": {
#                                     "data": {
#                                         "output": ["db", "foo"],
#                                         "input": ["db", "B"],
#                                         "type": "technosphere",
#                                         "amount": 0.1,
#                                     },
#                                     "input_database": "db",
#                                     "input_code": "B",
#                                     "output_database": "db",
#                                     "output_code": "foo",
#                                     "type": "technosphere",
#                                 },
#                             }
#                         }
#                     },
#                 }
#             ],
#         },
#         {
#             "metadata": {
#                 "parent_revision": revision_id_2,
#                 "revision": revision_id_3,
#                 "authors": "Anonymous",
#                 "title": "Untitled revision",
#                 "description": "No description",
#             },
#             "data": [
#                 {
#                     "type": "lci_edge",
#                     "id": 4,
#                     "change_type": "create",
#                     "delta": {
#                         "type_changes": {
#                             "root": {
#                                 "old_type": "NoneType",
#                                 "new_type": "dict",
#                                 "new_value": {
#                                     "data": {
#                                         "output": ["db", "foo"],
#                                         "input": ["db", "foo"],
#                                         "type": "production",
#                                         "amount": 1.0,
#                                     },
#                                     "input_database": "db",
#                                     "input_code": "foo",
#                                     "output_database": "db",
#                                     "output_code": "foo",
#                                     "type": "production",
#                                 },
#                             }
#                         }
#                     },
#                 }
#             ],
#         },
#     ]

#     for revision in revisions:
#         projects.dataset.apply_revision(revision)
#     assert projects.dataset.revision == revision_id_3
#     assert len(DatabaseChooser("db")) == 3
#     new_node = get_node(code="foo")
#     assert node["database"] == "db"
#     assert len(node.exchanges()) == 2
#     assert ExchangeDataset.select().count() == 4
#     for exc in node.production():
#         assert exc.input == node
#         assert exc.output == node
#         assert exc["amount"] == 1.0
#     for exc in new_node.production():
#         assert exc.input == new_node
#         assert exc.output == new_node
#         assert exc["amount"] == 1.0
#     for exc in new_node.technosphere():
#         assert exc.input == other
#         assert exc.output == new_node
#         assert exc["amount"] == 0.1

#     num_revisions_after = len(
#         [
#             fp
#             for fp in (projects.dataset.dir / "revisions").iterdir()
#             if fp.stem.lower() != "head" and fp.is_file()
#         ]
#     )
#     assert num_revisions_after == num_revisions_before


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
