import json

from bw2data import databases, get_node
from bw2data.backends.schema import ExchangeDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.tests import bw2test

from ..fixtures import basic


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


@bw2test
def test_database_metadata_revision_apply_create(num_revisions):
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
                            "geocollections": [],
                        }
                    }
                },
            }
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)

    database = DatabaseChooser("db")
    assert database.metadata["foo"] == "bar"
    assert database.metadata["other"] == 7

    assert databases["db"]["foo"] == "bar"
    assert databases["db"]["other"] == 7


@bw2test
def test_database_reset_revision_expected_format(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    node.new_edge(input=other, type="technosphere", amount=0.1).save()
    node.new_edge(input=node, type="production", amount=1.0).save()

    assert len(database) == 2
    assert ExchangeDataset.select().count() == 2

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    database.delete(warn=False, signal=True)

    assert projects.dataset.revision is not None
    assert num_revisions(projects) == 1
    assert not len(database)
    assert not ExchangeDataset.select().count()

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
            {"type": "lci_database", "id": "db", "change_type": "database_reset", "delta": {}}
        ],
    }

    assert revision == expected


@bw2test
def test_database_reset_revision_apply(num_revisions):
    projects.set_current("activity-event")
    assert projects.dataset.revision is None

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    node.new_edge(input=other, type="technosphere", amount=0.1).save()
    node.new_edge(input=node, type="production", amount=1.0).save()

    assert len(database) == 2
    assert ExchangeDataset.select().count() == 2

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
            {"type": "lci_database", "id": "db", "change_type": "database_reset", "delta": {}}
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)
    database = DatabaseChooser("db")
    assert not len(database)
    assert not ExchangeDataset.select().count()


@bw2test
def test_database_delete_revision_expected_format(num_revisions):
    projects.set_current("activity-event")

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    node.new_edge(input=other, type="technosphere", amount=0.1).save()
    node.new_edge(input=node, type="production", amount=1.0).save()

    assert len(database) == 2
    assert ExchangeDataset.select().count() == 2

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    del databases[database.name]

    assert projects.dataset.revision is not None
    assert num_revisions(projects) == 1
    assert database.name not in databases
    assert not ExchangeDataset.select().count()

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
            {"type": "lci_database", "id": "db", "change_type": "database_delete", "delta": {}}
        ],
    }

    assert revision == expected


@bw2test
def test_database_delete_revision_apply(num_revisions):
    projects.set_current("activity-event")
    assert not num_revisions(projects)
    assert projects.dataset.revision is None

    database = DatabaseChooser("db")
    database.register()
    node = database.new_node(code="A", name="A")
    node.save()
    other = database.new_node(code="B", name="B2", type="product")
    other.save()
    node.new_edge(input=other, type="technosphere", amount=0.1).save()
    node.new_edge(input=node, type="production", amount=1.0).save()

    assert len(database) == 2
    assert ExchangeDataset.select().count() == 2

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
            {"type": "lci_database", "id": "db", "change_type": "database_delete", "delta": {}}
        ],
    }

    projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == revision_id

    assert not num_revisions(projects)
    assert "db" not in databases
    assert not ExchangeDataset.select().count()


@bw2test
def test_database_write_revision_expected_format():
    projects.set_current("activity-event")
    DatabaseChooser("biosphere").write(basic.biosphere)

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    DatabaseChooser("food").write(basic.food)

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
                            "root['food']": {"depends": [], "backend": "sqlite"}
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
                    "id": "food",
                    "change_type": "database_reset",
                    "delta": {},
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": revisions[1][0],
                "revision": revisions[2][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_database",
                    "id": None,
                    "change_type": "database_metadata_change",
                    "delta": {"iterable_item_added": {"root['food']['depends'][0]": "biosphere"}},
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": revisions[2][0],
                "revision": revisions[3][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_node",
                    "id": get_node(code="1", database="food").id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "categories": ["stuff", "meals"],
                                    "code": "1",
                                    "location": "CA",
                                    "name": "lunch",
                                    "type": "processwithreferenceproduct",
                                    "unit": "kg",
                                    "database": "food",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_node",
                    "id": get_node(code="2", database="food").id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "categories": ["stuff", "meals"],
                                    "code": "2",
                                    "location": "CH",
                                    "name": "dinner",
                                    "type": "processwithreferenceproduct",
                                    "unit": "kg",
                                    "database": "food",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "2",
                        ExchangeDataset.output_code == "1",
                        ExchangeDataset.input_database == "food",
                        ExchangeDataset.output_database == "food",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.5,
                                        "input": ["food", "2"],
                                        "type": "technosphere",
                                        "uncertainty type": 0,
                                        "output": ["food", "1"],
                                    },
                                    "input_database": "food",
                                    "input_code": "2",
                                    "output_database": "food",
                                    "output_code": "1",
                                    "type": "technosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "1",
                        ExchangeDataset.output_code == "1",
                        ExchangeDataset.input_database == "biosphere",
                        ExchangeDataset.output_database == "food",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.05,
                                        "input": ["biosphere", "1"],
                                        "type": "biosphere",
                                        "uncertainty type": 0,
                                        "output": ["food", "1"],
                                    },
                                    "input_database": "biosphere",
                                    "input_code": "1",
                                    "output_database": "food",
                                    "output_code": "1",
                                    "type": "biosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "1",
                        ExchangeDataset.output_code == "2",
                        ExchangeDataset.input_database == "food",
                        ExchangeDataset.output_database == "food",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.25,
                                        "input": ["food", "1"],
                                        "type": "technosphere",
                                        "uncertainty type": 0,
                                        "output": ["food", "2"],
                                    },
                                    "input_database": "food",
                                    "input_code": "1",
                                    "output_database": "food",
                                    "output_code": "2",
                                    "type": "technosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "2",
                        ExchangeDataset.output_code == "2",
                        ExchangeDataset.input_database == "biosphere",
                        ExchangeDataset.output_database == "food",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.15,
                                        "input": ["biosphere", "2"],
                                        "type": "biosphere",
                                        "uncertainty type": 0,
                                        "output": ["food", "2"],
                                    },
                                    "input_database": "biosphere",
                                    "input_code": "2",
                                    "output_database": "food",
                                    "output_code": "2",
                                    "type": "biosphere",
                                },
                            }
                        }
                    },
                },
            ],
        },
    ]

    assert [x[1] for x in revisions] == expected
    assert projects.dataset.revision == revisions[3][0]


@bw2test
def test_database_write_revision_apply(num_revisions):
    projects.set_current("activity-event")
    DatabaseChooser("biosphere").write(basic.biosphere, searchable=True)

    r1 = next(snowflake_id_generator)
    r2 = next(snowflake_id_generator)
    r3 = next(snowflake_id_generator)
    r4 = next(snowflake_id_generator)
    r5 = next(snowflake_id_generator)
    revisions = [
        {
            "metadata": {
                "parent_revision": None,
                "revision": r1,
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
                            "root['food']": {"depends": [], "backend": "sqlite"}
                        }
                    },
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": r1,
                "revision": r2,
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_database",
                    "id": "food",
                    "change_type": "database_reset",
                    "delta": {},
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": r2,
                "revision": r3,
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
                            "root['food']['geocollections']": ["world"],
                            "root['food']['searchable']": True,
                        }
                    },
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": r3,
                "revision": r4,
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_database",
                    "id": None,
                    "change_type": "database_metadata_change",
                    "delta": {"iterable_item_added": {"root['food']['depends'][0]": "biosphere"}},
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": r4,
                "revision": r5,
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_node",
                    "id": next(snowflake_id_generator),
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "categories": ["stuff", "meals"],
                                    "code": "1",
                                    "location": "CA",
                                    "name": "lunch",
                                    "type": "processwithreferenceproduct",
                                    "unit": "kg",
                                    "database": "food",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_node",
                    "id": next(snowflake_id_generator),
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "categories": ["stuff", "meals"],
                                    "code": "2",
                                    "location": "CH",
                                    "name": "dinner",
                                    "type": "processwithreferenceproduct",
                                    "unit": "kg",
                                    "database": "food",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": next(snowflake_id_generator),
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.5,
                                        "input": ["food", "2"],
                                        "type": "technosphere",
                                        "uncertainty type": 0,
                                        "output": ["food", "1"],
                                    },
                                    "input_database": "food",
                                    "input_code": "2",
                                    "output_database": "food",
                                    "output_code": "1",
                                    "type": "technosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": next(snowflake_id_generator),
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.05,
                                        "input": ["biosphere", "1"],
                                        "type": "biosphere",
                                        "uncertainty type": 0,
                                        "output": ["food", "1"],
                                    },
                                    "input_database": "biosphere",
                                    "input_code": "1",
                                    "output_database": "food",
                                    "output_code": "1",
                                    "type": "biosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": next(snowflake_id_generator),
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.25,
                                        "input": ["food", "1"],
                                        "type": "technosphere",
                                        "uncertainty type": 0,
                                        "output": ["food", "2"],
                                    },
                                    "input_database": "food",
                                    "input_code": "1",
                                    "output_database": "food",
                                    "output_code": "2",
                                    "type": "technosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": next(snowflake_id_generator),
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.15,
                                        "input": ["biosphere", "2"],
                                        "type": "biosphere",
                                        "uncertainty type": 0,
                                        "output": ["food", "2"],
                                    },
                                    "input_database": "biosphere",
                                    "input_code": "2",
                                    "output_database": "food",
                                    "output_code": "2",
                                    "type": "biosphere",
                                },
                            }
                        }
                    },
                },
            ],
        },
    ]

    for revision in revisions:
        projects.dataset.apply_revision(revision)
    assert projects.dataset.revision == r5

    assert not num_revisions(projects)
    assert "food" in databases

    node = get_node(code="1", database="food")
    assert node["categories"] == ["stuff", "meals"]
    assert node["location"] == "CA"
    assert len(node.exchanges()) == 2
    for exc in node.technosphere():
        assert exc["amount"] == 0.5
        assert exc.input["code"] == "2"
        assert not exc["uncertainty type"]

    node = get_node(code="2", database="food")
    assert node["categories"] == ["stuff", "meals"]
    assert node["location"] == "CH"
    assert len(node.exchanges()) == 2
    for exc in node.biosphere():
        assert exc["amount"] == 0.15
        assert exc.input["code"] == "2"
        assert not exc["uncertainty type"]

    assert len(DatabaseChooser("food")) == 2
    assert ExchangeDataset.select().where(ExchangeDataset.output_database == "food").count() == 4


@bw2test
def test_database_write_unsourced_project(num_revisions):
    projects.set_current("activity-event")
    DatabaseChooser("biosphere").write(basic.biosphere)
    DatabaseChooser("food").write(basic.food)

    assert projects.dataset.revision is None
    assert not num_revisions(projects)


@bw2test
def test_database_copy_revision_expected_format():
    projects.set_current("activity-event")
    DatabaseChooser("biosphere").write(basic.biosphere)
    DatabaseChooser("food").write(basic.food)

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    DatabaseChooser("food").copy("yum")

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
                            "root['yum']": {
                                "depends": ["biosphere"],
                                "backend": "sqlite",
                                "geocollections": ["world"],
                                "format": "Copied from 'food'",
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
                    "delta": {
                        "iterable_item_removed": {
                            "root['yum']['depends'][0]": "biosphere",
                            "root['yum']['geocollections'][0]": "world",
                        }
                    },
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": revisions[1][0],
                "revision": revisions[2][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_database",
                    "id": "yum",
                    "change_type": "database_reset",
                    "delta": {},
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": revisions[2][0],
                "revision": revisions[3][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_database",
                    "id": None,
                    "change_type": "database_metadata_change",
                    "delta": {"iterable_item_added": {"root['yum']['depends'][0]": "biosphere"}},
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": revisions[3][0],
                "revision": revisions[4][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_node",
                    "id": get_node(code="2", database="yum").id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "categories": ["stuff", "meals"],
                                    "code": "2",
                                    "location": "CH",
                                    "name": "dinner",
                                    "type": "processwithreferenceproduct",
                                    "unit": "kg",
                                    "database": "yum",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_node",
                    "id": get_node(code="1", database="yum").id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "categories": ["stuff", "meals"],
                                    "code": "1",
                                    "location": "CA",
                                    "name": "lunch",
                                    "type": "processwithreferenceproduct",
                                    "unit": "kg",
                                    "database": "yum",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "1",
                        ExchangeDataset.output_code == "2",
                        ExchangeDataset.input_database == "yum",
                        ExchangeDataset.output_database == "yum",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.25,
                                        "input": ["yum", "1"],
                                        "type": "technosphere",
                                        "uncertainty type": 0,
                                        "output": ["yum", "2"],
                                    },
                                    "input_database": "yum",
                                    "input_code": "1",
                                    "output_database": "yum",
                                    "output_code": "2",
                                    "type": "technosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "2",
                        ExchangeDataset.output_code == "2",
                        ExchangeDataset.input_database == "biosphere",
                        ExchangeDataset.output_database == "yum",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.15,
                                        "input": ["biosphere", "2"],
                                        "type": "biosphere",
                                        "uncertainty type": 0,
                                        "output": ["yum", "2"],
                                    },
                                    "input_database": "biosphere",
                                    "input_code": "2",
                                    "output_database": "yum",
                                    "output_code": "2",
                                    "type": "biosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "2",
                        ExchangeDataset.output_code == "1",
                        ExchangeDataset.input_database == "yum",
                        ExchangeDataset.output_database == "yum",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.5,
                                        "input": ["yum", "2"],
                                        "type": "technosphere",
                                        "uncertainty type": 0,
                                        "output": ["yum", "1"],
                                    },
                                    "input_database": "yum",
                                    "input_code": "2",
                                    "output_database": "yum",
                                    "output_code": "1",
                                    "type": "technosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "1",
                        ExchangeDataset.output_code == "1",
                        ExchangeDataset.input_database == "biosphere",
                        ExchangeDataset.output_database == "yum",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.05,
                                        "input": ["biosphere", "1"],
                                        "type": "biosphere",
                                        "uncertainty type": 0,
                                        "output": ["yum", "1"],
                                    },
                                    "input_database": "biosphere",
                                    "input_code": "1",
                                    "output_database": "yum",
                                    "output_code": "1",
                                    "type": "biosphere",
                                },
                            }
                        }
                    },
                },
            ],
        },
    ]

    assert [x[1] for x in revisions][:-1] == expected[:-1]
    assert revisions[-1][1]["metadata"] == expected[-1]["metadata"]
    for x in range(2):
        assert revisions[-1][1]["data"][x] in expected[-1]["data"][:2]
    for x in range(2, 5):
        assert revisions[-1][1]["data"][x] in expected[-1]["data"][2:]
    assert projects.dataset.revision == revisions[4][0]


@bw2test
def test_database_rename_revision_expected_format():
    projects.set_current("activity-event")
    DatabaseChooser("biosphere").write(basic.biosphere)
    DatabaseChooser("food").write(basic.food)

    projects.dataset.set_sourced()
    assert projects.dataset.revision is None

    DatabaseChooser("food").rename("yum")

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
                            "root['yum']": {
                                "depends": ["biosphere"],
                                "backend": "sqlite",
                                "geocollections": ["world"],
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
                    "id": "yum",
                    "change_type": "database_reset",
                    "delta": {},
                }
            ],
        },
        {
            "metadata": {
                "parent_revision": revisions[1][0],
                "revision": revisions[2][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_node",
                    "id": get_node(code="2", database="yum").id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "categories": ["stuff", "meals"],
                                    "code": "2",
                                    "location": "CH",
                                    "name": "dinner",
                                    "type": "processwithreferenceproduct",
                                    "unit": "kg",
                                    "database": "yum",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_node",
                    "id": get_node(code="1", database="yum").id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "categories": ["stuff", "meals"],
                                    "code": "1",
                                    "location": "CA",
                                    "name": "lunch",
                                    "type": "processwithreferenceproduct",
                                    "unit": "kg",
                                    "database": "yum",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "1",
                        ExchangeDataset.output_code == "2",
                        ExchangeDataset.input_database == "yum",
                        ExchangeDataset.output_database == "yum",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.25,
                                        "input": ["yum", "1"],
                                        "type": "technosphere",
                                        "uncertainty type": 0,
                                        "output": ["yum", "2"],
                                    },
                                    "input_database": "yum",
                                    "input_code": "1",
                                    "output_database": "yum",
                                    "output_code": "2",
                                    "type": "technosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "2",
                        ExchangeDataset.output_code == "2",
                        ExchangeDataset.input_database == "biosphere",
                        ExchangeDataset.output_database == "yum",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.15,
                                        "input": ["biosphere", "2"],
                                        "type": "biosphere",
                                        "uncertainty type": 0,
                                        "output": ["yum", "2"],
                                    },
                                    "input_database": "biosphere",
                                    "input_code": "2",
                                    "output_database": "yum",
                                    "output_code": "2",
                                    "type": "biosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "2",
                        ExchangeDataset.output_code == "1",
                        ExchangeDataset.input_database == "yum",
                        ExchangeDataset.output_database == "yum",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.5,
                                        "input": ["yum", "2"],
                                        "type": "technosphere",
                                        "uncertainty type": 0,
                                        "output": ["yum", "1"],
                                    },
                                    "input_database": "yum",
                                    "input_code": "2",
                                    "output_database": "yum",
                                    "output_code": "1",
                                    "type": "technosphere",
                                },
                            }
                        }
                    },
                },
                {
                    "type": "lci_edge",
                    "id": ExchangeDataset.get(
                        ExchangeDataset.input_code == "1",
                        ExchangeDataset.output_code == "1",
                        ExchangeDataset.input_database == "biosphere",
                        ExchangeDataset.output_database == "yum",
                    ).id,
                    "change_type": "create",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "data": {
                                        "amount": 0.05,
                                        "input": ["biosphere", "1"],
                                        "type": "biosphere",
                                        "uncertainty type": 0,
                                        "output": ["yum", "1"],
                                    },
                                    "input_database": "biosphere",
                                    "input_code": "1",
                                    "output_database": "yum",
                                    "output_code": "1",
                                    "type": "biosphere",
                                },
                            }
                        }
                    },
                },
            ],
        },
        {
            "metadata": {
                "parent_revision": revisions[2][0],
                "revision": revisions[3][0],
                "authors": "Anonymous",
                "title": "Untitled revision",
                "description": "No description",
            },
            "data": [
                {
                    "type": "lci_database",
                    "id": "food",
                    "change_type": "database_delete",
                    "delta": {},
                }
            ],
        },
    ]

    assert [x[1] for x in revisions][:-2] == expected[:-2]
    assert revisions[-1][1] == expected[-1]
    assert revisions[-2][1]["metadata"] == expected[-2]["metadata"]
    for x in range(2):
        assert revisions[-2][1]["data"][x] in expected[-2]["data"][:2]
    for x in range(2, 5):
        assert revisions[-2][1]["data"][x] in expected[-2]["data"][2:]
    assert projects.dataset.revision == revisions[3][0]
