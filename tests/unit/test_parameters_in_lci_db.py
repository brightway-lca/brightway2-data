"""Parameters in ``lci/databases.db``: ``parameters.db`` is merged into the LCI file by schema
migration 9, and parameters are written in the same file (and transactions) as the LCI data."""

import pickle
import sqlite3
import warnings

import pytest

from bw2data import Database, get_node, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import _M9_TABLES, migrate, schema_version
from bw2data.backends.schema import ExchangeDataset
from bw2data.parameters import (
    PARAMETER_MODELS,
    ActivityParameter,
    DatabaseParameter,
    Group,
    GroupDependency,
    ParameterizedExchange,
    ProjectParameter,
    parameters,
)
from bw2data.tests import bw2test

TABLES = [table for table, _ in _M9_TABLES]


def objects(type_):
    return {
        row[0]
        for row in sqlite3_lci_db.execute_sql(
            "SELECT name FROM main.sqlite_master WHERE type = ?", (type_,)
        ).fetchall()
    }


def as_version_8():
    """As a file before migration 9: without the parameter tables."""
    for table in TABLES:
        sqlite3_lci_db.execute_sql(f'DROP TABLE IF EXISTS main."{table}"')
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 8")


def legacy_file(path, exchange_id, exchange_group="g"):
    """A ``parameters.db`` as earlier versions wrote it."""
    conn = sqlite3.connect(path)
    for _, sql in _M9_TABLES:
        conn.execute(sql)
    data = pickle.dumps({"uncertainty type": 2, "loc": 1.0}, protocol=4)
    conn.execute("INSERT INTO projectparameter VALUES (11, 'p', NULL, 2.0, ?)", (data,))
    conn.execute(
        "INSERT INTO databaseparameter VALUES (12, 'db', 'd', 'p * 3', 6.0, ?)",
        (pickle.dumps({}),),
    )
    conn.execute(
        "INSERT INTO activityparameter VALUES (13, 'g', 'db', 'a', 'x', 'd + 1', 7.0, ?)",
        (pickle.dumps({}),),
    )
    conn.execute(
        "INSERT INTO parameterizedexchange VALUES (14, ?, ?, 'x * 2')",
        (exchange_group, exchange_id),
    )
    conn.execute("INSERT INTO groupdependency VALUES (1, 'g', 'db')")
    for id_, name in ((15, "project"), (16, "db"), (17, "g")):
        conn.execute(
            "INSERT INTO group_table VALUES (?, ?, 1, '2020-01-02 03:04:05', ?)",
            (id_, name, pickle.dumps([])),
        )
    conn.commit()
    conn.close()


def write_activity():
    Database("db").write(
        {
            ("db", "a"): {
                "name": "a",
                "exchanges": [{"input": ("db", "a"), "amount": 1, "type": "production"}],
            }
        }
    )
    return get_node(database="db", code="a")


@bw2test
def test_parameters_are_in_the_lci_file():
    assert parameters.db is sqlite3_lci_db
    for model in PARAMETER_MODELS:
        assert model._meta.database is sqlite3_lci_db.db
    assert set(TABLES) <= objects("table")
    assert {"projectparameter_INSERT_trigger", "pe_group_INSERT", "gd_circular_UPDATE"} <= objects(
        "trigger"
    )
    ProjectParameter.create(name="p", amount=1)
    assert not (projects.dir / "parameters.db").exists()


@bw2test
def test_parameters_and_lci_data_share_transactions():
    node = write_activity()
    with pytest.raises(RuntimeError):
        with sqlite3_lci_db.atomic():
            ProjectParameter.create(name="p", amount=1)
            node["name"] = "changed"
            node.save()
            raise RuntimeError
    assert not ProjectParameter.select().exists()
    assert get_node(id=node.id)["name"] == "a"


@bw2test
def test_parameters_belong_to_their_project():
    ProjectParameter.create(name="p", amount=1)
    name = projects.current
    projects.set_current("other")
    assert not ProjectParameter.select().exists()
    projects.set_current(name)
    assert ProjectParameter.get(name="p").amount == 1


@bw2test
def test_triggers_still_refuse():
    write_activity()
    Group.create(name="g")
    ActivityParameter.create(group="g", database="db", code="a", name="x", amount=1)
    with pytest.raises(Exception, match="Cross database group"):
        ActivityParameter.create(group="g", database="other", code="b", name="y", amount=1)
    GroupDependency.create(group="g", depends="h")
    with pytest.raises(Exception, match="Circular dependency"):
        GroupDependency.create(group="h", depends="g")


# Migration 9


@bw2test
def test_migration_moves_the_rows():
    node = write_activity()
    exchange = ExchangeDataset.get(ExchangeDataset.output_code == "a")
    legacy_file(projects.dir / "parameters.db", exchange.id)
    as_version_8()
    assert migrate(sqlite3_lci_db.db, projects.dir) == [9, 10]
    assert schema_version(sqlite3_lci_db.db) == 10

    assert ProjectParameter.get(name="p").id == 11
    assert ProjectParameter.get(name="p").data == {"uncertainty type": 2, "loc": 1.0}
    assert DatabaseParameter.get(name="d").formula == "p * 3"
    assert ActivityParameter.get(name="x").code == "a"
    assert ParameterizedExchange.get(id=14).exchange == exchange.id
    assert GroupDependency.get(group="g").depends == "db"
    # Copied before the triggers exist: the groups keep their times
    assert {str(g.updated) for g in Group.select()} == {"2020-01-02 03:04:05"}
    # The triggers work afterwards
    assert {"activityparameter_UPDATE_trigger", "ap_crossgroup_INSERT"} <= objects("trigger")
    # And so do the parameters: the copied groups are fresh, so expire them first
    Group.update(fresh=False).execute()
    parameters.recalculate()
    assert DatabaseParameter.get(name="d").amount == 6
    assert ActivityParameter.get(name="x").amount == 7
    assert ExchangeDataset.get(id=exchange.id).data["amount"] == 14
    # The file is kept as a backup
    assert (projects.dir / "parameters.db").is_file()


@bw2test
@pytest.mark.parametrize("exchange_group", ["g", "no such group"])
def test_migration_when_the_project_is_opened(exchange_group):
    """Opening a project creates the parameter tables with their triggers before the
    migrations run. The rows are still copied without them: the groups keep their times, and
    a row the triggers would refuse (a parameterized exchange whose group has no activity
    parameter) doesn't stop the copy."""
    write_activity()
    exchange = ExchangeDataset.get(ExchangeDataset.output_code == "a")
    legacy_file(projects.dir / "parameters.db", exchange.id, exchange_group)
    as_version_8()
    name = projects.current
    projects.set_current("other")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        projects.set_current(name)
    assert schema_version(sqlite3_lci_db.db) == 9

    assert ProjectParameter.get(name="p").id == 11
    assert ActivityParameter.get(name="x").code == "a"
    assert ParameterizedExchange.get(id=14).group == exchange_group
    assert GroupDependency.get(group="g").depends == "db"
    assert {g.name for g in Group.select()} == {"project", "db", "g"}
    assert {str(g.updated) for g in Group.select()} == {"2020-01-02 03:04:05"}
    # The triggers are there again
    assert {"activityparameter_UPDATE_trigger", "pe_group_INSERT", "gd_circular_UPDATE"} <= objects(
        "trigger"
    )
    with pytest.raises(Exception, match="Missing activity parameter group"):
        ParameterizedExchange.create(group="another", exchange=1, formula="1")


@bw2test
def test_failed_copy_leaves_no_rows():
    write_activity()
    exchange = ExchangeDataset.get(ExchangeDataset.output_code == "a")
    path = projects.dir / "parameters.db"
    legacy_file(path, exchange.id)
    # The last table to be copied has an id which can't be stored
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE group_table")
    conn.execute('CREATE TABLE group_table (id TEXT, name TEXT, fresh, updated, "order")')
    conn.execute("INSERT INTO group_table VALUES ('x', 'g', 1, '2020-01-02 03:04:05', x'00')")
    conn.commit()
    conn.close()
    as_version_8()
    with pytest.warns(UserWarning, match="Can't copy the parameters .* only kept in the file"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == [9]
    for model in PARAMETER_MODELS:
        assert not model.select().exists()
    # The tables can be used
    assert {"projectparameter_INSERT_trigger", "pe_group_INSERT"} <= objects("trigger")
    ProjectParameter.create(name="p", amount=1)


@bw2test
def test_rows_which_are_already_there_are_kept():
    write_activity()
    exchange = ExchangeDataset.get(ExchangeDataset.output_code == "a")
    legacy_file(projects.dir / "parameters.db", exchange.id)
    as_version_8()
    for _, sql in _M9_TABLES:
        sqlite3_lci_db.execute_sql(sql)
    sqlite3_lci_db.execute_sql(
        "INSERT INTO main.projectparameter VALUES (11, 'p', NULL, 5.0, ?)", (pickle.dumps({}),)
    )
    assert migrate(sqlite3_lci_db.db, projects.dir) == [9, 10]
    assert ProjectParameter.get(id=11).amount == 5


@bw2test
def test_migration_without_a_file():
    as_version_8()
    assert migrate(sqlite3_lci_db.db, projects.dir) == [9, 10]
    assert set(TABLES) <= objects("table")
    assert not ProjectParameter.select().exists()


@bw2test
def test_unreadable_file():
    (projects.dir / "parameters.db").write_bytes(b"not a database " * 100)
    as_version_8()
    with pytest.warns(UserWarning, match="Can't copy the parameters .* only kept in the file"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == [9, 10]
    assert set(TABLES) <= objects("table")


@bw2test
def test_locked_file_postpones_the_step():
    write_activity()
    exchange = ExchangeDataset.get(ExchangeDataset.output_code == "a")
    path = projects.dir / "parameters.db"
    legacy_file(path, exchange.id)
    as_version_8()
    lock = sqlite3.connect(path, isolation_level=None)
    lock.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.warns(UserWarning, match="locked by another process"):
            assert migrate(sqlite3_lci_db.db, projects.dir) == []
        assert schema_version(sqlite3_lci_db.db) == 8
    finally:
        lock.execute("ROLLBACK")
        lock.close()
    assert migrate(sqlite3_lci_db.db, projects.dir) == [9, 10]
    assert ProjectParameter.get(name="p").amount == 2
