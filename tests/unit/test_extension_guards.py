"""Changes to the data of read-only LCI extensions are refused in Python, before any SQL runs
(extensions PR 5), and what is allowed: reading, copying into the project, processing and
searching."""

import pytest
from peewee import IntegrityError

from bw2data import Database, calculation_setups, databases, get_node, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.schema import ExtensionDatabaseState
from bw2data.errors import ReadOnlyExtensionError
from bw2data.parameters import DatabaseParameter, parameters

from bw2data.search import IndexManager
from bw2data.search.indices import index_table

from .test_extension_attach import edit, files  # noqa: F401


@pytest.fixture
def attached(files):
    projects.attach_extension(list(files))
    Database("mine").write(
        {
            ("mine", "x"): {
                "name": "x",
                "unit": "kg",
                "exchanges": [
                    {"input": ("mine", "x"), "amount": 1, "type": "production"},
                    {"input": ("inv", "b"), "amount": 2, "type": "technosphere"},
                ],
            }
        }
    )
    return files


def local_state():
    return {row.name: row for row in ExtensionDatabaseState.select()}


def state_values():
    return sorted(
        (row.name, row.dirty, row.searchable, row.modified, row.processed)
        for row in ExtensionDatabaseState.select()
    )


def refused(match="read-only extension"):
    return pytest.raises(ReadOnlyExtensionError, match=match)


def test_error_class():
    assert issubclass(ReadOnlyExtensionError, IntegrityError)


def test_introspection(attached):
    assert Database("inv").is_extension
    assert Database("inv").extension.name == "inv"
    assert Database("bio").extension.id == "bio-id"
    assert not Database("mine").is_extension
    assert Database("mine").extension is None
    assert not Database("unknown").is_extension


# Databases


def test_database_changes_refused(attached):
    before = local_state()
    with refused("'inv' is stored in the read-only extension 'inv' .* can't be written"):
        Database("inv").write({})
    with refused("can't be deleted"):
        Database("inv").delete(warn=False)
    with refused("can't be renamed"):
        Database("inv").rename("other")
    with refused("can't get new nodes"):
        Database("inv").new_node("new", name="new")
    with refused("can't get new nodes"):
        Database("inv").new_activity("new")
    with refused():
        Database("inv").delete_duplicate_exchanges()
    with refused("can't be written"):
        Database("inv").register()
    # Nothing changed, also not the local state of the records (``dirty``, ``modified``)
    assert local_state().keys() == before.keys()
    assert len(Database("inv")) == 2
    assert sorted(databases) == ["bio", "inv", "mine"]


def test_extension_names_are_not_free(attached):
    with refused("its name can't be used for a database of the project"):
        Database("mine").rename("inv")
    with refused("its name can't be used"):
        Database("mine").copy("bio")
    with refused("its metadata can't be replaced"):
        databases["bio"] = {"backend": "sqlite"}
    assert sorted(databases) == ["bio", "inv", "mine"]
    assert len(Database("mine")) == 1


def test_del_databases_refused_before_anything_changes(attached):
    node = get_node(database="inv", code="a")
    calculation_setups["setup"] = {"inv": [{node.id: 1}, {("mine", "x"): 1}], "ia": []}
    with refused("can't be deleted; `projects.detach_extension` removes the extension"):
        del databases["inv"]
    assert calculation_setups["setup"]["inv"] == [{node.id: 1}, {("mine", "x"): 1}]
    assert "inv" in databases


def test_metadata(attached):
    metadata = databases["inv"]
    metadata["dirty"] = True
    metadata["searchable"] = False
    metadata["processed"] = "then"
    metadata.save()
    assert databases["inv"]["dirty"] is True
    assert databases["inv"]["processed"] == "then"
    state = local_state()["inv"]
    assert (state.dirty, state.searchable, state.processed) == (True, False, "then")

    metadata["description"] = "mine now"
    metadata["dirty"] = False
    with refused(
        "only its keys dirty, modified, processed, searchable can be changed, not "
        "description. The unsaved changes of its metadata were discarded"
    ):
        metadata.save()
    assert "description" not in databases["inv"]
    assert databases["inv"]["dirty"] is True
    # The project's own databases are not affected
    databases["mine"]["description"] = "mine"
    databases["mine"].save()
    assert databases["mine"]["description"] == "mine"


def test_replaying_metadata_leaves_extension_records_alone(attached):
    databases["inv"]["dirty"] = True
    databases["inv"].save()
    data = databases.data
    data["mine"]["description"] = "replayed"
    data["inv"]["description"] = "not for an extension"
    del data["bio"]
    databases.data = data
    assert sorted(databases) == ["bio", "inv", "mine"]
    assert databases["mine"]["description"] == "replayed"
    assert "description" not in databases["inv"]
    assert databases["inv"]["dirty"] is True


# Nodes and edges


def test_node_changes_refused(attached):
    node = get_node(database="inv", code="a")
    calculation_setups["setup"] = {"inv": [{node.id: 1}], "ia": []}
    before = local_state()

    node["name"] = "changed"
    with refused("its nodes can't be saved"):
        node.save()
    with refused("its nodes can't be deleted"):
        node.delete()
    with refused("its nodes can't be changed"):
        node["code"] = "other"
    with refused("its nodes can't be moved to another database"):
        node["database"] = "mine"
    new = Database("mine").new_node("new", name="new", unit="kg")
    with refused("can't get new nodes"):
        new["database"] = "inv"
    with refused("its nodes can't be saved"):
        get_node(database="mine", code="x").copy(database="inv")

    assert local_state().keys() == before.keys()
    assert calculation_setups["setup"]["inv"] == [{node.id: 1}]
    assert get_node(database="inv", code="a")["name"] == "a"
    assert len(Database("inv")) == 2


def test_moving_a_node_of_the_project_into_an_extension_refused(attached):
    node = get_node(database="mine", code="x")
    with refused("can't get new nodes"):
        node["database"] = "bio"
    assert get_node(database="mine", code="x")["database"] == "mine"


def test_edge_changes_refused(attached):
    before = state_values()
    node = get_node(database="inv", code="b")
    edge = next(iter(node.technosphere()))
    edge["amount"] = 7
    with refused("its edges can't be saved"):
        edge.save()
    with refused("its edges can't be deleted"):
        edge.delete()
    with refused("its edges can't be saved"):
        node.new_edge(input=("bio", "1"), amount=1, type="biosphere").save()
    with refused("its edges can't be deleted"):
        node.exchanges().delete()
    assert [e["amount"] for e in node.technosphere()] == [0.5]
    assert len(node.exchanges()) == 2
    assert state_values() == before


def test_moving_an_edge_out_of_an_extension_refused(attached):
    Database("mine").process()
    edge = next(iter(get_node(database="inv", code="b").technosphere()))
    edge["output"] = ("mine", "x")
    with refused("'inv' is stored in the read-only extension .* its edges can't be saved"):
        edge.save()
    # Refused before the project's database is marked as changed
    assert not databases["mine"].get("dirty")
    assert len(get_node(database="inv", code="b").exchanges()) == 2


def test_upstream_edges_are_deleted_where_they_are_stored(attached):
    # Edges of the project into a node of an extension are the project's
    node = get_node(database="inv", code="b")
    Database("mine").process()
    before = state_values()
    assert [e.output.key for e in node.upstream()] == [("mine", "x")]
    node.upstream().delete()
    assert len(node.upstream()) == 0
    assert databases["mine"]["dirty"] is True
    assert state_values() == before
    # Edges of an extension into a node of an extension are not
    node = get_node(database="inv", code="a")
    assert [e.output.key for e in node.upstream()] == [("inv", "b")]
    with refused("its edges can't be deleted"):
        node.upstream().delete()
    assert len(node.upstream()) == 1


def test_bulk_insert_trigger_errors_are_read_only_extension_errors(attached):
    from bw2data.backends.schema import ActivityDataset, _insert_many

    node = get_node(database="bio", code="0")
    row = {"id": node.id, "database": "mine", "code": "same id", "data": {}}
    with refused("bw2data-extension: this id is already used in a read-only extension"):
        with sqlite3_lci_db.atomic():
            _insert_many(ActivityDataset, [row])


def test_io_table_exchanges_refused(attached):
    from bw2data.backends.iotable import IOTableBackend

    with refused("can't be written"):
        IOTableBackend("inv").write_exchanges([], [], [])
    database = Database("inv")
    assert not (database.dirpath_processed() / database.filename_processed()).exists()


def test_edges_from_the_project_into_extensions_are_allowed(attached):
    node = get_node(database="mine", code="x")
    node.new_edge(input=("bio", "2"), amount=3, type="biosphere").save()
    edge = next(iter(node.biosphere()))
    edge["amount"] = 4
    edge.save()
    assert [e["amount"] for e in node.biosphere()] == [4]
    edge.delete()
    assert len(node.biosphere()) == 0


def test_trigger_errors_are_read_only_extension_errors(attached):
    from bw2data.backends.schema import ActivityDataset

    with refused("bw2data-extension: this database is stored in a read-only extension"):
        ActivityDataset.delete().where(ActivityDataset.database == "bio").execute()
    # Other integrity errors are left as they are
    with pytest.raises(IntegrityError) as caught:
        sqlite3_lci_db.execute_sql("INSERT INTO main.databaserecord (name) VALUES ('mine')")
    assert not isinstance(caught.value, ReadOnlyExtensionError)


def test_refused_changes_write_no_revision(attached):
    projects.dataset.set_sourced()
    revisions = projects.dataset.dir / "revisions"
    before = sorted(revisions.iterdir()) if revisions.exists() else []
    node = get_node(database="inv", code="a")
    node["name"] = "changed"
    with refused():
        node.save()
    with refused():
        node.delete()
    with refused():
        databases["inv"]["description"] = "x"
        databases["inv"].save()
    after = sorted(revisions.iterdir()) if revisions.exists() else []
    assert after == before


# What is allowed


def test_copy_into_the_project(attached):
    copy = Database("inv").copy("my inv")
    assert not copy.is_extension
    assert len(copy) == 2
    node = get_node(database="my inv", code="b")
    assert {e.input.key for e in node.technosphere()} == {("my inv", "a")}
    assert "format" in databases["my inv"]
    new = get_node(database="inv", code="a").copy(database="mine", code="a copy")
    assert new.key == ("mine", "a copy")
    assert {e.input.key for e in new.biosphere()} == {("bio", "0")}


def test_process_and_calculate(attached):
    import bw2calc

    depends = databases["inv"]["depends"]
    # Marked to be processed when attached, whatever the record in the file says
    assert databases["inv"]["dirty"] is True
    assert databases["bio"]["dirty"] is True
    Database("bio").process()
    Database("inv").process()
    metadata = databases["inv"]
    assert metadata["dirty"] is False
    assert metadata["processed"]
    assert metadata["depends"] == depends
    assert local_state()["inv"].processed == metadata["processed"]
    for name in ("bio", "inv", "mine"):
        assert Database(name).filepath_processed().is_file()

    lca = bw2calc.LCA({get_node(database="mine", code="x"): 1})
    lca.lci()
    # 2 b, 1 a per b: 1 a, and 2 kg of flow 0 per a
    assert lca.inventory.sum() == pytest.approx(2)


def test_new_version_of_a_file_is_processed_again(attached):
    import json
    import sqlite3

    import bw2calc

    from bw2data.backends.extensions import database_fingerprint

    def inventory():
        lca = bw2calc.LCA({get_node(database="inv", code="b"): 1})
        lca.lci()
        return lca.inventory.sum()

    # Processed as needed, without ``process()``
    assert inventory() == pytest.approx(1)
    assert databases["inv"]["dirty"] is False
    assert databases["inv"]["processed"]

    inv = attached[1]
    Database("mine").delete(warn=False)
    projects.detach_extension("inv")
    assert sorted(local_state()) == ["bio"]

    conn = sqlite3.connect(inv)
    conn.execute("DELETE FROM exchangedataset WHERE type = 'biosphere'")
    fingerprints = {"inv": database_fingerprint(conn, "main", "inv")}
    conn.execute(
        "UPDATE extension_info SET value = ? WHERE key = 'fingerprints'",
        (json.dumps(fingerprints),),
    )
    conn.commit()
    conn.close()

    projects.attach_extension(inv)
    state = local_state()["inv"]
    assert (state.dirty, state.processed) == (True, None)
    assert inventory() == 0
    # ``bio`` stayed attached, and was not marked again
    assert databases["bio"]["dirty"] is False


# Search: the index is read from the extension's file


def main_tables():
    return {
        row[0]
        for row in sqlite3_lci_db.execute_sql("SELECT name FROM main.sqlite_master").fetchall()
    }


def codes(database, term):
    return {node["code"] for node in Database(database).search(term)}


def test_search_reads_the_index_in_the_extension(attached):
    assert databases["bio"]["searchable"] is True
    index = IndexManager("bio")
    assert index.read_only
    assert index.schema == Database("bio").extension.schema
    assert codes("bio", "flow") == {"0", "1", "2"}
    assert codes("inv", "b") == {"b"}
    assert [r["code"] for r in Database("bio").search("flow 1", proxy=False)] == ["1"]
    # Nothing is built in the project
    Database("bio").make_searchable(reset=True)
    assert index_table("bio") not in main_tables()
    assert codes("bio", "flow") == {"0", "1", "2"}
    # The project's own databases are searched as before
    assert codes("mine", "x") == {"x"}


def test_unsearchable_extension_database(attached):
    Database("bio").make_unsearchable()
    assert databases["bio"]["searchable"] is False
    assert local_state()["bio"].searchable is False
    assert not IndexManager("bio").read_only
    assert codes("bio", "flow") == set()
    Database("bio").make_searchable()
    assert codes("bio", "flow") == {"0", "1", "2"}
    assert index_table("bio") not in main_tables()


def test_index_in_the_extension_cant_be_changed(attached):
    node = get_node(database="bio", code="0")
    index = IndexManager("bio")
    for change in (
        lambda: index.add([node.id]),
        lambda: index.remove([node.id]),
        index.add_all,
        index.create,
    ):
        with refused("search index of database 'bio' is stored in a read-only extension"):
            change()
    # Dropping only drops an index in the project
    index.delete_database()
    assert codes("bio", "flow") == {"0", "1", "2"}


def test_extension_without_index(files):
    bio, inv = files
    edit(bio, f'DROP TABLE "{index_table("bio")}"')
    projects.attach_extension(list(files))
    assert codes("bio", "flow") == set()
    Database("bio").make_searchable(reset=True)
    assert index_table("bio") in main_tables()
    assert not IndexManager("bio").read_only
    assert codes("bio", "flow") == {"0", "1", "2"}
    projects.detach_extension("inv")
    projects.detach_extension("bio")
    assert index_table("bio") not in main_tables()


def test_copy_of_an_extension_database_is_indexed_in_the_project(attached):
    Database("bio").copy("my bio")
    assert codes("my bio", "flow") == {"0", "1", "2"}
    assert index_table("my bio") in main_tables()


def test_parameters_are_kept(attached):
    parameters.new_database_parameters([{"name": "p", "amount": 1}], "inv")
    with refused():
        Database("inv").delete(warn=False)
    with refused():
        del databases["inv"]
    assert [p.name for p in DatabaseParameter.select()] == ["p"]
