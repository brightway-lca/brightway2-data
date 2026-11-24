import pytest

from bw2data import databases, get_node, projects
from bw2data.backends import SQLiteBackend, sqlite3_lci_db
from bw2data.backends.schema import ActivityDataset
from bw2data.errors import UnknownObject
from bw2data.search import IndexManager
from bw2data.search import Searcher as _Searcher
from bw2data.search.indices import index_table
from bw2data.tests import bw2test


class Searcher(_Searcher):
    """Without the node ids in the results with ``proxy=False``, which differ in every run."""

    def search(self, *args, **kwargs):
        results = super().search(*args, **kwargs)
        if kwargs.get("proxy", True):
            return results
        groups = results.values() if isinstance(results, dict) else [results]
        for group in groups:
            for result in group:
                assert isinstance(result.pop("id"), int)
        return results


def add(*datasets):
    """Store ``datasets`` as nodes of the database ``foo``. Saving a node indexes it: the index
    refers to nodes by id, so only stored nodes can be indexed."""
    if "foo" not in databases:
        SQLiteBackend("foo").register()
    for ds in datasets:
        # Not ``new_node``, which adds a default location
        SQLiteBackend.node_class(**{"type": "process", "name": "unnamed", **ds}).save()


@bw2test
def test_search_dataset_containing_stop_word():
    add({"database": "foo", "code": "bar", "name": "foo of bar, high voltage"})
    with Searcher("foo") as s:
        assert s.search("foo of bar, high voltage", proxy=False)


@bw2test
def test_add_dataset():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_search_dataset():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "lollipop",
                "database": "foo",
                "location": "",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ]


@bw2test
def test_search_geocollection_location():
    add(
        {
            "database": "foo",
            "code": "bar",
            "name": "lollipop",
            "location": ("foo", "Here"),
        }
    )
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "lollipop",
                "database": "foo",
                "location": "here",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ]


@bw2test
def test_update_dataset():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    node = get_node(database="foo", code="bar")
    node["name"] = "lemon cake"
    node.save()
    with Searcher("foo") as s:
        assert not s.search("lollipop", proxy=False)
        assert s.search("lemon", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "lemon cake",
                "database": "foo",
                "location": "",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ]


@bw2test
def test_delete_dataset():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)
    get_node(database="foo", code="bar").delete()
    with Searcher("foo") as s:
        assert not s.search("lollipop", proxy=False)


@bw2test
def test_add_datasets():
    ds = [{"database": "foo", "code": "bar", "name": "lollipop"}]
    add(*ds)
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_add_database():
    db = SQLiteBackend("foo")
    ds = {("foo", "bar"): {"database": "foo", "code": "bar", "name": "lollipop"}}
    db.write(ds)
    with Searcher(db.filename) as s:
        assert s.search("lollipop", proxy=False)
    db.make_unsearchable()
    with Searcher(db.filename) as s:
        assert not s.search("lollipop", proxy=False)


@bw2test
def test_add_searchable_database():
    db = SQLiteBackend("foo")
    ds = {("foo", "bar"): {"database": "foo", "code": "bar", "name": "lollipop"}}
    db.write(ds)
    with Searcher(db.filename) as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_modify_database():
    db = SQLiteBackend("foo")
    ds = {("foo", "bar"): {"database": "foo", "code": "bar", "name": "lollipop"}}
    db.write(ds)
    with Searcher(db.filename) as s:
        assert not s.search("cream", proxy=False)
        assert s.search("lollipop", proxy=False)
    ds2 = {("foo", "bar"): {"database": "foo", "code": "bar", "name": "ice cream"}}
    db.write(ds2)
    with Searcher(db.filename) as s:
        assert s.search("cream", proxy=False)


@bw2test
def test_searchable_database():
    db = SQLiteBackend("foo")
    ds = {("foo", "bar"): {"database": "foo", "code": "bar", "name": "lollipop"}}
    db.write(ds)
    with Searcher(db.filename) as s:
        assert s.search("lollipop", proxy=False)
    db.make_unsearchable()
    with Searcher(db.filename) as s:
        assert not s.search("lollipop", proxy=False)
    db.make_searchable()
    with Searcher(db.filename) as s:
        assert s.search("lollipop", proxy=False)
    del databases["foo"]
    with Searcher(db.filename) as s:
        assert not s.search("lollipop", proxy=False)


@bw2test
def test_delete_database():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    im = IndexManager("foo")
    assert im.exists()
    im.delete_database()
    assert not im.exists()
    with Searcher("foo") as s:
        assert not s.search("lollipop", proxy=False)
    # The index is created again when a node is added
    im.add_dataset({"database": "foo", "code": "bar"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_reset_index():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    IndexManager("foo").create()
    with Searcher("foo") as s:
        assert not s.search("lollipop", proxy=False)


@bw2test
def test_basic_search():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_product_term():
    add({"database": "foo", "code": "bar", "reference product": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_comment_term():
    add({"database": "foo", "code": "bar", "comment": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_categories_term():
    add({"database": "foo", "code": "bar", "categories": ("lollipop",)})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_limit():
    add(
        *[
            {"database": "foo", "code": "bar{}".format(x), "name": "lollipop {}".format(x)}
            for x in range(50)
        ]
    )
    with Searcher("foo") as s:
        assert len(s.search("lollipop", limit=25, proxy=False)) == 25


@bw2test
def test_star_search():
    add(
        *[
            {"database": "foo", "code": "bar{}".format(x), "name": "lollipop {}".format(x)}
            for x in range(50)
        ]
    )
    with Searcher("foo") as s:
        assert len(s.search("*", limit=25, proxy=False)) == 25


@bw2test
def test_search_faceting():
    ds = [
        {"database": "foo", "code": "bar", "name": "lollipop", "location": "CH"},
        {"database": "foo", "code": "baz", "name": "ice lollipop", "location": "FR"},
    ]
    add(*ds)
    with Searcher("foo") as s:
        res = s.search("lollipop", proxy=False, facet="location")
    assert res == {
        "fr": [
            {
                "comment": "",
                "product": "",
                "name": "ice lollipop",
                "database": "foo",
                "location": "fr",
                "code": "baz",
                "categories": "",
                "synonyms": "",
            }
        ],
        "ch": [
            {
                "comment": "",
                "product": "",
                "name": "lollipop",
                "database": "foo",
                "location": "ch",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ],
    }


@bw2test
def test_copy_save_propogates_to_search_index():
    db = SQLiteBackend("foo")
    ds = {("foo", "bar"): {"database": "foo", "code": "bar", "name": "lollipop"}}
    db.write(ds)
    assert db.search("lollipop")
    cp = db.get("bar").copy(code="baz")
    cp["name"] = "candy"
    cp.save()
    assert db.search("candy")


@bw2test
def test_case_sensitivity_convert_lowercase():
    db = SQLiteBackend("foo")
    ds = {("foo", "bar"): {"database": "foo", "code": "bar", "name": "LOLLIpop"}}
    db.write(ds)
    assert db.search("LOLLIpop".lower())
    assert db.search("lollipop")
    assert db.search("LOLLipop")
    assert db.search("LOLL*")
    assert db.search("Lollipop")
    assert not db.search("nope")
    assert not db.search("Lolipop")


@bw2test
def test_synonym_search():
    add(
        {
            "database": "foo",
            "code": "bar",
            "name": "polytetrafluoroethylene",
            "synonyms": ["PTFE", "Teflon"],
        }
    )
    with Searcher("foo") as s:
        assert s.search("Teflon", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "polytetrafluoroethylene",
                "database": "foo",
                "location": "",
                "code": "bar",
                "categories": "",
                "synonyms": "ptfe, teflon",
            }
        ]


@bw2test
def test_search_single_char():
    """Check we can disambiguate between "system 1", "system 2" and "system 3" """
    for i in [1, 2, 3]:
        add(
            {
                "database": "foo",
                "code": "bar%s" % i,
                "name": "Milk organic system %s" % i,
            }
        )
    with Searcher("foo") as s:
        assert s.search("milk organic system 2", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "milk organic system 2",
                "database": "foo",
                "location": "",
                "code": "bar2",
                "categories": "",
                "synonyms": "",
            }
        ]


@bw2test
def test_search_with_parentheses():
    """Test that searching with parentheses works correctly"""
    add({"database": "foo", "code": "bar", "name": "beam dried (u=10%) planed"})
    with Searcher("foo") as s:
        assert s.search("dried (u=10%)", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "beam dried (u=10%) planed",
                "database": "foo",
                "location": "",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ]


@bw2test
def test_search_with_special_chars():
    """Test that searching with ' works correctly"""
    add(
        {
            "database": "foo",
            "code": "bar",
            "name": "Comte cheese, from cow's milk, consumption mix: {FR} U & (test)",
        }
    )
    with Searcher("foo") as s:
        assert s.search("Comte cheese, from cow's milk test {FR}", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "comte cheese, from cow's milk, consumption mix: {fr} u & (test)",
                "database": "foo",
                "location": "",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ]


@bw2test
def test_filter_by_location():
    add(
        *[
            {"database": "foo", "code": "bar", "name": "electricity", "location": "MX"},
            {"database": "foo", "code": "baz", "name": "electricity", "location": "DE"},
        ]
    )
    with Searcher("foo") as s:
        results = s.search("electricity", proxy=False, filter={"location": "MX"})
    assert len(results) == 1
    assert results[0]["location"] == "mx"
    assert results[0]["code"] == "bar"


@bw2test
def test_filter_by_location_case_insensitive():
    add(
        *[
            {"database": "foo", "code": "bar", "name": "electricity", "location": "MX"},
            {"database": "foo", "code": "baz", "name": "electricity", "location": "DE"},
        ]
    )
    with Searcher("foo") as s:
        results = s.search("electricity", proxy=False, filter={"location": "mx"})
    assert len(results) == 1
    assert results[0]["code"] == "bar"


@bw2test
def test_filter_by_categories():
    add(
        *[
            {
                "database": "foo",
                "code": "bar",
                "name": "electricity",
                "categories": ("air",),
            },
            {
                "database": "foo",
                "code": "baz",
                "name": "electricity",
                "categories": ("water",),
            },
        ]
    )
    with Searcher("foo") as s:
        results = s.search("electricity", proxy=False, filter={"categories": "air"})
    assert len(results) == 1
    assert results[0]["code"] == "bar"


@bw2test
def test_filter_no_match_returns_empty():
    add({"database": "foo", "code": "bar", "name": "electricity", "location": "DE"})
    with Searcher("foo") as s:
        results = s.search("electricity", proxy=False, filter={"location": "MX"})
    assert results == []


@bw2test
def test_mask_excludes_results():
    add(
        *[
            {"database": "foo", "code": "bar", "name": "electricity", "location": "MX"},
            {"database": "foo", "code": "baz", "name": "electricity", "location": "DE"},
        ]
    )
    with Searcher("foo") as s:
        results = s.search("electricity", proxy=False, mask={"location": "MX"})
    assert len(results) == 1
    assert results[0]["code"] == "baz"


@bw2test
def test_filter_respects_limit():
    add(
        *[
            {"database": "foo", "code": str(i), "name": "electricity", "location": "MX"}
            for i in range(10)
        ]
    )
    with Searcher("foo") as s:
        results = s.search("electricity", proxy=False, filter={"location": "MX"}, limit=5)
    assert len(results) == 5


def test_escape_search():
    assert (
        IndexManager.escape_search_for_fts5("Comte cheese from cow's")
        == '"Comte" "cheese" "from" "cow\'s"'
    )

    assert (
        IndexManager.escape_search_for_fts5("Comte* cheese from cow's")
        == '"Comte"* "cheese" "from" "cow\'s"'
    )

    assert (
        IndexManager.escape_search_for_fts5('Comte* "cheese" from cow\'s')
        == '"Comte"* "cheese" "from" "cow\'s"'
    )

    assert '"Comte"*' == IndexManager.escape_search_for_fts5("Comte*")


@bw2test
def test_search_with_substrings():
    """Test that searching with ' works correctly"""
    add(
        *[
            {
                "database": "foo",
                "code": "bar",
                "name": "Comte cheese, from cow's milk, consumption mix: {FR} U & (test)",
            },
            {
                "database": "foo",
                "code": "baz",
                "name": 'meat without bone chicken for direct consumption "fr-organic"',
            },
        ]
    )
    with Searcher("foo") as s:
        assert s.search("Comte cheese from cow's", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "comte cheese, from cow's milk, consumption mix: {fr} u & (test)",
                "database": "foo",
                "location": "",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ]

        assert s.search("Comte cheese cow's", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "comte cheese, from cow's milk, consumption mix: {fr} u & (test)",
                "database": "foo",
                "location": "",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ]

        assert s.search("Com* cheese cow's", proxy=False) == [
            {
                "comment": "",
                "product": "",
                "name": "comte cheese, from cow's milk, consumption mix: {fr} u & (test)",
                "database": "foo",
                "location": "",
                "code": "bar",
                "categories": "",
                "synonyms": "",
            }
        ]

        assert s.search("Com cheese cow's", proxy=False) == []

        assert s.search(
            'meat without bone chicken for direct consumption "fr-organic"',
            proxy=False,
        ) == [
            {
                "comment": "",
                "product": "",
                "name": 'meat without bone chicken for direct consumption "fr-organic"',
                "database": "foo",
                "location": "",
                "code": "baz",
                "categories": "",
                "synonyms": "",
            }
        ]


# The index is in the project's ``lci/databases.db`` (storage S7)


def tables():
    return {row[0] for row in sqlite3_lci_db.execute_sql("SELECT name FROM sqlite_master")}


def indexed(database="foo"):
    """Number of rows in the index of ``database``."""
    table = index_table(database)
    return sqlite3_lci_db.execute_sql(f'SELECT count(*) FROM "{table}"').fetchone()[0]


@bw2test
def test_index_is_a_contentless_table_in_the_lci_file():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    table = index_table("foo")
    assert table in tables()
    assert not (projects.dir / "search").exists() or not list((projects.dir / "search").iterdir())
    node = get_node(database="foo", code="bar")
    # The row is the node's, and the text is not stored a second time
    assert sqlite3_lci_db.execute_sql(f'SELECT rowid, name FROM "{table}"').fetchall() == [
        (node.id, None)
    ]
    assert f"{table}_content" not in tables()


@bw2test
def test_results_have_node_ids():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    node = get_node(database="foo", code="bar")
    with _Searcher("foo") as s:
        assert [result["id"] for result in s.search("lollipop", proxy=False)] == [node.id]
        assert s.search("lollipop") == [node]


@bw2test
def test_database_filename_is_accepted():
    db = SQLiteBackend("Foo / bar")
    db.write({("Foo / bar", "x"): {"name": "lollipop"}})
    assert db.filename != db.name
    assert IndexManager(db.filename).database == "Foo / bar"
    with Searcher(db.filename) as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_nodes_which_are_not_stored_cant_be_indexed():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    with pytest.raises(UnknownObject, match="is not stored"):
        IndexManager("foo").add_dataset({"database": "foo", "code": "other", "name": "candy"})


@bw2test
def test_search_by_code():
    add({"database": "foo", "code": "abc123", "name": "lollipop"})
    assert [node["code"] for node in SQLiteBackend("foo").search("abc123")] == ["abc123"]


@bw2test
def test_saving_twice_indexes_once():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    node = get_node(database="foo", code="bar")
    for _ in range(3):
        node.save()
    assert indexed() == 1
    assert len(SQLiteBackend("foo").search("lollipop")) == 1


@bw2test
def test_writing_twice_indexes_once():
    db = SQLiteBackend("foo")
    data = {("foo", str(i)): {"name": f"lollipop {i}"} for i in range(5)}
    db.write(data)
    db.write(data)
    db.make_searchable(reset=True)
    assert indexed() == 5
    assert len(db.search("lollipop")) == 5


@bw2test
def test_failed_save_leaves_the_index_alone(monkeypatch):
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    node = get_node(database="foo", code="bar")
    node["name"] = "candy"

    def fail(self, *args, **kwargs):
        raise RuntimeError("no")

    monkeypatch.setattr(ActivityDataset, "save", fail)
    with pytest.raises(RuntimeError):
        node.save()
    monkeypatch.undo()
    db = SQLiteBackend("foo")
    assert db.search("lollipop")
    assert not db.search("candy")
    assert indexed() == 1


@bw2test
def test_code_change_updates_the_index():
    add({"database": "foo", "code": "abc123", "name": "lollipop"})
    node = get_node(database="foo", code="abc123")
    node["code"] = "xyz789"
    db = SQLiteBackend("foo")
    assert not db.search("abc123")
    assert [n["code"] for n in db.search("xyz789")] == ["xyz789"]
    assert indexed() == 1


@bw2test
def test_database_change_moves_the_node_to_the_other_index():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    other = SQLiteBackend("other")
    other.write({("other", "x"): {"name": "candy"}})
    node = get_node(database="foo", code="bar")
    node["database"] = "other"
    assert not SQLiteBackend("foo").search("lollipop")
    assert [n.key for n in other.search("lollipop")] == [("other", "bar")]
    assert (indexed("foo"), indexed("other")) == (0, 2)


@bw2test
def test_deleting_a_database_drops_its_index():
    db = SQLiteBackend("foo")
    db.write({("foo", "bar"): {"name": "lollipop"}})
    assert index_table("foo") in tables()
    del databases["foo"]
    assert not [name for name in tables() if name.startswith("searchindex_")]


@bw2test
def test_databases_have_their_own_index():
    SQLiteBackend("foo").write({("foo", "a"): {"name": "lollipop"}})
    SQLiteBackend("other").write(
        {("other", "a"): {"name": "lollipop"}, ("other", "b"): {"name": "x"}}
    )
    assert [n.key for n in SQLiteBackend("foo").search("lollipop")] == [("foo", "a")]
    assert [n.key for n in SQLiteBackend("other").search("lollipop")] == [("other", "a")]
    assert len(SQLiteBackend("other").search("*")) == 2


@bw2test
def test_rows_of_nodes_changed_without_the_index_are_not_returned():
    db = SQLiteBackend("foo")
    db.write({("foo", "a"): {"name": "lollipop"}, ("foo", "b"): {"name": "lollipop"}})
    ActivityDataset.delete().where(ActivityDataset.code == "a").execute()
    assert [n.key for n in db.search("lollipop")] == [("foo", "b")]
    # Building the index again removes the row
    db.make_searchable(reset=True)
    assert indexed() == 1


@bw2test
def test_unsearchable_database_is_not_indexed():
    db = SQLiteBackend("foo")
    db.write({("foo", "a"): {"name": "lollipop"}})
    db.make_unsearchable()
    assert index_table("foo") not in tables()
    assert db.search("lollipop") == []
    node = get_node(database="foo", code="a")
    node["name"] = "candy"
    node.save()
    node.delete()
    assert index_table("foo") not in tables()


@bw2test
def test_merge_databases_indexes_the_moved_nodes():
    from bw2data.utils import merge_databases

    SQLiteBackend("foo").write({("foo", "a"): {"name": "lollipop"}})
    SQLiteBackend("other").write({("other", "b"): {"name": "candy"}})
    merge_databases("foo", "other")
    assert [n.key for n in SQLiteBackend("foo").search("candy")] == [("foo", "b")]
    assert index_table("other") not in tables()


# The index only removes rows it holds, with the text which was added (review of #348)


def integrity_check(database="foo"):
    """Raises ``sqlite3.DatabaseError`` if the index of ``database`` is corrupt."""
    table = index_table(database)
    sqlite3_lci_db.execute_sql(f'INSERT INTO "{table}" ("{table}") VALUES (\'integrity-check\')')


@bw2test
def test_removing_nodes_which_are_not_indexed():
    db = SQLiteBackend("foo")
    db.write({("foo", "a"): {"name": "lollipop"}, ("foo", "b"): {"name": "candy"}})
    b = get_node(database="foo", code="b")
    index = IndexManager("foo")
    index.remove([b.id])
    index.remove([b.id])
    b.delete()
    integrity_check()
    assert indexed() == 1
    assert [n["code"] for n in db.search("lollipop")] == ["a"]


@bw2test
def test_adding_nodes_which_are_indexed():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    node = get_node(database="foo", code="bar")
    IndexManager("foo").add([node.id])
    IndexManager("foo").add_datasets([node])
    assert indexed() == 1
    node["name"] = "candy"
    node.save()
    integrity_check()
    assert indexed() == 1
    assert not SQLiteBackend("foo").search("lollipop")


@bw2test
def test_nodes_of_another_database_are_not_indexed():
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    SQLiteBackend("other").write({("other", "x"): {"name": "candy"}})
    node = get_node(database="foo", code="bar")
    IndexManager("other").add([node.id])
    IndexManager("other").remove([node.id])
    assert indexed("other") == 1
    integrity_check("other")


@bw2test
def test_saving_into_a_database_written_as_unsearchable():
    db = SQLiteBackend("foo")
    db.write({("foo", str(i)): {"name": f"lollipop {i}"} for i in range(4)}, searchable=False)
    assert databases["foo"]["searchable"] is False
    get_node(database="foo", code="0").save()
    for code in "123":
        get_node(database="foo", code=code).delete()
    assert index_table("foo") not in tables()
    assert len(db) == 1


@bw2test
def test_unsearchable_rewrite_of_a_searchable_database():
    db = SQLiteBackend("foo")
    data = {("foo", str(i)): {"name": f"lollipop {i}"} for i in range(3)}
    db.write(data)
    db.write(data, searchable=False)
    assert databases["foo"]["searchable"] is False
    assert index_table("foo") not in tables()
    node = get_node(database="foo", code="1")
    node.save()
    node.delete()
    db.make_searchable()
    assert len(db.search("lollipop")) == 2


@bw2test
def test_registered_database_indexes_saved_nodes():
    db = SQLiteBackend("foo")
    db.register()
    assert databases["foo"].get("searchable") is None
    db.new_node(code="a", name="lollipop", type="process").save()
    assert [n["code"] for n in db.search("lollipop")] == ["a"]


@bw2test
def test_copying_a_registered_database_keeps_it_searchable():
    db = SQLiteBackend("foo")
    db.register()
    db.new_node(code="a", name="lollipop", type="process").save()
    copy = db.copy("bar")
    assert [n["code"] for n in copy.search("lollipop")] == ["a"]


@bw2test
def test_save_with_a_new_database_in_the_document():
    """As revisions do it: the document has the new database before ``save``."""
    add({"database": "foo", "code": "bar", "name": "lollipop"})
    other = SQLiteBackend("other")
    other.write({("other", "x"): {"name": "candy"}})
    node = get_node(database="foo", code="bar")
    node._document.database = "other"
    node._data["database"] = "other"
    node.save(data_already_set=True)
    integrity_check("foo")
    integrity_check("other")
    assert (indexed("foo"), indexed("other")) == (0, 2)
    assert [n.key for n in other.search("lollipop")] == [("other", "bar")]
    get_node(database="other", code="bar").delete()
    integrity_check("other")
    assert indexed("other") == 1


@bw2test
def test_update_dataset_indexes_the_stored_values():
    add({"database": "foo", "code": "bar", "name": "lollipop", "comment": "sweet"})
    IndexManager("foo").update_dataset({"database": "foo", "code": "bar", "name": "toffee"})
    db = SQLiteBackend("foo")
    assert not db.search("toffee")
    assert db.search("sweet")
    node = get_node(database="foo", code="bar")
    node["comment"] = "sour"
    node.save()
    integrity_check()
    assert not db.search("sweet")
    assert db.search("sour")
