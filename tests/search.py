from bw2data.tests import bw2test
from bw2data import databases
from bw2data.search import Searcher, IndexManager
from bw2data.backends import SQLiteBackend


@bw2test
def test_search_dataset_containing_stop_word():
    im = IndexManager("foo")
    im.add_dataset(
        {"database": "foo", "code": "bar", "name": "foo of bar, high voltage"}
    )
    with Searcher("foo") as s:
        assert s.search("foo of bar, high voltage", proxy=False)


@bw2test
def test_add_dataset():
    im = IndexManager("foo")
    im.add_dataset({"database": "foo", "code": "bar", "name": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_search_dataset():
    im = IndexManager("foo")
    im.add_dataset({"database": "foo", "code": "bar", "name": "lollipop"})
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
    im = IndexManager("foo")
    im.add_dataset(
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
    im = IndexManager("foo")
    ds = {"database": "foo", "code": "bar", "name": "lollipop"}
    im.add_dataset(ds)
    ds["name"] = "lemon cake"
    im.update_dataset(ds)
    with Searcher("foo") as s:
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
    im = IndexManager("foo")
    ds = {"database": "foo", "code": "bar", "name": "lollipop"}
    im.add_dataset(ds)
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)
    im.delete_dataset(ds)
    with Searcher("foo") as s:
        assert not s.search("lollipop", proxy=False)


@bw2test
def test_add_datasets():
    im = IndexManager("foo")
    ds = [{"database": "foo", "code": "bar", "name": "lollipop"}]
    im.add_datasets(ds)
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
def test_delete_database():
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
def test_reset_index():
    im = IndexManager("foo")
    ds = {"database": "foo", "code": "bar", "name": "lollipop"}
    im.add_dataset(ds)
    im.create()
    with Searcher("foo") as s:
        assert not s.search("lollipop", proxy=False)


@bw2test
def test_basic_search():
    im = IndexManager("foo")
    im.add_dataset({"database": "foo", "code": "bar", "name": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_product_term():
    im = IndexManager("foo")
    im.add_dataset({"database": "foo", "code": "bar", "reference product": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_comment_term():
    im = IndexManager("foo")
    im.add_dataset({"database": "foo", "code": "bar", "comment": "lollipop"})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_categories_term():
    im = IndexManager("foo")
    im.add_dataset({"database": "foo", "code": "bar", "categories": ("lollipop",)})
    with Searcher("foo") as s:
        assert s.search("lollipop", proxy=False)


@bw2test
def test_limit():
    im = IndexManager("foo")
    im.add_datasets(
        [
            {"database": "foo", "code": "bar", "name": "lollipop {}".format(x)}
            for x in range(50)
        ]
    )
    with Searcher("foo") as s:
        assert len(s.search("lollipop", limit=25, proxy=False)) == 25


@bw2test
def test_search_faceting():
    im = IndexManager("foo")
    ds = [
        {"database": "foo", "code": "bar", "name": "lollipop", "location": "CH"},
        {"database": "foo", "code": "bar", "name": "ice lollipop", "location": "FR"},
    ]
    im.add_datasets(ds)
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
                "code": "bar",
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


@bw2test
def test_case_sensitivity_filter():
    db = SQLiteBackend("foo")
    ds = {
        ("foo", "bar"): {
            "database": "foo",
            "code": "bar",
            "name": "lollipop",
            "location": "CH",
        },
        ("foo", "baz"): {
            "database": "foo",
            "code": "baz",
            "name": "ice lollipop",
            "location": "FR",
        },
    }
    db.write(ds)
    assert len(db.search("lollipop")) == 2
    assert len(db.search("lollipop", filter={"location": "fr"})) == 1
    assert len(db.search("lollipop", filter={"location": "FR"})) == 1


@bw2test
def test_case_sensitivity_mask():
    db = SQLiteBackend("foo")
    ds = {
        ("foo", "bar"): {
            "database": "foo",
            "code": "bar",
            "name": "lollipop",
            "location": "CH",
        },
        ("foo", "baz"): {
            "database": "foo",
            "code": "baz",
            "name": "ice lollipop",
            "location": "FR",
            "reference product": "ZEBRA",
        },
    }
    db.write(ds)
    assert len(db.search("lollipop")) == 2
    assert len(db.search("lollipop", mask={"product": "ZEbra"})) == 1


@bw2test
def test_synonym_search():
    im = IndexManager("foo")
    im.add_dataset({"database": "foo", "code": "bar", "name": "polytetrafluoroethylene", "synonyms":["PTFE", "Teflon"]})
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

if __name__ == "__main__":
    test_synonym_search()