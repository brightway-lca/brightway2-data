"""Calculation setups in SQLite (#314): ``calculation_setups`` and
``dynamic_calculation_setups``, and schema migration 8, which moves ``setups.pickle`` and
``dynamicsetups.pickle`` into ``lci/databases.db``."""

import pickle
import warnings

import numpy as np
import pytest

from bw2data import Database, calculation_setups, dynamic_calculation_setups, get_node, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import migrate, schema_version
from bw2data.backends.schema import CalculationSetupDemand, CalculationSetupRecord
from bw2data.errors import UnknownObject
from bw2data.meta import MAX_SETUP_NODES, CalculationSetupProxy
from bw2data.tests import bw2test

from .test_extension_attach import files  # noqa: F401


def write_nodes(name="db", codes=("a", "b", "c")):
    Database(name).write({(name, code): {"name": code} for code in codes})
    return [get_node(database=name, code=code) for code in codes]


# Reading and writing


@bw2test
def test_round_trip_as_written():
    a, b, c = write_nodes()
    setup = {
        "inv": [{a.key: 1, b.id: 2.5}, {}, {c.key: 3}],
        "ia": [("m", "1"), ("m", "2")],
        "comment": {"made by": ("someone", 1)},
    }
    calculation_setups["s"] = setup
    assert calculation_setups["s"] == setup
    assert isinstance(calculation_setups["s"], CalculationSetupProxy)
    # Keys come back as tuples, in the order they were written
    assert list(calculation_setups["s"]["inv"][0]) == [a.key, b.id]
    assert type(calculation_setups["s"]["inv"][0][b.id]) is float
    assert calculation_setups.data == {"s": setup}


@bw2test
def test_keys_which_were_not_given_are_not_added():
    calculation_setups["only ia"] = {"ia": [("m",)]}
    assert dict(calculation_setups["only ia"]) == {"ia": [("m",)]}
    calculation_setups["empty"] = {}
    assert dict(calculation_setups["empty"]) == {}
    calculation_setups["no units"] = {"inv": []}
    assert dict(calculation_setups["no units"]) == {"inv": []}


@bw2test
def test_registry():
    a, b, c = write_nodes()
    calculation_setups["b"] = {"inv": [{a.key: 1}]}
    calculation_setups["a"] = {"inv": [{b.key: 1}]}
    assert list(calculation_setups) == ["a", "b"] == calculation_setups.list
    assert len(calculation_setups) == 2
    assert (
        "a" in calculation_setups and "x" not in calculation_setups and 1 not in calculation_setups
    )
    assert calculation_setups.random() in ("a", "b")
    del calculation_setups["a"]
    assert list(calculation_setups) == ["b"]
    with pytest.raises(KeyError):
        del calculation_setups["a"]
    with pytest.raises(KeyError):
        calculation_setups["a"]
    # Replacing keeps one row
    calculation_setups["b"] = {"inv": [{c.id: 2}]}
    assert calculation_setups["b"]["inv"] == [{c.id: 2}]
    assert CalculationSetupRecord.select().count() == 1
    assert CalculationSetupDemand.select().count() == 1
    assert "CalculationSetups with 1 setup(s): ['b']" == repr(calculation_setups)


@bw2test
def test_static_and_dynamic_are_separate():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: 1}], "ia": [("m",)]}
    dynamic_calculation_setups["s"] = {"inv": [{b.key: 1}], "ia": [{("worst",): ("dynamic",)}]}
    assert calculation_setups["s"]["inv"] == [{a.key: 1}]
    assert dynamic_calculation_setups["s"]["ia"] == [{("worst",): ("dynamic",)}]
    del calculation_setups["s"]
    assert "s" in dynamic_calculation_setups


@bw2test
def test_proxies_keep_changes_until_saved():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: 1}], "ia": []}
    setup = calculation_setups["s"]
    setup["inv"].append({b.id: 2})
    setup["ia"] = [("m",)]
    # The same proxy, with its pending changes
    assert calculation_setups["s"] is setup
    assert calculation_setups.data["s"] == {"inv": [{a.key: 1}], "ia": []}
    setup.save()
    assert calculation_setups.data["s"] == {"inv": [{a.key: 1}, {b.id: 2}], "ia": [("m",)]}
    # ``flush`` still saves pending changes, with a warning
    calculation_setups["s"]["ia"].append(("n",))
    with pytest.warns(DeprecationWarning, match="calculation_setups\\[name\\].save\\(\\)"):
        calculation_setups.flush()
    assert calculation_setups.data["s"]["ia"] == [("m",), ("n",)]
    # Assigning a proxy stores its content
    calculation_setups["copy"] = calculation_setups["s"]
    assert calculation_setups["copy"] == calculation_setups["s"]


@bw2test
def test_refused_setups_change_nothing():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: 1}]}
    with pytest.raises(UnknownObject, match="nodes which don't exist: \\('db', 'nope'\\)"):
        calculation_setups["s"] = {"inv": [{("db", "nope"): 1}]}
    with pytest.raises(ValueError, match="node .* twice"):
        calculation_setups["s"] = {"inv": [{a.key: 1, a.id: 2}]}
    with pytest.raises(TypeError, match="list of dicts"):
        calculation_setups["s"] = {"inv": {a.key: 1}}
    with pytest.raises(TypeError, match="by key or id"):
        calculation_setups["s"] = {"inv": [{"a": 1}]}
    with pytest.raises(ValueError, match="Not a node key"):
        calculation_setups["s"] = {"inv": [{("db", "a", "x"): 1}]}
    for amount in (None, float("nan"), np.float64("nan")):
        with pytest.raises(ValueError, match="has no amount for node"):
            calculation_setups["s"] = {"inv": [{a.key: amount}]}
    with pytest.raises(TypeError, match="must be strings"):
        calculation_setups[("s",)] = {}
    with pytest.raises(TypeError, match="must be a dict"):
        calculation_setups["s"] = [1]
    with pytest.raises(ValueError, match="has 5001 nodes .* at most 5000 can be stored"):
        calculation_setups["s"] = {"inv": [{a.key: 1}] * (MAX_SETUP_NODES + 1)}
    assert calculation_setups["s"]["inv"] == [{a.key: 1}]


@bw2test
def test_largest_setup():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: 1}] * MAX_SETUP_NODES}
    assert len(calculation_setups["s"]["inv"]) == MAX_SETUP_NODES


@bw2test
def test_numpy_amounts():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: np.float64(1.5), b.id: np.int64(2)}]}
    assert calculation_setups["s"]["inv"] == [{a.key: 1.5, b.id: 2}]
    assert type(calculation_setups["s"]["inv"][0][b.id]) is int


@bw2test
def test_numpy_node_ids():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{np.int64(a.id): 1, b.key: 2}]}
    assert calculation_setups["s"]["inv"] == [{a.id: 1, b.key: 2}]
    assert type(next(iter(calculation_setups["s"]["inv"][0]))) is int
    with pytest.raises(ValueError, match="node .* twice"):
        calculation_setups["s"] = {"inv": [{np.int64(a.id): 1, a.key: 2}]}
    a.delete()
    assert calculation_setups["s"]["inv"] == [{b.key: 2}]


@bw2test
def test_setups_belong_to_their_project():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: 1}]}
    name = projects.current
    projects.set_current("other")
    assert "s" not in calculation_setups
    assert len(calculation_setups) == 0
    projects.set_current(name)
    assert calculation_setups["s"]["inv"] == [{a.key: 1}]


# Changes to the nodes (#191)


@bw2test
def test_keys_follow_renamed_nodes_and_databases():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: 1, b.id: 2}]}
    a["code"] = "renamed"
    a.save()
    assert calculation_setups["s"]["inv"] == [{("db", "renamed"): 1, b.id: 2}]
    # ``rename`` writes the nodes again, with new ids
    Database("db").rename("other db")
    b = get_node(database="other db", code="b")
    assert calculation_setups["s"]["inv"] == [{("other db", "renamed"): 1, b.id: 2}]


@bw2test
def test_deleting_a_node_removes_it_everywhere(caplog):
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: 1}, {b.key: 2, a.id: 3}, {c.id: 4}]}
    dynamic_calculation_setups["d"] = {"inv": [{c.key: 1}, {a.key: 1}]}
    calculation_setups["untouched"] = {"inv": [{b.key: 1}]}
    get_node(id=a.id).delete()
    # Functional units which are then empty go
    assert calculation_setups["s"]["inv"] == [{b.key: 2}, {c.id: 4}]
    assert dynamic_calculation_setups["d"]["inv"] == [{c.key: 1}]
    assert calculation_setups["untouched"]["inv"] == [{b.key: 1}]


@bw2test
def test_deleting_a_database_removes_its_nodes():
    a, b, c = write_nodes()
    (x,) = write_nodes("other", ("x",))
    calculation_setups["s"] = {"inv": [{a.key: 1, x.key: 1}, {b.id: 2}]}
    Database("db").delete(warn=False)
    assert calculation_setups["s"]["inv"] == [{x.key: 1}]


@bw2test
def test_writing_a_database_again_removes_its_old_nodes():
    a, b, c = write_nodes()
    calculation_setups["s"] = {"inv": [{a.key: 1}], "ia": [("m",)]}
    # New nodes, with new ids
    write_nodes()
    assert calculation_setups["s"] == {"inv": [], "ia": [("m",)]}


# Migration 8


def legacy(path, data):
    with open(path, "wb") as f:
        pickle.dump(data, f, protocol=4)


@bw2test
def test_migration_moves_both_files():
    a, b, c = write_nodes()
    legacy(
        projects.dir / "setups.pickle",
        {
            "s": {"inv": [{a.key: 1, b.id: np.float64(2.0)}, {c.key: 3}], "ia": [("m", "1")]},
            "numpy id": {"inv": [{np.int64(a.id): 1}]},
            # Amounts SQLite can't store: skipped, and the other setups are still moved
            "nan": {"inv": [{a.key: float("nan")}]},
            "none": {"inv": [{a.key: 1}, {b.id: None}]},
            "gone": {"inv": [{("db", "deleted"): 1}, {a.key: 2}], "ia": []},
            "odd": [1, 2],
            # A name which can't be stored
            ("a", "tuple"): {"inv": [{a.key: 1}]},
        },
    )
    legacy(
        projects.dir / "dynamicsetups.pickle",
        {"d": {"inv": [{a.key: 1}], "ia": [{("worst",): ("dynamic",)}], "colour": "red"}},
    )
    calculation_setups["kept"] = {"inv": [{a.key: 5}]}
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 7")
    with pytest.warns(UserWarning) as caught:
        assert migrate(sqlite3_lci_db.db, projects.dir) == [8, 9, 10]
    messages = " ".join(str(w.message) for w in caught)
    assert "can't be stored are only kept in the file: ['nan', 'none', 'odd', ('a', 'tuple')]" in messages
    assert "'gone': [('db', 'deleted')]" in messages
    assert calculation_setups["s"] == {
        "inv": [{a.key: 1, b.id: 2.0}, {c.key: 3}],
        "ia": [("m", "1")],
    }
    # Keys of nodes which don't exist are left out; the functional unit stays
    assert calculation_setups["gone"]["inv"] == [{}, {a.key: 2}]
    assert dynamic_calculation_setups["d"] == {
        "inv": [{a.key: 1}],
        "ia": [{("worst",): ("dynamic",)}],
        "colour": "red",
    }
    assert calculation_setups["kept"]["inv"] == [{a.key: 5}]
    assert calculation_setups["numpy id"]["inv"] == [{a.id: 1}]
    assert not {"odd", "nan", "none"} & set(calculation_setups)
    # The files are kept as backups
    assert (projects.dir / "setups.pickle").is_file()
    assert (projects.dir / "dynamicsetups.pickle").is_file()


@bw2test
def test_migration_without_files_and_with_unreadable_files():
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 7")
    assert migrate(sqlite3_lci_db.db, projects.dir) == [8, 9, 10]
    (projects.dir / "setups.pickle").write_bytes(b"not a pickle")
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 7")
    with pytest.warns(UserWarning, match="Can't read .*setups.pickle"):
        assert migrate(sqlite3_lci_db.db, projects.dir) == [8, 9, 10]
    assert schema_version(sqlite3_lci_db.db) == 10
    assert len(calculation_setups) == 0


# Extensions


def test_nodes_in_extensions(files):  # noqa: F811
    bio, inv = files
    projects.attach_extension(bio)
    flow = get_node(database="bio", code="1")
    calculation_setups["s"] = {"inv": [{("bio", "0"): 1, flow.id: 2}]}
    # Detached: the nodes are gone, but the setup keeps them, as the key it was written with
    projects.detach_extension("bio")
    assert calculation_setups["s"]["inv"] == [{("bio", "0"): 1, flow.id: 2}]
    projects.attach_extension(bio)
    assert calculation_setups["s"]["inv"] == [{("bio", "0"): 1, flow.id: 2}]
