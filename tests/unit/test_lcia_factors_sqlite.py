"""Characterization factors, normalization factors and weightings stored in SQLite, and
schema migration 5 which moves them from ``intermediate/*.pickle``."""

import json
import pickle
import sqlite3

import numpy as np
import pytest

from bw2data import (
    Database,
    Method,
    Normalization,
    Weighting,
    geomapping,
    get_node,
    methods,
    normalizations,
    projects,
    weightings,
)
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.migrations import SCHEMA_VERSION, migrate, schema_version
from bw2data.backends.schema import (
    CharacterizationFactor,
    MethodRecord,
    NormalizationFactor,
    WeightingFactor,
)
from bw2data.backends.utils import replace_cfs
from bw2data.errors import MissingIntermediateData, UnknownObject
from bw2data.tests import bw2test

NAME = ("a", "method")


@pytest.fixture
@bw2test
def bio():
    Database("bio").write(
        {
            ("bio", "co2"): {"name": "CO2", "type": "emission"},
            ("bio", "ch4"): {"name": "CH4", "type": "emission"},
        }
    )
    return get_node(code="co2").id, get_node(code="ch4").id


def _stored(table):
    return list(sqlite3_lci_db.execute_sql(f'SELECT * FROM "{table}" ORDER BY 1, 2'))


# ----------------------------------------------------------------------
# Methods
# ----------------------------------------------------------------------


def test_method_round_trip(bio):
    co2, ch4 = bio
    uncertainty = {"amount": 2.0, "uncertainty type": 2, "loc": 0.5, "scale": 0.1}
    method = Method(NAME)
    method.write(
        [
            (("bio", "co2"), 1.5),
            (get_node(code="ch4"), uncertainty, "CH"),
            (co2, {"amount": 3}, ("world", "DE")),
            (ch4, 4, ["world", "FR"]),
            (co2, 5, None),
            (co2, 6, ""),
        ]
    )
    assert method.load() == [
        (co2, 1.5),
        (ch4, uncertainty, "CH"),
        (co2, {"amount": 3.0}, ("world", "DE")),
        (ch4, 4.0, ("world", "FR")),
        (co2, 5.0),
        (co2, 6.0),
    ]
    assert method.metadata["num_cfs"] == 6
    assert method.metadata["geocollections"] == ["world"]
    assert method.datapackage().get_resource("a_method_matrix_data.data")[0].shape == (6,)


def test_method_rows_store_names_not_geomapping_ids(bio):
    co2, _ = bio
    Method(NAME).write([(co2, 1, ("world", "CH")), (co2, 2, "RER"), (co2, 3)])
    assert [
        (row.position, row.flow, row.amount, row.uncertainty, row.geocollection, row.location)
        for row in CharacterizationFactor.select().order_by(CharacterizationFactor.position)
    ] == [
        (0, co2, 1.0, None, "world", "CH"),
        (1, co2, 2.0, None, None, "RER"),
        (2, co2, 3.0, None, None, None),
    ]


def test_numpy_values_are_stored_as_python_numbers(bio):
    co2, _ = bio
    Method(NAME).write(
        [(co2, np.float32(1.5)), (co2, {"amount": np.float64(2), "uncertainty type": np.int64(3)})]
    )
    assert Method(NAME).load() == [(co2, 1.5), (co2, {"amount": 2.0, "uncertainty type": 3})]
    assert CharacterizationFactor.get(CharacterizationFactor.position == 1).uncertainty == {
        "uncertainty type": 3
    }


def test_nothing_is_written_to_intermediate(bio):
    co2, _ = bio
    Method(NAME).write([(co2, 1)])
    assert not list((projects.dir / "intermediate").iterdir())


@pytest.mark.parametrize(
    "row",
    [
        ("co2", 1, 5),
        ("co2", 1, {"a": "b"}),
        ("co2", 1, ("world", 1)),
        ("co2", 1, ("world",)),
        ("co2", "one"),
        ("co2", True),
        ("co2", {"amount": "one"}),
        ("co2", float("nan")),
        ("co2", np.float64("nan")),
        ("co2", {"amount": float("nan"), "uncertainty type": 0}),
        ("co2", 1j),
        ("co2", 1, "CH", "extra"),
        ("co2",),
    ],
)
def test_invalid_rows_raise_and_change_nothing(bio, row):
    co2, _ = bio
    method = Method(NAME)
    method.write([(co2, 1)])
    bad = (co2, *row[1:])
    with pytest.raises(ValueError):
        method.write([(co2, 2), bad])
    assert method.load() == [(co2, 1.0)]
    assert methods[NAME]["num_cfs"] == 1


@pytest.mark.parametrize("row", [5, None, "ab", (), {"flow": 1}])
def test_rows_which_are_not_sequences_raise_value_error(bio, row):
    co2, _ = bio
    method = Method(NAME)
    method.write([(co2, 1)])
    with pytest.raises(ValueError, match="A CF row must be"):
        method.write([(co2, 2), row])
    assert method.load() == [(co2, 1.0)]


def test_nan_amounts_are_rejected_for_all_kinds(bio):
    co2, _ = bio
    nan = float("nan")
    with pytest.raises(ValueError, match="NaN"):
        Normalization(("n",)).write([(co2, nan)])
    with pytest.raises(ValueError, match="NaN"):
        Weighting(("w",)).write([{"amount": nan}])
    with pytest.raises(ValueError, match="NaN"):
        Weighting(("w",)).write([nan])
    # Infinite amounts, and NaN in other fields, can be stored
    Method(NAME).write([(co2, float("inf")), (co2, {"amount": 1, "minimum": nan})])
    first, second = Method(NAME).load()
    assert first == (co2, float("inf"))
    assert second[1]["amount"] == 1.0 and np.isnan(second[1]["minimum"])


def test_numpy_integer_flow_ids(bio):
    co2, ch4 = bio
    Method(NAME).write([(np.int64(co2), 1)])
    Normalization(("n",)).write([(np.int64(ch4), 2)])
    assert Method(NAME).load() == [(co2, 1.0)]
    assert Normalization(("n",)).load() == [(ch4, 2.0)]
    assert type(CharacterizationFactor.get().flow) is int


def test_unknown_flow_raises(bio):
    with pytest.raises(UnknownObject):
        Method(NAME).write([(("bio", "missing"), 1)])
    with pytest.raises(ValueError):
        Method(NAME).write([("not a key", 1)])


def test_failed_write_forgets_its_metadata_changes(bio, monkeypatch):
    co2, _ = bio
    method = Method(NAME)
    method.write([(co2, 1)])

    def broken(model, rows):
        raise RuntimeError("disk full")

    monkeypatch.setattr("bw2data.backends.schema._insert_many", broken)
    with pytest.raises(RuntimeError):
        method.write([(co2, 1), (co2, 2)])
    monkeypatch.undo()

    assert methods[NAME]["num_cfs"] == 1
    assert method.load() == [(co2, 1.0)]


@bw2test
def test_registered_but_never_written_raises_missing_data():
    method = Method(NAME)
    method.register()
    with pytest.raises(MissingIntermediateData):
        method.load()
    with pytest.raises(UnknownObject):
        Method(("unknown",)).load()


@bw2test
def test_written_empty_loads_empty():
    method = Method(NAME)
    method.write([], process=False)
    assert method.load() == []
    assert methods[NAME]["num_cfs"] == 0


def test_rewrite_replaces_factors(bio):
    co2, ch4 = bio
    method = Method(NAME)
    method.write([(co2, 1), (ch4, 2)])
    method.write([(ch4, 3)])
    assert method.load() == [(ch4, 3.0)]
    assert CharacterizationFactor.select().count() == 1


def test_replacing_metadata_keeps_factors_and_deleting_removes_them(bio):
    co2, _ = bio
    method = Method(NAME)
    method.write([(co2, 1)])
    methods[NAME] = {**methods[NAME], "unit": "kg"}
    assert method.load() == [(co2, 1.0)]

    del methods[NAME]
    assert CharacterizationFactor.select().count() == 0
    with pytest.raises(UnknownObject):
        method.load()


def test_copy_copies_factors(bio):
    co2, _ = bio
    method = Method(NAME)
    method.write([(co2, {"amount": 1, "loc": 1}, "CH")])
    copy = method.copy(("b",))
    assert copy.load() == method.load()
    assert CharacterizationFactor.select().count() == 2


def test_factors_follow_project_switch(bio):
    co2, _ = bio
    Method(NAME).write([(co2, 1)])
    start = projects.current
    projects.set_current("other")
    assert NAME not in methods
    projects.set_current(start)
    assert Method(NAME).load() == [(co2, 1.0)]


# ----------------------------------------------------------------------
# Weightings and normalizations
# ----------------------------------------------------------------------


@bw2test
def test_weighting_round_trip():
    weighting = Weighting(("w",))
    weighting.write([42])
    assert weighting.load() == [42.0]
    weighting.write([{"amount": 2, "uncertainty type": 0}])
    assert weighting.load() == [{"amount": 2.0, "uncertainty type": 0}]
    assert WeightingFactor.select().count() == 1
    with pytest.raises(ValueError):
        weighting.write([1, 2])
    with pytest.raises(ValueError):
        weighting.write(["heavy"])


def test_normalization_stores_flow_ids(bio):
    co2, ch4 = bio
    normalization = Normalization(("n",))
    normalization.write([(("bio", "co2"), 2), (["bio", "ch4"], {"amount": 3, "loc": 3})])
    assert normalization.load() == [(co2, 2.0), (ch4, {"amount": 3.0, "loc": 3})]
    assert [row.flow for row in NormalizationFactor.select()] == [co2, ch4]
    assert normalization.datapackage()
    with pytest.raises(ValueError):
        normalization.write([(co2, 1, "CH")])


def test_kinds_have_separate_factor_tables(bio):
    co2, _ = bio
    Method(("x",)).write([(co2, 1)])
    Normalization(("x",)).write([(co2, 2)])
    Weighting(("x",)).write([3])
    assert Method(("x",)).load() == [(co2, 1.0)]
    assert Normalization(("x",)).load() == [(co2, 2.0)]
    assert Weighting(("x",)).load() == [3.0]


# ----------------------------------------------------------------------
# replace_cfs
# ----------------------------------------------------------------------


def test_replace_cfs(bio):
    co2, ch4 = bio
    Method(("one",)).write([(co2, 1), (ch4, 2)])
    Method(("two",)).write([(ch4, 3)])
    Method(("three",)).write([(co2, 4, "CH")])

    assert replace_cfs(("bio", "co2"), ("bio", "ch4")) == [("one",), ("three",)]
    assert Method(("one",)).load() == [(ch4, 1.0), (ch4, 2.0)]
    assert Method(("two",)).load() == [(ch4, 3.0)]
    assert Method(("three",)).load() == [(ch4, 4.0, "CH")]
    assert replace_cfs(co2, ch4) == []


def test_replace_cfs_accepts_deleted_old_flow_but_not_missing_new_flow(bio):
    co2, ch4 = bio
    Method(NAME).write([(co2, 1)])
    get_node(id=co2).delete()
    assert replace_cfs(co2, ("bio", "ch4")) == [NAME]
    assert Method(NAME).load() == [(ch4, 1.0)]
    with pytest.raises(UnknownObject):
        replace_cfs(ch4, ("bio", "missing"))


def test_replace_cfs_processes_all_methods_when_one_fails(bio, monkeypatch):
    co2, ch4 = bio
    for name in ("one", "two", "three"):
        Method((name,)).write([(co2, 1)])
    processed = []
    process = Method.process

    def failing_process(self, **kwargs):
        if self.name == ("one",):
            raise RuntimeError("can't process")
        processed.append(self.name)
        return process(self, **kwargs)

    monkeypatch.setattr(Method, "process", failing_process)
    with pytest.raises(ExceptionGroup, match="could not be processed") as excinfo:
        replace_cfs(co2, ch4)
    assert processed == [("two",), ("three",)]
    assert [str(err) for err in excinfo.value.exceptions] == ["can't process"]
    message = str(excinfo.value)
    assert message.count("('one',)") == 2 and message.count("('two',)") == 1
    for name in ("one", "two", "three"):
        assert Method((name,)).load() == [(ch4, 1.0)]


# ----------------------------------------------------------------------
# Migration 5
# ----------------------------------------------------------------------


def _write_pickle(abbreviation, data):
    with open(projects.dir / "intermediate" / f"{abbreviation}.pickle", "wb") as f:
        pickle.dump(data, f, protocol=4)


def _register_legacy(registry, name, abbreviation):
    """Metadata as left by migration 4: registered, data still in a pickle."""
    registry[name] = {"abbreviation": abbreviation}


def _migrate_from_version_4():
    sqlite3_lci_db.execute_sql("PRAGMA main.user_version = 4")
    return migrate(sqlite3_lci_db.db, projects.dir)


def _legacy_method(co2, ch4):
    return [
        (co2, 1),
        (("bio", "ch4"), {"amount": 2, "uncertainty type": np.int64(2), "loc": 2}, "CH"),
        (co2, np.float64(3.5), ("world", "DE")),
        [["bio", "co2"], {"amount": 4}, None],
    ]


def test_migrates_pickles_and_keeps_them(bio):
    co2, ch4 = bio
    _register_legacy(methods, NAME, "m.1")
    _write_pickle("m.1", _legacy_method(co2, ch4))
    _register_legacy(normalizations, ("n",), "n.1")
    _write_pickle("n.1", [(("bio", "co2"), 2), (("bio", "ch4"), {"amount": 3})])
    _register_legacy(weightings, ("w",), "w.1")
    _write_pickle("w.1", [{"amount": 5, "uncertainty type": 0}])

    assert _migrate_from_version_4() == list(range(5, SCHEMA_VERSION + 1))
    assert Method(NAME).load() == [
        (co2, 1.0),
        (ch4, {"amount": 2.0, "uncertainty type": 2, "loc": 2}, "CH"),
        (co2, 3.5, ("world", "DE")),
        (co2, {"amount": 4.0}),
    ]
    assert Normalization(("n",)).load() == [(co2, 2.0), (ch4, {"amount": 3.0})]
    assert Weighting(("w",)).load() == [{"amount": 5.0, "uncertainty type": 0}]
    for abbreviation in ("m.1", "n.1", "w.1"):
        assert (projects.dir / "intermediate" / f"{abbreviation}.pickle").is_file()


def test_migrated_rows_match_rows_written_by_peewee(bio):
    """The raw SQL in the migration stores values exactly as ``write`` does."""
    co2, ch4 = bio
    data = _legacy_method(co2, ch4)
    _register_legacy(methods, NAME, "m.1")
    _write_pickle("m.1", data)
    _register_legacy(normalizations, ("n",), "n.1")
    _write_pickle("n.1", [(("bio", "co2"), {"amount": 2, "scale": np.float32(0.5)})])
    _register_legacy(weightings, ("w",), "w.1")
    _write_pickle("w.1", [7])
    _migrate_from_version_4()
    tables = ("characterizationfactor", "normalizationfactor", "weightingfactor")
    migrated = {table: _stored(table) for table in tables}

    Method(NAME).write(data, process=False)
    Normalization(("n",)).write(
        [(("bio", "co2"), {"amount": 2, "scale": np.float32(0.5)})], process=False
    )
    Weighting(("w",)).write([7], process=False)
    assert {table: _stored(table) for table in tables} == migrated


@bw2test
def test_objects_without_pickle_keep_no_data():
    _register_legacy(methods, NAME, "missing.1")
    _migrate_from_version_4()
    with pytest.raises(MissingIntermediateData):
        Method(NAME).load()


def test_objects_which_cant_be_migrated_are_skipped_with_warning(bio):
    co2, _ = bio
    _register_legacy(methods, ("good",), "good.1")
    _write_pickle("good.1", [(co2, 1)])
    _register_legacy(methods, ("missing flow",), "flow.1")
    _write_pickle("flow.1", [(co2, 1), (("bio", "deleted"), 2)])
    _register_legacy(methods, ("bad location",), "loc.1")
    _write_pickle("loc.1", [(co2, 1, 5)])
    _register_legacy(methods, ("huge id",), "huge.1")
    _write_pickle("huge.1", [(2**70, 1)])
    _register_legacy(methods, ("nan amount",), "nan.1")
    _write_pickle("nan.1", [(co2, 1), (co2, float("nan"))])
    _register_legacy(methods, ("nan in dict",), "nan.2")
    _write_pickle("nan.2", [(co2, {"amount": np.float64("nan"), "uncertainty type": 0})])
    _register_legacy(methods, ("unreadable",), "bad.1")
    (projects.dir / "intermediate" / "bad.1.pickle").write_bytes(b"not a pickle")

    with pytest.warns(UserWarning, match="can't be moved into SQLite") as record:
        _migrate_from_version_4()
    message = str(record[0].message)
    for name in (
        "missing flow",
        "bad location",
        "huge id",
        "nan amount",
        "nan in dict",
        "unreadable",
    ):
        assert name in message
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert Method(("good",)).load() == [(co2, 1.0)]
    for name in (
        ("missing flow",),
        ("bad location",),
        ("huge id",),
        ("nan amount",),
        ("nan in dict",),
        ("unreadable",),
    ):
        with pytest.raises(MissingIntermediateData):
            Method(name).load()
    assert CharacterizationFactor.select().count() == 1


def test_objects_with_data_are_not_migrated_again(bio):
    co2, ch4 = bio
    Method(NAME).write([(co2, 1)])
    _write_pickle(methods[NAME]["abbreviation"], [(ch4, 2)])
    _migrate_from_version_4()
    assert Method(NAME).load() == [(co2, 1.0)]


@bw2test
def test_legacy_project_is_migrated_on_open():
    """A project last opened by bw2data with JSON metadata and pickled data."""
    projects.set_current("legacy")
    Database("bio").write({("bio", "co2"): {"name": "CO2", "type": "emission"}})
    co2 = get_node(code="co2").id
    geomapping.add(["CH"])  # Migrated from geomapping.pickle in a real legacy project
    legacy_dir = projects.dir
    projects.set_current("other")

    with open(legacy_dir / "methods.json", "w", encoding="utf-8") as f:
        json.dump([[list(NAME), {"abbreviation": "am.1", "num_cfs": 1, "unit": "kg"}]], f)
    with open(legacy_dir / "intermediate" / "am.1.pickle", "wb") as f:
        pickle.dump([(("bio", "co2"), 2.5, "CH")], f, protocol=4)
    with sqlite3.connect(legacy_dir / "lci" / "databases.db") as conn:
        conn.execute("DELETE FROM method")
        conn.execute("PRAGMA user_version = 3")
    conn.close()

    projects.set_current("legacy")
    assert schema_version(sqlite3_lci_db.db) == SCHEMA_VERSION
    assert methods[NAME]["unit"] == "kg"
    assert Method(NAME).load() == [(co2, 2.5, "CH")]
    Method(NAME).process()
    assert MethodRecord.get().has_data


def test_migrates_integer_feature_ids_and_numpy_flow_ids(bio):
    """Integer feature ids become strings, as in the migration of ``geomapping.pickle``."""
    co2, ch4 = bio
    _register_legacy(methods, NAME, "m.1")
    _write_pickle(
        "m.1",
        [
            (np.int64(co2), 1.5, ("coll", 5)),
            (np.int32(1), 2, ("coll", np.int64(6))),
            (ch4, 3, ("coll", "7")),
        ],
    )
    _register_legacy(normalizations, ("n",), "n.1")
    _write_pickle("n.1", [(np.int64(ch4), 2)])
    _register_legacy(methods, ("bad",), "bad.1")
    _write_pickle("bad.1", [(co2, 1, ("coll", True))])

    with pytest.warns(UserWarning, match="can't be moved into SQLite") as record:
        _migrate_from_version_4()
    assert "bad" in str(record[0].message) and "a, method" not in str(record[0].message)
    assert Method(NAME).load() == [
        (co2, 1.5, ("coll", "5")),
        (1, 2.0, ("coll", "6")),
        (ch4, 3.0, ("coll", "7")),
    ]
    assert Normalization(("n",)).load() == [(ch4, 2.0)]
    with pytest.raises(MissingIntermediateData):
        Method(("bad",)).load()
