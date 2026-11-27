"""LCIA methods, normalizations and weightings in read-only LCI extensions (extensions PR 7):
exporting them with the databases of their flows, the overlay of their tables, the guards, and
what the project does with their processed data and locations."""

import json
import sqlite3
import warnings

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
from bw2data.backends.extensions import (
    INFO_LCIA_FINGERPRINTS,
    LCIA_NAME,
    READ_ONLY_LCIA,
    lcia_extension_of,
    lcia_fingerprint,
)
from bw2data.backends.schema import CharacterizationFactor, MethodRecord
from bw2data.backends.utils import replace_cfs
from bw2data.errors import ExtensionError, ReadOnlyExtensionError

from .test_extension_attach import _temporary_directory, lci_path
from .test_extension_export import BIO, INV, file_rows, info

METHOD = ("bio method", "total")
REGIONAL = ("bio method", "regional")
NORMALIZATION = ("bio normalization",)
WEIGHTING = ("bio weighting",)
CFS = [(("bio", "0"), 10), (("bio", "1"), {"amount": 1, "uncertainty_type": 0})]
REGIONAL_CFS = [(("bio", "0"), 5, "CH"), (("bio", "2"), 7, ("world", "Mars"))]


def write_lcia():
    Method(METHOD).write(CFS)
    Method(METHOD).metadata["unit"] = "points"
    Method(METHOD).metadata.save()
    Method(REGIONAL).write(REGIONAL_CFS)
    Normalization(NORMALIZATION).write([(("bio", "0"), 0.5)])
    Weighting(WEIGHTING).write([{"amount": 2}])


def export_bio(path, **kwargs):
    lcia = {
        "methods": [METHOD, REGIONAL],
        "normalizations": [NORMALIZATION],
        "weightings": [WEIGHTING],
    }
    lcia.update(kwargs)
    return projects.export_extension(path, ["bio"], version="1.0", **lcia)


@pytest.fixture
def built(tmp_path):
    """``bio.db``, with database ``bio`` and its LCIA objects, exported from project
    ``bio-builder``, and ``inv.db`` (requires ``bio``) from project ``inv-builder``. Then a new,
    empty project ``user`` is current."""
    _temporary_directory()
    bio, inv = tmp_path / "bio.db", tmp_path / "inv.db"
    projects.set_current("bio-builder")
    Database("bio").write(BIO)
    write_lcia()
    export_bio(bio)
    projects.set_current("inv-builder")
    projects.attach_extension(bio)
    Database("inv").write(INV)
    projects.export_extension(inv, "inv", version="2.0")
    projects.set_current("user")
    yield bio, inv
    sqlite3_lci_db.change_path(lci_path())


# The file


def test_file_contents(built):
    bio, inv = built
    names = [json.loads(row[0]) for row in file_rows(bio, "SELECT name FROM method ORDER BY name")]
    assert names == [list(REGIONAL), list(METHOD)]
    assert file_rows(bio, "SELECT count(*) FROM characterizationfactor") == [(4,)]
    assert file_rows(bio, "SELECT count(*) FROM normalizationfactor") == [(1,)]
    assert file_rows(bio, "SELECT count(*) FROM weightingfactor") == [(1,)]
    # Metadata and locations by name
    ((extra,),) = file_rows(
        bio, "SELECT extra FROM method WHERE name = ?", (json.dumps(list(METHOD)),)
    )
    assert json.loads(extra) == {"unit": "points"}
    assert sorted(
        file_rows(
            bio,
            "SELECT geocollection, location FROM characterizationfactor WHERE location IS NOT NULL",
        ),
        key=str,
    ) == [("world", "Mars"), (None, "CH")]
    # Every factor is for an activity in the file
    assert file_rows(
        bio,
        "SELECT count(*) FROM characterizationfactor WHERE flow NOT IN (SELECT id FROM activitydataset)",
    ) == [(0,)]
    # The inventory file has none
    assert file_rows(inv, "SELECT count(*) FROM method") == [(0,)]
    assert INFO_LCIA_FINGERPRINTS not in info(inv)

    fingerprints = json.loads(info(bio)[INFO_LCIA_FINGERPRINTS])
    conn = sqlite3.connect(bio)
    try:
        expected = {
            "method": [
                [list(name), lcia_fingerprint(conn, "main", "method", name)]
                for name in (REGIONAL, METHOD)
            ],
            "normalization": [
                [
                    list(NORMALIZATION),
                    lcia_fingerprint(conn, "main", "normalization", NORMALIZATION),
                ]
            ],
            "weighting": [
                [
                    list(WEIGHTING),
                    lcia_fingerprint(conn, "main", "weighting", WEIGHTING),
                ]
            ],
        }
    finally:
        conn.close()
    assert fingerprints == expected
    assert len({fp for pairs in expected.values() for _, fp in pairs}) == 4


def test_ids_are_kept(built):
    bio, _ = built
    projects.set_current("bio-builder")
    ids = {row.name: row.id for row in MethodRecord.select()}
    assert {
        tuple(json.loads(name)): id_ for id_, name in file_rows(bio, "SELECT id, name FROM method")
    } == ids


def test_fingerprint():
    """The same name and factors give the same fingerprint, in any order and with other
    metadata and ids; another amount, flow or location gives another one."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE activitydataset (id INTEGER PRIMARY KEY, database TEXT, code TEXT);
        CREATE TABLE method (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE characterizationfactor (method_id INTEGER, position INTEGER, flow INTEGER,
            amount REAL, uncertainty TEXT, geocollection TEXT, location TEXT);
        INSERT INTO activitydataset VALUES (1, 'bio', 'a'), (2, 'bio', 'b');
        INSERT INTO method VALUES (10, '["m"]'), (11, '["n"]');
        INSERT INTO characterizationfactor VALUES
            (10, 0, 1, 1.0, NULL, NULL, NULL), (10, 1, 2, 2.0, '{"a": 1, "b": 2}', NULL, 'CH'),
            (11, 0, 2, 2.0, '{"b":2,"a":1}', NULL, 'CH'), (11, 1, 1, 1.0, NULL, NULL, NULL);
        """
    )
    first = lcia_fingerprint(conn, "main", "method", ("m",))
    conn.execute("UPDATE method SET name = '[\"m\"]' WHERE id = 11")
    conn.execute("UPDATE method SET name = '[\"x\"]' WHERE id = 10")
    assert lcia_fingerprint(conn, "main", "method", ("m",)) == first
    assert lcia_fingerprint(conn, "main", "method", ("x",)) != first
    for change in (
        "UPDATE characterizationfactor SET amount = 3 WHERE method_id = 11 AND position = 1",
        "UPDATE characterizationfactor SET location = 'DE' WHERE method_id = 11 AND position = 0",
        "UPDATE activitydataset SET code = 'c' WHERE id = 1",
    ):
        conn.execute(change)
        changed = lcia_fingerprint(conn, "main", "method", ("m",))
        assert changed != first
        first = changed
    with pytest.raises(KeyError):
        lcia_fingerprint(conn, "main", "method", ("nothing",))
    conn.close()


def record_id(name):
    return next(row.id for row in MethodRecord.select() if row.name == name)


# Exporting


def test_flows_must_be_in_the_exported_databases(built, tmp_path):
    projects.set_current("bio-builder")
    Database("other").write({("other", "x"): {"name": "x", "type": "emission"}})
    Method(("mixed",)).write([(("bio", "0"), 1), (("other", "x"), 2)])
    with pytest.raises(ExtensionError, match=r"'mixed'.*\('other', 'x'\)"):
        projects.export_extension(tmp_path / "x.db", ["bio"], methods=[("mixed",)])
    Normalization(("n",)).write([(("other", "x"), 1)])
    with pytest.raises(ExtensionError, match="normalization"):
        projects.export_extension(tmp_path / "x.db", ["bio"], normalizations=[("n",)])
    assert not (tmp_path / "x.db").exists()
    # With the database of all its flows
    projects.export_extension(tmp_path / "x.db", ["bio", "other"], methods=[("mixed",)])


def test_flows_in_an_attached_extension_are_refused(built, tmp_path):
    """A method of a project which uses the flows of an attached extension can't be exported
    on its own: its flows would be in another file."""
    bio, _ = built
    projects.set_current("inv-builder")
    Method(("mine",)).write([(("bio", "0"), 1)])
    with pytest.raises(ExtensionError, match=r"'mine'.*not in the exported databases"):
        projects.export_extension(tmp_path / "x.db", ["inv"], methods=[("mine",)])


def test_refused_objects(built, tmp_path):
    bio, _ = built
    projects.set_current("inv-builder")
    # Stored in an extension
    with pytest.raises(ExtensionError, match="copy it into the project"):
        projects.export_extension(tmp_path / "x.db", ["inv"], methods=[METHOD])
    projects.set_current("bio-builder")
    with pytest.raises(ExtensionError, match="the project has no method"):
        projects.export_extension(tmp_path / "x.db", ["bio"], methods=[("nothing",)])
    methods[("empty",)] = {}
    with pytest.raises(ExtensionError, match="has no data"):
        projects.export_extension(tmp_path / "x.db", ["bio"], methods=[("empty",)])
    with pytest.raises(ValueError, match="list of names"):
        projects.export_extension(tmp_path / "x.db", ["bio"], methods=METHOD)
    with pytest.raises(ValueError, match="distinct"):
        projects.export_extension(tmp_path / "x.db", ["bio"], methods=[METHOD, list(METHOD)])
    assert not (tmp_path / "x.db").exists()


def test_weightings_alone(built, tmp_path):
    """Weightings have no flows, so a file can hold them without any database."""
    projects.set_current("bio-builder")
    projects.export_extension(tmp_path / "w.db", [], weightings=[WEIGHTING], overwrite=True)
    projects.set_current("user")
    projects.attach_extension(tmp_path / "w.db")
    assert list(weightings) == [WEIGHTING]
    assert Weighting(WEIGHTING).load() == [{"amount": 2}]


# Attaching


def test_attached_objects_are_read(built):
    bio, _ = built
    (entry,) = projects.attach_extension(bio)
    assert entry.methods == (REGIONAL, METHOD)
    assert entry.normalizations == (NORMALIZATION,)
    assert entry.weightings == (WEIGHTING,)
    assert sorted(methods) == sorted([METHOD, REGIONAL])
    assert list(normalizations) == [NORMALIZATION]
    assert list(weightings) == [WEIGHTING]
    assert methods[METHOD]["unit"] == "points"
    assert methods[METHOD]["num_cfs"] == 2
    flows = {node.key: node.id for node in Database("bio")}
    assert Method(METHOD).load() == [
        (flows[("bio", "0")], 10),
        (flows[("bio", "1")], {"amount": 1, "uncertainty_type": 0}),
    ]
    assert Method(REGIONAL).load() == [
        (flows[("bio", "0")], 5, "CH"),
        (flows[("bio", "2")], 7, ("world", "Mars")),
    ]
    assert Normalization(NORMALIZATION).load() == [(flows[("bio", "0")], 0.5)]
    assert Weighting(WEIGHTING).load() == [{"amount": 2}]
    (attached,) = sqlite3_lci_db.extensions
    assert set(attached.lcia_fingerprints["method"]) == {METHOD, REGIONAL}
    assert lcia_extension_of("method", METHOD) is attached
    assert lcia_extension_of("method", list(METHOD)) is attached
    assert lcia_extension_of("method", ("nothing",)) is None
    # The locations of its factors get geomapping ids
    assert "CH" in geomapping and ("world", "Mars") in geomapping


def test_name_used_in_the_project(built):
    bio, _ = built
    Database("bio2").write({("bio2", "x"): {"name": "x", "type": "emission"}})
    Method(METHOD).write([(("bio2", "x"), 1)])
    with pytest.raises(ExtensionError, match=r"Method \('bio method', 'total'\) is in"):
        projects.attach_extension(bio)
    assert projects.extensions == ()


def test_name_used_in_two_extensions(built, tmp_path):
    bio, _ = built
    projects.set_current("other-builder")
    Database("bio2").write({("bio2", "x"): {"name": "x", "type": "emission"}})
    Method(METHOD).write([(("bio2", "x"), 1)])
    projects.export_extension(tmp_path / "bio2.db", ["bio2"], methods=[METHOD])
    projects.set_current("user")
    projects.attach_extension(bio)
    with pytest.raises(ExtensionError, match="each method can only be stored in one file"):
        projects.attach_extension(tmp_path / "bio2.db")


def test_old_files_without_lcia_still_attach(built):
    """Files built before this change have empty LCIA tables and no fingerprints for them."""
    bio, inv = built
    projects.attach_extension([bio, inv])
    assert {e.name: e.methods for e in projects.extensions}["inv"] == ()
    assert sqlite3_lci_db.extensions[1].lcia_fingerprints == {}


def test_bad_fingerprints_are_refused(built, tmp_path):
    bio, _ = built
    conn = sqlite3.connect(bio)
    conn.execute(
        'UPDATE extension_info SET value = \'{"method": ["x"]}\' WHERE key = ?',
        (INFO_LCIA_FINGERPRINTS,),
    )
    conn.commit()
    conn.close()
    with pytest.raises(ExtensionError, match=INFO_LCIA_FINGERPRINTS):
        projects.attach_extension(bio)


# Guards


def test_extension_objects_cannot_be_changed(built):
    bio, _ = built
    projects.attach_extension(bio)
    flow = get_node(database="bio", code="2")
    with pytest.raises(ReadOnlyExtensionError, match="read-only"):
        Method(METHOD).write([(flow, 1)])
    with pytest.raises(ReadOnlyExtensionError):
        Method(METHOD).deregister()
    with pytest.raises(ReadOnlyExtensionError):
        del methods[METHOD]
    with pytest.raises(ReadOnlyExtensionError):
        methods[METHOD] = {"unit": "other"}
    proxy = Method(METHOD).metadata
    proxy["unit"] = "other"
    with pytest.raises(ReadOnlyExtensionError):
        proxy.save()
    # The refused change is still pending
    proxy["unit"] = "points"
    with pytest.raises(ReadOnlyExtensionError):
        Normalization(NORMALIZATION).write([(flow, 1)])
    with pytest.raises(ReadOnlyExtensionError):
        Weighting(WEIGHTING).write([1])
    # Nothing has changed
    assert len(Method(METHOD).load()) == 2
    assert methods[METHOD]["unit"] == "points"
    assert ("mine",) not in methods


def test_triggers_are_the_backstop(built):
    """Writes which don't go through the Python guards are refused by the triggers."""
    bio, _ = built
    projects.attach_extension(bio)
    method_id = record_id(METHOD)
    with pytest.raises(ReadOnlyExtensionError, match=READ_ONLY_LCIA):
        CharacterizationFactor.delete().where(CharacterizationFactor.method == method_id).execute()
    with pytest.raises(ReadOnlyExtensionError, match=READ_ONLY_LCIA):
        CharacterizationFactor.update(amount=0).where(
            CharacterizationFactor.method == method_id
        ).execute()
    with pytest.raises(ReadOnlyExtensionError, match=READ_ONLY_LCIA):
        CharacterizationFactor.insert(method=method_id, position=99, flow=1, amount=1).execute()
    with pytest.raises(ReadOnlyExtensionError, match=READ_ONLY_LCIA):
        MethodRecord.update(abbreviation="x").where(MethodRecord.id == method_id).execute()
    with pytest.raises(ReadOnlyExtensionError, match=READ_ONLY_LCIA):
        MethodRecord.delete().where(MethodRecord.id == method_id).execute()
    with pytest.raises(ReadOnlyExtensionError, match=LCIA_NAME):
        MethodRecord.insert(id=1, name=METHOD).execute()
    # A factor of a method of the project can't be moved into one of the extension
    Method(("mine",)).write([(get_node(database="bio", code="0"), 1)])
    mine = record_id(("mine",))
    with pytest.raises(ReadOnlyExtensionError, match=READ_ONLY_LCIA):
        CharacterizationFactor.update(method=method_id).where(
            CharacterizationFactor.method == mine
        ).execute()
    with pytest.raises(ReadOnlyExtensionError, match=LCIA_NAME):
        MethodRecord.update(name=METHOD).where(MethodRecord.id == mine).execute()
    assert len(Method(METHOD).load()) == 2


def test_objects_of_the_project_stay_editable(built):
    bio, _ = built
    projects.attach_extension(bio)
    flow = get_node(database="bio", code="2")
    Method(("mine",)).write([(flow, 3)])
    Method(("mine",)).metadata["unit"] = "kg"
    Method(("mine",)).metadata.save()
    assert Method(("mine",)).load() == [(flow.id, 3)]
    Method(("mine",)).write([(flow, 4)])
    assert Method(("mine",)).load() == [(flow.id, 4)]
    Normalization(("my n",)).write([(flow, 1)])
    Weighting(("my w",)).write([3])
    Method(("mine",)).deregister()
    del normalizations[("my n",)]
    del weightings[("my w",)]
    assert ("mine",) not in methods
    assert sorted(methods) == sorted([METHOD, REGIONAL])
    with pytest.raises(KeyError):
        del methods[("mine",)]


def test_copy_into_the_project(built):
    bio, _ = built
    projects.attach_extension(bio)
    copy = Method(METHOD).copy(("my copy",))
    assert copy.load() == Method(METHOD).load()
    assert methods[("my copy",)]["unit"] == "points"
    assert lcia_extension_of("method", ("my copy",)) is None
    copy.write([(get_node(database="bio", code="2"), 1)])
    # The copy uses the extension's flows, so the extension can't be detached
    with pytest.raises(ExtensionError, match="my copy"):
        projects.detach_extension("bio")


def test_replace_cfs_keeps_extension_methods(built):
    bio, _ = built
    projects.attach_extension(bio)
    old, new = get_node(database="bio", code="0"), get_node(database="bio", code="2")
    Method(("mine",)).write([(old, 1)])
    assert replace_cfs(old.id, new.id) == [("mine",)]
    assert Method(("mine",)).load() == [(new.id, 1)]
    assert Method(METHOD).load()[0][0] == old.id


# Calculations and processed data


def test_round_trip(built):
    from bw2calc import LCA

    from bw2data import prepare_lca_inputs

    bio, inv = built
    projects.attach_extension([bio, inv])
    processed = Method(METHOD).filepath_processed()
    assert not processed.exists()
    demand, data_objs, remapping = prepare_lca_inputs(
        {get_node(database="inv", code="b"): 1}, method=METHOD
    )
    lca = LCA(demand, data_objs=data_objs, remapping_dicts=remapping)
    lca.lci()
    lca.lcia()
    # 0.5 kg steel with 2 kg of flow 0 each, at 10
    assert lca.score == pytest.approx(10)
    # Processed in the project, when first used
    assert processed.exists()
    Normalization(NORMALIZATION).process()
    Weighting(WEIGHTING).process()
    Method(REGIONAL).process()

    projects.detach_extension("inv")
    projects.detach_extension("bio")
    # The processed data of its LCIA objects is removed with it
    assert not processed.exists()
    assert not Normalization(NORMALIZATION).filepath_processed().exists()
    assert sorted(methods) == []


def test_stale_processed_data_is_removed_on_attach(built):
    """Processed data of another object with the same name, e.g. of a method of the project
    which was deleted, is not taken for the extension's."""
    bio, _ = built
    Database("bio2").write({("bio2", "x"): {"name": "x", "type": "emission"}})
    Method(METHOD).write([(("bio2", "x"), 1)])
    processed = Method(METHOD).filepath_processed()
    assert processed.exists()
    Method(METHOD).deregister()
    assert processed.exists()
    projects.attach_extension(bio)
    assert not processed.exists()


def test_update_removes_processed_data(built, tmp_path):
    bio, _ = built
    projects.attach_extension(bio)
    Method(METHOD).process()
    processed = Method(METHOD).filepath_processed()
    assert processed.exists()
    # A new version, with another factor
    projects.set_current("bio-builder")
    Method(METHOD).write([(("bio", "0"), 20)])
    new = tmp_path / "bio-2.db"
    export_bio(new)
    projects.set_current("user")
    projects.update_extension("bio", new)
    assert not processed.exists()
    assert [amount for _, amount in Method(METHOD).load()] == [20]
    (attached,) = sqlite3_lci_db.extensions
    old_fingerprints = json.loads(info(bio)[INFO_LCIA_FINGERPRINTS])["method"]
    assert (
        attached.lcia_fingerprints["method"][METHOD]
        != dict((tuple(name), fp) for name, fp in old_fingerprints)[METHOD]
    )
    assert (
        attached.lcia_fingerprints["method"][REGIONAL]
        == dict((tuple(name), fp) for name, fp in old_fingerprints)[REGIONAL]
    )


def test_processed_data_which_cannot_be_removed(built, monkeypatch):
    bio, _ = built
    projects.attach_extension(bio)
    Method(METHOD).process()
    path_type = type(Method(METHOD).filepath_processed())

    def unlink(self, missing_ok=False):
        raise PermissionError("in use")

    monkeypatch.setattr(path_type, "unlink", unlink)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        projects.detach_extension("bio")
    messages = [str(warning.message) for warning in caught]
    # Of the database, and of the method
    assert any("of an LCIA object" in message for message in messages), messages
    assert projects.extensions == ()
