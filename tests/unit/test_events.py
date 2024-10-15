from bw2data import revisions
from bw2data.backends.schema import ActivityDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.tests import bw2test


@bw2test
def test_setting_sourced():
    projects.set_current("sourced-project")
    # Default is not sourced
    assert not projects.dataset.is_sourced

    projects.dataset.set_sourced()
    assert projects.dataset.is_sourced
    assert projects.dataset.revision is not None


@bw2test
def test_writes_activity_evt_on_save():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    database = DatabaseChooser("db")
    database.register()
    activity = database.new_node(code="A", name="A")
    previous_revision = projects.dataset.revision
    activity.save()
    assert projects.dataset.revision != previous_revision
    activity = database.new_node(code="B", name="B")
    previous_revision = projects.dataset.revision
    activity.save()
    assert projects.dataset.revision != previous_revision
    activity["name"] = "bar"
    previous_revision = projects.dataset.revision
    activity.save()
    assert projects.dataset.revision != previous_revision


@bw2test
def test_writes_activity_evt_on_db_write():
    """
        key=("db", "foo"),
        name="foo",
        type="production",
        location="bar",
    )
    """
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    previous_revision = projects.dataset.revision
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "foo"): {
                "exchanges": [
                    {
                        "input": ("db", "foo"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "baz",
            },
        }
    )
    activity = database.get("foo")
    assert activity["name"] == "baz"
    activity["foo"] = "bar"
    activity.save()
    act = DatabaseChooser("db").get("foo")
    assert act["foo"] == "bar"
    assert projects.dataset.revision != previous_revision


@bw2test
def test_writes_exchange_evt_on_save():
    projects.set_current("activity-event")
    projects.dataset.set_sourced()
    previous_revision = projects.dataset.revision
    database = DatabaseChooser("db")
    database.write(
        {
            ("db", "foo"): {
                "exchanges": [
                    {
                        "input": ("db", "foo"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "bar",
            },
        }
    )
    activity = database.get("foo")
    new_exchange = activity.new_edge(
        input=("db", "foo"), amount=0.5, type="production", unit="kilogram"
    )
    new_exchange.save()
    assert projects.dataset.revision != previous_revision


@bw2test
def test_diffs():
    pass


@bw2test
def test_load_diff():
    """
    Load example diff
    projects.set_current("example-diff")
    project.import()
    """
    pass


@bw2test
def test_apply_revision():
    rev, obj_db, obj_name, obj_code = "r0", "db0", "a0", "c0"
    projects.set_current("test_apply")
    projects.dataset.set_sourced()
    database = DatabaseChooser(obj_db)
    database.register()
    data = {"database": obj_db, "name": obj_name, "code": obj_code}
    revision = revisions.generate_revision(
        revisions.generate_metadata(None, rev),
        (revisions.generate_delta(None, ActivityDataset(data=data, **data)),),
    )
    assert not ActivityDataset.select().exists()
    projects.dataset.apply_revision(revision)
    activity = database.get(obj_code)
    assert activity._document.name == obj_name
    assert projects.dataset.revision == rev


def test_apply_revisions():
    root, head = "r0", "r2"
    code0, code1, code2 = "c0", "c1", "c2"
    obj_id, obj_db, obj_name = 1, "db0", "a0"
    projects.set_current("test_apply_sequence_to_obj")
    projects.dataset.set_sourced()
    database = DatabaseChooser(obj_db)
    database.register()
    data0 = {"database": obj_db, "name": obj_name, "code": code0}
    data1 = {**data0, "id": 1, "code": code1}
    data2 = {**data0, "id": 1, "code": code2}
    a0 = ActivityDataset(data=data0, **data0)
    a1 = ActivityDataset(data=data1, **data1)
    a2 = ActivityDataset(data=data2, **data2)
    projects.dataset.apply_revision(
        revisions.generate_revision(
            revisions.generate_metadata(None, root),
            (revisions.generate_delta(None, a0),),
        )
    )
    projects.dataset.apply_revision(
        revisions.generate_revision(
            revisions.generate_metadata(root, "r1"),
            (revisions.generate_delta(a0, a1),),
        )
    )
    projects.dataset.apply_revision(
        revisions.generate_revision(
            revisions.generate_metadata("r1", head),
            (revisions.generate_delta(a1, a2),),
        )
    )
    activity = database.get(code2)
    assert activity._document.database == obj_db
    assert activity._document.name == obj_name
    assert activity._document.code == code2
    assert projects.dataset.revision == head
