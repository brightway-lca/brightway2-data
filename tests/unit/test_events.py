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
def test_apply():
    revision, obj_db, obj_name, obj_code = "r0", "db0", "a0", "c0"
    projects.set_current("test_apply")
    projects.dataset.set_sourced()
    database = DatabaseChooser(obj_db)
    database.register()
    projects.dataset.apply_revision(
        {
            "metadata": {"revision": revision},
            "data": [
                {
                    "type": "activitydataset",
                    "delta": {
                        "type_changes": {
                            "root": {
                                "old_type": "NoneType",
                                "new_type": "dict",
                                "new_value": {
                                    "database": obj_db,
                                    "name": obj_name,
                                    "code": obj_code,
                                    "data": {
                                        "database": obj_db,
                                        "name": obj_name,
                                        "code": obj_code,
                                    },
                                },
                            },
                        },
                    },
                }
            ],
        }
    )
    activity = database.get(obj_code)
    assert activity._document.name == obj_name
    assert projects.dataset.revision == revision
