import pytest

from bw2data.backends import ActivityDataset, ExchangeDataset
from bw2data.database import DatabaseChooser
from bw2data.errors import ValidityError, UnknownObject
from bw2data.parameters import ActivityParameter, ParameterizedExchange, parameters
from bw2data.tests import bw2test
from bw2data.utils import get_activity


@bw2test
def test_change_code_not_unique_raises_error():
    database = DatabaseChooser("a database")
    database.write(
        {
            ("a database", "foo"): {
                "exchanges": [
                    {
                        "input": ("a database", "foo"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "baz",
            },
            ("a database", "already there"): {
                "exchanges": [
                    {
                        "input": ("a database", "already there"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "baz",
            },
        }
    )
    act = database.get("foo")
    with pytest.raises(ValueError):
        act["code"] = "already there"


@bw2test
def test_save_invalid_activity_raises_error():
    db = DatabaseChooser("a database")
    db.register()
    act = db.new_activity("foo")
    with pytest.raises(ValidityError):
        act.save()


@pytest.fixture
@bw2test
def activity():
    database = DatabaseChooser("a database")
    database.write(
        {
            ("a database", "foo"): {
                "exchanges": [
                    {
                        "input": ("a database", "foo"),
                        "amount": 1,
                        "type": "production",
                    }
                ],
                "location": "bar",
                "name": "baz",
            },
        }
    )
    return database.get("foo")


def test_set_item(activity):
    activity["foo"] = "bar"
    activity.save()
    act = DatabaseChooser("a database").get("foo")
    assert act["foo"] == "bar"


def test_key(activity):
    assert activity.key == ("a database", "foo")


def test_change_code(activity):
    db = DatabaseChooser("a database")
    assert len(db) == 1
    old_key = activity.key[:]
    activity["code"] = "a new one"
    assert len(db) == 1
    assert get_activity(("a database", "a new one"))
    with pytest.raises(UnknownObject):
        get_activity(old_key)


def test_change_code_same_code(activity):
    activity["code"] = "foo"


def test_change_database(activity):
    db = DatabaseChooser("a database")
    db2 = DatabaseChooser("another database")
    db2.write({})
    assert len(db2) == 0
    assert len(db) == 1
    old_key = activity.key[:]
    assert len(get_activity(old_key).production()) == 1
    activity["database"] = "another database"
    assert len(db) == 0
    assert len(db2) == 1
    assert get_activity(("another database", "foo"))
    assert len(get_activity(("another database", "foo")).production()) == 1
    with pytest.raises(UnknownObject):
        get_activity(old_key)


def test_change_database_not_exist(activity):
    with pytest.raises(ValueError):
        activity["database"] = "nope!"


def test_database_same_database(activity):
    activity["database"] = "a database"


def test_delete(activity):
    assert ExchangeDataset.select().count() == 1
    assert ActivityDataset.select().count() == 1
    activity.delete()
    assert ExchangeDataset.select().count() == 0
    assert ActivityDataset.select().count() == 0

@bw2test
def test_delete_activity_only_self_references():
    database = DatabaseChooser("a database")
    database.write({
        ("a database", "foo"): {
            'exchanges': [{
                'input': ("a database", "foo"),
                'amount': 1,
                'type': 'production',
            }, {
                'input': ("a database", "foo"),
                'amount': 0.1,
                'type': 'technosphere',
            }],
            'location': 'bar',
            'name': 'baz'
        },
    })
    activity = database.get('foo')

    assert ExchangeDataset.select().count() == 2
    assert ActivityDataset.select().count() == 1
    activity.delete()
    assert ExchangeDataset.select().count() == 0
    assert ActivityDataset.select().count() == 0


def test_copy(activity):
    assert ExchangeDataset.select().count() == 1
    assert ActivityDataset.select().count() == 1
    cp = activity.copy("baz")
    assert cp["code"] != activity["code"]
    assert cp["name"] == "baz"
    assert cp["location"] == "bar"
    assert ExchangeDataset.select().count() == 2
    assert ActivityDataset.select().count() == 2
    assert (
        ActivityDataset.select()
        .where(
            ActivityDataset.code == cp["code"],
            ActivityDataset.database == cp["database"],
        )
        .count()
        == 1
    )
    assert (
        ActivityDataset.select()
        .where(
            ActivityDataset.code == activity["code"],
            ActivityDataset.database == activity["database"],
        )
        .count()
        == 1
    )
    assert (
        ExchangeDataset.select()
        .where(
            ExchangeDataset.input_code == cp["code"],
            ExchangeDataset.input_database == cp["database"],
        )
        .count()
        == 1
    )
    assert (
        ExchangeDataset.select()
        .where(
            ExchangeDataset.input_database == activity["database"],
            ExchangeDataset.input_code == activity["code"],
        )
        .count()
        == 1
    )


def test_copy_with_kwargs(activity):
    assert ExchangeDataset.select().count() == 1
    assert ActivityDataset.select().count() == 1
    cp = activity.copy("baz", location="here", widget="squirt gun")
    assert cp["code"] != activity["code"]
    assert cp["name"] == "baz"
    assert cp["location"] == "here"
    assert cp["widget"] == "squirt gun"
    assert ExchangeDataset.select().count() == 2
    assert ActivityDataset.select().count() == 2


@bw2test
def test_delete_activity_parameters():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(
        amount=0, input=b, type="technosphere", formula="foo * bar + 4"
    ).save()

    activity_data = [
        {
            "name": "reference_me",
            "formula": "sqrt(25)",
            "database": "example",
            "code": "B",
        },
        {
            "name": "bar",
            "formula": "reference_me + 2",
            "database": "example",
            "code": "A",
        },
    ]
    parameters.new_activity_parameters(activity_data, "my group")
    parameters.add_exchanges_to_group("my group", a)

    assert ActivityParameter.select().count() == 2
    assert ParameterizedExchange.select().count() == 1

    a.delete()
    assert ActivityParameter.select().count() == 1
    assert not ParameterizedExchange.select().count()


@bw2test
def test_get_classifications_ref_product():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(
        amount=1,
        input=b,
        type="production",
        classifications={'CPC': ['17300: Steam and hot water']},
    ).save()

    assert a['CPC'] == ['17300: Steam and hot water']


@bw2test
def test_get_classifications_main_activity_dict():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity", classifications={
                        'EcoSpold01Categories': 'transport systems/train',
                        'ISIC rev.4 ecoinvent': '4912:Freight rail transport',
                        'CPC': '6512: Railway transport services of freight'})
    a.save()

    assert a['CPC'] == '6512: Railway transport services of freight'
    assert a['ISIC rev.4 ecoinvent'] == '4912:Freight rail transport'
    assert a['EcoSpold01Categories'] == 'transport systems/train'


@bw2test
def test_get_classifications_main_activity_list():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity", classifications=[('EcoSpold01Categories', 'transport systems/train'),
 ('ISIC rev.4 ecoinvent', '4912:Freight rail transport'),
 ('CPC', '6512: Railway transport services of freight')])
    a.save()

    assert a['CPC'] == '6512: Railway transport services of freight'
    assert a['ISIC rev.4 ecoinvent'] == '4912:Freight rail transport'
    assert a['EcoSpold01Categories'] == 'transport systems/train'


@bw2test
def test_get_classifications_also_in_activity():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity", CPC='foo')
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(
        amount=1,
        input=b,
        type="production",
        classifications={'CPC': ['17300: Steam and hot water']},
    ).save()

    assert a['CPC'] == 'foo'


@bw2test
def test_get_properties_ref_product():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(
        amount=1,
        input=b,
        type="production",
        properties={'corresponding fuel use, propane, furnace >100kW': 7}
    ).save()

    assert a['corresponding fuel use, propane, furnace >100kW'] == 7


@bw2test
def test_get_properties_missing_property():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()
    a.new_exchange(
        amount=1,
        input=b,
        type="production",
        properties={'corresponding fuel use, propane, furnace >100kW': 7}
    ).save()

    with pytest.raises(KeyError):
        a['CPC']


@bw2test
def test_get_properties_no_rp_exchange():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="Another activity")
    b.save()

    with pytest.raises(KeyError):
        a['CPC']


@bw2test
def test_rp_exchange_single_production_wrong_rp_name():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a['reference product'] = 'something'
    a.save()
    b = db.new_activity(code="B", name="else")
    b.save()
    a.new_exchange(
        amount=1,
        input=b,
        type="production",
    ).save()

    exc = a.rp_exchange()
    assert exc.input.id == b.id and exc.output.id == a.id


@bw2test
def test_rp_exchange_multiple_produuction_match_rp_name():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a['reference product'] = 'something'
    a.save()
    b = db.new_activity(code="B", name="else")
    b.save()
    c = db.new_activity(code="C", name="something")
    c.save()
    a.new_exchange(
        amount=1,
        input=b,
        type="production",
    ).save()
    a.new_exchange(
        amount=1,
        input=c,
        type="production",
    ).save()

    exc = a.rp_exchange()
    assert exc.input.id == c.id and exc.output.id == a.id


@bw2test
def test_rp_exchange_value_error_multiple():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="else")
    b.save()
    c = db.new_activity(code="C", name="something")
    c.save()
    a.new_exchange(
        amount=1,
        input=b,
        type="production",
    ).save()
    a.new_exchange(
        amount=1,
        input=c,
        type="production",
    ).save()

    with pytest.raises(ValueError):
        a.rp_exchange()


@bw2test
def test_rp_exchange_value_error_no_production():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="else")
    b.save()

    with pytest.raises(ValueError):
        a.rp_exchange()

@bw2test
def test_rp_exchange_value_error_only_substitution():
    db = DatabaseChooser("example")
    db.register()

    a = db.new_activity(code="A", name="An activity")
    a.save()
    b = db.new_activity(code="B", name="else")
    b.save()
    a.new_exchange(
        amount=1,
        input=b,
        type="substitution",
    ).save()

    with pytest.raises(ValueError):
        a.rp_exchange()
