from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    ProjectParameter,
)


def test_get_data_dict():
    param_data = {
        "name": "A",
        "database": "test",
        "group": "foo",
        "code": "some code",
        "amount": 3,
        "maximum": 2,
        "unit": "kg",
        "formula": "1 kg",
        "random field": None,
    }

    pp_result = ProjectParameter().get_data_dict(param_data)
    pp_expected = {
        "database": "test",
        "maximum": 2,
        "unit": "kg",
        "random field": None,
        "group": "foo",
        "code": "some code",
    }
    assert pp_result == pp_expected

    dp_result = DatabaseParameter().get_data_dict(param_data)
    dp_expected = {
        "maximum": 2,
        "unit": "kg",
        "random field": None,
        "group": "foo",
        "code": "some code",
    }
    assert dp_result == dp_expected

    ap_result = ActivityParameter().get_data_dict(param_data)
    ap_expected = {"maximum": 2, "unit": "kg", "random field": None}
    assert ap_result == ap_expected
