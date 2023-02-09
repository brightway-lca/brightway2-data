import pytest

from bw2data.tests import bw2test
from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    ProjectParameter,
)
from bw2data import config

bw2parameters = pytest.importorskip("bw2parameters", "1.0.0")


@bw2test
def test_warning_no_pint():
    if not bw2parameters.PintWrapper.pint_installed and config.use_pint_parameters:
        with pytest.warns():
            import bw2data.parameters  # noqa


@bw2test
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
