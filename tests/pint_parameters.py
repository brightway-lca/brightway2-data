import pytest

from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    ProjectParameter,
    Interpreter,
    ParameterManager,
)
from bw2data import config, Database, projects
import shutil

bw2parameters = pytest.importorskip("bw2parameters", "1.0.0")

# repeat tests in tests/parameters.py with PintInterpreter and PintParameterSet
if bw2parameters.PintWrapper.pint_installed:
    config.use_pint_parameters = True
    from .parameters import *  # noqa


def test_config():
    config.use_pint_parameters = True
    i = Interpreter()
    if bw2parameters.PintWrapper.pint_installed:
        assert i.__class__ == bw2parameters.PintInterpreter
    else:
        assert i.__class__ == bw2parameters.DefaultInterpreter
    config.use_pint_parameters = False
    i = Interpreter()
    assert i.__class__ == bw2parameters.DefaultInterpreter


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


# repeat tests in tests/parameters.py with PintInterpreter and PintParameterSet
if bw2parameters.PintWrapper.pint_installed:
    config.use_pint_parameters = True
    from .parameters import *  # noqa
