import pytest

from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    ProjectParameter,
    Interpreter,
    ParameterManager,
    Group
)
from bw2data import config, Database, projects
import shutil

bw2parameters = pytest.importorskip("bw2parameters", "1.0.0")

# repeat tests in tests/parameters.py with PintInterpreter and PintParameterSet
if bw2parameters.PintWrapper.pint_installed:
    config.use_pint_parameters = True
    from .parameters import *  # noqa


@pytest.fixture(scope="module")
def use_pint():
    if not bw2parameters.PintWrapper.pint_installed:
        pytest.skip("Pint not installed.")
    config.use_pint_parameters = True
    config.dont_warn = True
    config.is_test = True
    config.cache = {}
    tempdir = projects._use_temp_directory()
    db = Database("some_db")
    db.register()
    a = db.new_activity(code="some_code", name="some activity")
    a.save()
    yield db, a

    def close_all_databases():
        for path, db in config.sqlite3_databases:
            db.db.autoconnect = False
            db.db.close()

    close_all_databases()
    shutil.rmtree(tempdir)


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
    config.use_pint_parameters = True


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


def test_project_parameter_recalculate(use_pint):
    ProjectParameter.create(
        formula="1 kg + 200 g",
        name="p_proj",
        amount=3.14,
    )
    ProjectParameter.recalculate()
    obj = ProjectParameter.get(name="p_proj")
    assert obj.amount == 1.2
    assert obj.data["unit"] == "kilogram"


def test_database_parameter_recalculate(use_pint):
    DatabaseParameter.create(
        database="some_db",
        name="p_db",
        amount=3.14,
        formula="2 * p_proj * m/kg"
    )
    DatabaseParameter.recalculate("some_db")
    obj = DatabaseParameter.get(name="p_db")
    assert obj.amount == 2.4
    assert obj.data["unit"] == "meter"


def test_activity_parameter_recalculate(use_pint):
    ActivityParameter.create(
        database="some_db",
        group="some_group",
        code="some_code",
        name="p_act",
        amount=3.14,
        formula="2 * p_db * V/m"
    )
    ActivityParameter.recalculate("some_group")
    obj = ActivityParameter.get(name="p_act")
    assert obj.amount == 4.8
    assert obj.data["unit"] == "volt"


def test_parameterized_exchange_recalculate(use_pint):
    _, act = use_pint
    act.new_exchange(
        amount=3.14, input=act, type="production", formula="2 * p_act * s/V"
    ).save()
    ParameterManager.add_exchanges_to_group("some_group", act)
    ActivityParameter.recalculate_exchanges("some_group")
    obj = next(iter(act.exchanges()))
    assert obj["amount"] == 9.6
    assert obj["unit"] == "second"


def test_mix_parameters_with_and_without_units(use_pint):
    ProjectParameter.create(
        name="p_proj2",
        formula="1 + 0.2",
    )
    ProjectParameter.recalculate()
    obj = ProjectParameter.get(name="p_proj2")
    assert obj.amount == 1.2
    assert "unit" not in obj.dict.get("data", {})
    # assert multiplication works
    DatabaseParameter.create(
        database="some_db",
        name="p_db2",
        amount=3.14,
        formula="2 * p_proj2 kg"
    )
    DatabaseParameter.recalculate("some_db")
    obj = DatabaseParameter.get(name="p_db2")
    assert obj.amount == 2.4
    assert obj.data["unit"] == "kilogram"
    # assert addition fails
    obj.formula = "2 kg + p_proj2"
    obj.save()
    with pytest.raises(bw2parameters.PintWrapper.DimensionalityError):
        DatabaseParameter.recalculate("some_db")


def test_mix_parameters_optional_units(use_pint):
    # delete existing project parameters because some of them contain units
    ProjectParameter.delete().execute()
    # define parameters
    ProjectParameter.create(
        name="p_proj3",
        amount=1,
        data={"unit": "kilogram"},
    )
    ProjectParameter.create(
        name="p_proj4",
        amount=200,
        data={"unit": "gram"},
    )
    ProjectParameter.create(
        name="p_proj5",
        formula="p_proj3 + p_proj4",
    )
    # solve with pint
    config.use_pint_parameters = True
    ProjectParameter.recalculate()
    obj = ProjectParameter.get(name="p_proj5")
    assert obj.amount == 1.2
    assert obj.dict["unit"] == "kilogram"
    # solve without pint
    config.use_pint_parameters = False
    group = Group.get(name="project")
    group.fresh = False
    group.save()
    ProjectParameter.recalculate()
    obj = ProjectParameter.get(name="p_proj5")
    assert obj.amount == 201
    assert obj.dict["unit"] == "kilogram"
    config.use_pint_parameters = True


def test_error_if_recalculated_without_pint(use_pint):
    config.use_pint_parameters = False
    with pytest.raises(bw2parameters.MissingName):
        ActivityParameter.recalculate_exchanges("some_group")
    config.use_pint_parameters = True

