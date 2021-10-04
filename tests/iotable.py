from bw2data import Method, Database, databases, methods, projects, get_activity
import pytest
from bw2data.tests import bw2test
from bw_processing.constants import INDICES_DTYPE
from bw2calc import LCA
import numpy as np


@pytest.fixture
@bw2test
def activity_and_method():

    def write_dummy_technosphere(name):

        db = Database(name, backend="iotable")

        # define activity metadata
        technosphere_data = {
            (name, "a"): {"name": "a"},
            (name, "b"): {"name": "b"},
            (name, "d"): {"name": "d"},
        }
        db.write(technosphere_data)

        # define exchanges
        exchanges = {
            "input": [(name, "a"), (name, "b"), (name, "a")],
            "output": [(name, "a"), (name, "a"), (name, "d")],
            "amount": [2, 3, 5],
        }
        return dict(
            indices_array=np.array(
                [
                    (get_activity(i).id, get_activity(o).id)
                    for i, o in zip(exchanges["input"], exchanges["output"])
                ],
                dtype=INDICES_DTYPE,
            ),
            data_array=np.array(exchanges["amount"]),
            flip_array=np.zeros((len(exchanges), 1)),
        )

    def write_dummy_biosphere(name):

        db = Database(name, backend="iotable")

        # define activity metadata
        data = {
            (name, "c"): {"name": "c"},
        }
        db.write(data)

        # define exchanges
        exchanges = {
            "input": [(name, "c"), (name, "c")],
            "output": [("db", "a"), ("db", "b")],
            "amount": [4,1],
        }
        return dict(
            indices_array=np.array(
                [
                    (get_activity(i).id, get_activity(o).id)
                    for i, o in zip(exchanges["input"], exchanges["output"])
                ],
                dtype=INDICES_DTYPE,
            ),
            data_array=np.array(exchanges["amount"]),
            flip_array=np.zeros((len(exchanges), 1)),
        )

    # create dummy technosphere and biosphere
    techno_name = "db"
    technosphere = write_dummy_technosphere(techno_name)
    bio_name = "db_bio"
    biosphere = write_dummy_biosphere(bio_name)
    dependents = [techno_name]
    Database(techno_name).write_exchanges(technosphere, biosphere, dependents)

    # create dummy method
    cfs = [((bio_name, "c"), 42)]
    method = Method(("a method",))
    method.register()
    method.write(cfs)

    return Database(techno_name).get("a"), method


def test_setup_clean(activity_and_method):
    assert len(databases) == 1
    assert list(methods) == [("a method",)]
    assert len(projects) == 2  # Default project
    assert "default" in projects


def test_production(activity_and_method):
    activity, method = activity_and_method
    assert len(list(activity.production())) == 1
    assert len(activity.production()) == 1
    exc = list(activity.production())[0]
    assert exc["amount"] == 2


def test_substitution(activity_and_method):
    activity, method = activity_and_method
    assert len(activity.substitution()) == 0


def test_biosphere(activity_and_method):
    activity, method = activity_and_method
    assert len(list(activity.biosphere())) == 1
    assert len(activity.biosphere()) == 1
    exc = list(activity.biosphere())[0]
    assert exc["amount"] == 4


def test_technosphere(activity_and_method):
    activity, method = activity_and_method
    assert len(list(activity.technosphere())) == 1
    assert len(activity.technosphere()) == 1
    exc = list(activity.technosphere())[0]
    assert exc["amount"] == 3

def test_lca(activity_and_method):
    _, method = activity_and_method
    lca = LCA({("db","d"):1}, method.name)
    lca.lci()
    lca.lcia()
    lca.score

def test_readonlyexchange(activity_and_method):
    # test if all properties and methods implemented in class ReadOnlyExchange are working correctly

    activity, method = activity_and_method
    exc = list(activity.technosphere())[0]
    # __str__
    assert str(exc) == "Exchange: 3.0 None 'd' (None, None, None) to 'a' (None, None, None)>"
    # __contains__
    assert "output" in exc
    # __eq__
    assert exc == exc
    # __len__
    assert len(exc) == 4
    # __hash__
    assert exc.__hash__() is not None
    # output getter
    assert exc.output == activity
    # input getter
    assert exc.input == get_activity(("db","d"))
    # amount getter
    assert exc.amount == 3
    # unit getter
    assert exc.unit == None
    # type getter
    assert exc['type'] == "technosphere"
    # as_dict
    assert exc.as_dict() == {'input': ('db', 'd'), 'output': ('db', 'a'), 'amount': 3.0, 'type': 'technosphere'}
    # valid
    assert exc.valid()
    # lca
    exc.lca(method.name, 1)

    # test if exchange is read-only
    with pytest.raises(AttributeError, match="can't set attribute"):
        exc.input = exc.output
        exc.output = exc.input
        exc.amount = 1
    with pytest.raises(AttributeError, match="'ReadOnlyExchange' object has no attribute .*"):
        exc.delete()
        exc.save()
    with pytest.raises(TypeError, match="'ReadOnlyExchange' object does not support item assignment"):
        exc['type'] = 'biosphere'





