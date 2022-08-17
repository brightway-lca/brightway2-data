from bw2data import Method, Database, databases, methods, projects, get_activity
import pytest
from bw2data.tests import bw2test
from bw2calc import LCA
import numpy as np


@pytest.fixture
@bw2test
def iotable_fixture():
    """Technosphere matrix:

        a   b   c
    a   2   0   -3
    b   -1  1   0
    c   4   0.2 -1

    Biosphere matrix:

        a   b   c
    d   0   1   2

    Characterization matrix:

        d
    d   42

    """
    tech_exchanges = [
        ("a", "a", 2, False),
        ("a", "c", 3, True),
        ("b", "a", 1, True),
        ("b", "b", 1, False),
        ("c", "a", 4, False),
        ("c", "b", 0.2, True),
        ("c", "c", 1, True),
    ]
    bio_exchanges = [
        ("d", "b", 1, False),
        ("d", "c", 2, False),
    ]

    mouse = Database("mouse")
    mouse.write({("mouse", "d"): {"name": "squeak", "type": "emission"}})

    cat = Database("cat", backend="iotable")
    cat_data = {
        ("cat", "a"): {"name": "a", "unit": "meow", "location": "sunshine"},
        ("cat", "b"): {"name": "b", "unit": "purr", "location": "curled up"},
        ("cat", "c"): {"name": "c", "unit": "meow", "location": "on lap"},
    }
    cat.write(cat_data)
    cat.write_exchanges(
        technosphere=[
            {
                "row": get_activity(code=m).id,
                "col": get_activity(code=n).id,
                "amount": o,
                "flip": p,
            }
            for m, n, o, p in tech_exchanges
        ],
        biosphere=[
            {
                "row": get_activity(code=m).id,
                "col": get_activity(code=n).id,
                "amount": o,
                "flip": p,
            }
            for m, n, o, p in bio_exchanges
        ],
        dependents=["mouse"],
    )

    cfs = [(("mouse", "d"), 42)]
    method = Method(("a method",))
    method.register()
    method.write(cfs)


def test_setup_clean(iotable_fixture):
    print(databases)
    assert len(databases) == 2
    assert list(methods) == [("a method",)]
    assert len(projects) == 2  # Default project
    assert "default" in projects


# def test_production(activity_and_method):
#     activity, method = activity_and_method
#     assert len(list(activity.production())) == 1
#     assert len(activity.production()) == 1
#     exc = list(activity.production())[0]
#     assert exc["amount"] == 2


# def test_substitution(activity_and_method):
#     activity, method = activity_and_method
#     assert len(activity.substitution()) == 0


# def test_biosphere(activity_and_method):
#     activity, method = activity_and_method
#     for exc in activity.exchanges():
#         print(exc)
#     assert len(list(activity.biosphere())) == 1
#     assert len(activity.biosphere()) == 1
#     exc = list(activity.biosphere())[0]
#     assert exc["amount"] == 4


# def test_technosphere(activity_and_method):
#     activity, method = activity_and_method
#     assert len(list(activity.technosphere())) == 1
#     assert len(activity.technosphere()) == 1
#     exc = list(activity.technosphere())[0]
#     assert exc["amount"] == 3

# def test_lca(activity_and_method):
#     _, method = activity_and_method
#     lca = LCA({("db","d"):1}, method.name)
#     lca.lci()
#     lca.lcia()
#     lca.score

# def test_readonlyexchange(activity_and_method):
#     # test if all properties and methods implemented in class ReadOnlyExchange are working correctly

#     activity, method = activity_and_method
#     exc = list(activity.technosphere())[0]
#     # __str__
#     assert str(exc) == "Exchange: 3.0 None 'd' (None, None, None) to 'a' (None, None, None)>"
#     # __contains__
#     assert "output" in exc
#     # __eq__
#     assert exc == exc
#     # __len__
#     assert len(exc) == 4
#     # __hash__
#     assert exc.__hash__() is not None
#     # output getter
#     assert exc.output == activity
#     # input getter
#     assert exc.input == get_activity(("db","d"))
#     # amount getter
#     assert exc.amount == 3
#     # unit getter
#     assert exc.unit == None
#     # type getter
#     assert exc['type'] == "technosphere"
#     # as_dict
#     assert exc.as_dict() == {'input': ('db', 'd'), 'output': ('db', 'a'), 'amount': 3.0, 'type': 'technosphere'}
#     # valid
#     assert exc.valid()
#     # lca
#     exc.lca(method.name, 1)

#     # test if exchange is read-only
#     with pytest.raises(AttributeError, match="can't set attribute"):
#         exc.input = exc.output
#         exc.output = exc.input
#         exc.amount = 1
#     with pytest.raises(AttributeError, match="'ReadOnlyExchange' object has no attribute .*"):
#         exc.delete()
#         exc.save()
#     with pytest.raises(TypeError, match="'ReadOnlyExchange' object does not support item assignment"):
#         exc['type'] = 'biosphere'
