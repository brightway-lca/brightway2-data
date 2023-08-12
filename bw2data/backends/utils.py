import copy
import warnings

import numpy as np

from ..errors import InvalidExchange, UntypedExchange
from ..meta import methods
from ..method import Method
from .schema import get_id


def get_csv_data_dict(ds):
    fields = {"name", "reference product", "unit", "location"}
    dd = {field: ds.get(field) for field in fields}
    dd["id"] = get_id(ds)
    return dd


def check_exchange_amount(exc):
    """Check exchange data validity when processing"""
    if "amount" not in exc:
        raise InvalidExchange
    if np.isnan(exc["amount"]) or np.isinf(exc["amount"]):
        raise ValueError("Invalid amount in exchange {}".format(exc))


def dict_as_activitydataset(ds):
    ds = copy.copy(ds)
    return {
        "data": ds,
        "database": ds.pop("database"),
        "code": ds.pop("code"),
        "location": ds.pop("location", None),
        "name": ds.pop("name", None),
        "product": ds.pop("reference product", None),
        "type": ds.pop("type", "process"),
    }


def dict_as_exchangedataset(ds):
    ds = copy.copy(ds)

    input_ = ds.pop("input")
    output_ = ds.pop("output")

    return {
        "data": ds,
        "input_database": input_[0],
        "input_code": input_[1],
        "output_database": output_[0],
        "output_code": output_[1],
        "type": ds.pop("type"),
    }


def replace_cfs(old_key, new_key):
    """Replace ``old_key`` with ``new_key`` in characterization factors.

    Returns list of modified methods."""
    altered_methods = []
    for name in methods:
        changed = False
        data = Method(name).load()
        for line in data:
            if line[0] == old_key:
                line[0], changed = new_key, True
        if changed:
            Method(name).write(data)
            altered_methods.append(name)
    return altered_methods


def retupleize_geo_strings(value):
    """Transform data from SQLite representation to Python objects.

    We are using a SQLite3 cursor, which means that the Peewee data conversion code is not called. So ``('foo', 'bar')`` is stored as a string, not a tuple. This code tries to do this conversion correctly.

    TODO: Adapt what Peewee does in this case?"""
    if not value:
        return value
    elif "(" not in value:
        return value
    try:
        # Is this a dirty, dirty hack, or inspiration?
        # Location is retrieved as a string from the database
        # The alternative is to retrieve and process the
        # entire activity dataset...
        return eval(value)
    except NameError:
        # Not everything with a parentheses is a tuple.
        return value
