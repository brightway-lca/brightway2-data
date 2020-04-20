# -*- coding: utf-8 -*-
from ...meta import methods
from ...method import Method


def dict_as_activitydataset(ds):
    return {
        "data": ds,
        "database": ds["database"],
        "code": ds["code"],
        "location": ds.get("location"),
        "name": ds.get("name"),
        "product": ds.get("reference product"),
        "type": ds.get("type", "process"),
    }


def dict_as_exchangedataset(ds):
    return {
        "data": ds,
        "input_database": ds["input"][0],
        "input_code": ds["input"][1],
        "output_database": ds["output"][0],
        "output_code": ds["output"][1],
        "type": ds["type"],
    }


# def replace_exchanges(old_key, new_key):
#     """Replace ``old_key`` with ``new_key`` in input field of exchanges.

#     Returns number of modified exchanges."""
#     from .proxies import Exchanges

#     # reverse means search by input field, not output field of exchange
#     for index, exc in enumerate(Exchanges(old_key, reverse=True)):
#         exc["input"] = new_key
#         exc.save()
#     else:
#         return 0
#     return index + 1


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
