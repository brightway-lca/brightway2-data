# encoding: utf-8
from __future__ import unicode_literals
from eight import *
from voluptuous import Schema, Required, Invalid, Any, All, Length, Optional


def valid_tuple(obj):
    try:
        assert isinstance(obj, tuple)
        assert isinstance(obj[0], str)
        assert isinstance(obj[1], (str, int, tuple, list))
    except:
        raise Invalid("{} is not a valid key tuple".format(obj))
    return obj


uncertainty_dict = {
    Required("amount"): Any(float, int),
    Optional("uncertainty type"): int,
    Optional("loc"): Any(float, int),
    Optional("scale"): Any(float, int),
    Optional("shape"): Any(float, int),
    Optional("minimum"): Any(float, int),
    Optional("maximum"): Any(float, int)
}

exchange = {
    Required("input"): valid_tuple,
    Required("type"): str,
}
exchange.update(uncertainty_dict)

lci_dataset = {
    Optional("categories"): Any(list, tuple),
    Optional("location"): object,
    Optional("unit"): str,
    Optional("name"): str,
    Optional("type"): str,
    Optional("exchanges"): [exchange]
}

db_validator = Schema({valid_tuple: lci_dataset}, extra=True)

# TODO: elements in a list don't maintain order
# See https://github.com/alecthomas/voluptuous/issues/59
# Each list needs to be a separate function...

maybe_uncertainty = Any(float, int, uncertainty_dict)

ia_validator = Schema([Any(
    [valid_tuple, maybe_uncertainty],         # site-generic
    [valid_tuple, maybe_uncertainty, object]  # regionalized
)])

weighting_validator = Schema(All(
    [uncertainty_dict],
    Length(min=1, max=1)
))

normalization_validator = Schema([
    [valid_tuple, maybe_uncertainty]
])
