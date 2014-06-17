# encoding: utf-8
from voluptuous import Schema, Required, Invalid, Any, All, Length, Optional


def valid_tuple(obj):
    try:
        assert isinstance(obj, tuple)
        assert isinstance(obj[0], basestring)
        assert isinstance(obj[1], (basestring, int, tuple, list))
    except:
        raise Invalid(u"%s is not a valid key tuple" % unicode(obj))
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
        Required("type"): basestring,
        }
exchange.update(**uncertainty_dict)

lci_dataset = {
    Optional("categories"): Any(list, tuple),
    Optional("location"): object,
    Optional("unit"): basestring,
    Optional("name"): basestring,
    Optional("type"): basestring,
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

bw2package_validator = Schema({
    Required('metadata'): {basestring: object},
    Required('name'): Any(basestring, tuple, list),
    'class': {
        Required('module'): basestring,
        Required('name'): basestring,
        "unrolled dict": bool,
    },
    Optional('unrolled_dict'): bool,
    Required('data'): object
})
