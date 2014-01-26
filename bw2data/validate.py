# encoding: utf-8
from voluptuous import Schema, Required, Invalid, Any, All, Length


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
    "uncertainty_type": int,
    "loc": Any(float, int),
    "scale": Any(float, int),
    "shape": Any(float, int),
    "minimum": Any(float, int),
    "maximum": Any(float, int)
}

maybe_uncertainty = Any(float, int, uncertainty_dict)

exchange = {
        Required("input"): valid_tuple,
        Required("type"): basestring,
        }
exchange.update(**uncertainty_dict)

db_validator = Schema({valid_tuple: {
    "categories": Any(list, tuple),
    "location": object,
    "unit": basestring,
    Required("name"): basestring,
    Required("type"): basestring,
    Required("exchanges"): [exchange]
    }},
    extra=True)

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
