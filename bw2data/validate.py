# encoding: utf-8
try:
    from voluptuous import Schema, Required, Invalid, Any
except:
    raise ImportError("The voluptuous package is Required for validation")


def valid_tuple(o):
    try:
        assert isinstance(o, tuple)
        assert isinstance(o[0], basestring)
        assert isinstance(o[1], (basestring, int, tuple, list))
    except:
        raise Invalid("The key %s is invalid" % o)
    return o

db_validator = Schema({valid_tuple: {
    "code": object,
    "categories": list or tuple,
    "location": object,
    Required("name"): basestring,
    Required("type"): basestring,
    Required("unit"): basestring,
    Required("exchanges"): [{
        Required("amount"): float,
        Required("input"): valid_tuple,
        "comment": basestring,
        "code": object,
        "sigma": float,
        Required("uncertainty type"): int,
        Required("type"): basestring,
        }]
    }},
    extra=True)

ia_validator = Schema([[valid_tuple, float, object]])

weighting_validator = Schema({
    Required("amount"): Any(float, int),
    "uncertainty_type": int,
    "loc": Any(float, int),
    "scale": Any(float, int),
    "shape": Any(float, int),
    "minimum": Any(float, int),
    "maximum": Any(float, int)
})

normalization_validator = Schema([{
    Required("amount"): Any(float, int),
    Required("flow"): valid_tuple,
    "uncertainty_type": int,
    "loc": Any(float, int),
    "scale": Any(float, int),
    "shape": Any(float, int),
    "minimum": Any(float, int),
    "maximum": Any(float, int)
}])
