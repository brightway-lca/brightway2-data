import datetime
import decimal
import math
import os
import pickle
import uuid
import warnings
import zoneinfo
from collections import OrderedDict
from unittest.mock import patch

import numpy as np
import pytest

from bw2data import codec, config
from bw2data.codec import UnknownTaggedValue, dumps, loads, safe_pickle_loads
from bw2data.errors import UnsafePickleError


def same(a, b) -> bool:
    """Equal, with identical types all the way down (tuple != list, int != float, ...)."""
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return list(a) == list(b) and all(same(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, np.ndarray):
        if a.dtype != b.dtype or a.shape != b.shape:
            return False
        if a.dtype.kind == "O":
            return all(same(x, y) for x, y in zip(a.ravel(), b.ravel()))
        return np.array_equal(a, b, equal_nan=a.dtype.kind in "fcmM")
    if isinstance(a, float) and math.isnan(a):
        return math.isnan(b)
    if isinstance(a, decimal.Decimal) and a.is_nan():
        return b.is_nan()
    if isinstance(a, complex):
        return same(a.real, b.real) and same(a.imag, b.imag)
    if isinstance(a, float) and a == 0:
        return math.copysign(1, a) == math.copysign(1, b)
    if isinstance(a, (datetime.datetime, datetime.time)):
        return a == b and a.tzinfo == b.tzinfo and a.fold == b.fold
    return a == b


def roundtrip(value):
    return loads(dumps(value))


@pytest.fixture
def clean_registry():
    """Undo registrations and entry point loading done by a test."""
    encoders, decoders = dict(codec._ENCODERS), dict(codec._DECODERS)
    loaded = codec._entry_points_loaded
    yield
    codec._ENCODERS.clear()
    codec._ENCODERS.update(encoders)
    codec._DECODERS.clear()
    codec._DECODERS.update(decoders)
    codec._entry_points_loaded = loaded


VALUES = [
    None,
    True,
    0,
    -(2**63),
    2**64 - 1,
    2**64,
    -(2**63) - 1,
    10**40,
    1.5,
    -0.0,
    1e-320,
    math.nan,
    math.inf,
    -math.inf,
    "",
    "Zürich ⚡",
    "__bw__",
    '"__bw__"',
    "\ud800 lone surrogate",
    [],
    {},
    [1, [2, [3]]],
    (1, (2, [3, (4,)])),
    (),
    {"a": (1, 2)},
    {1: "a", (2, 3): "b", None: 1.5, 2.5: "c"},
    {"__bw__": "datetime", "v": "not a date"},
    {"x": {"__bw__": "tuple", "v": [1]}},
    {"\udcff": 1},
    {1, 2, 3},
    frozenset({"a", "b"}),
    {1, "a", (2, 3)},
    OrderedDict([("b", 1), ("a", 2)]),
    datetime.datetime(2026, 11, 22, 7, 20, 5, 123456),
    datetime.datetime(2026, 11, 22, 7, 20, tzinfo=datetime.timezone.utc),
    datetime.datetime(2026, 11, 22, 7, 20, tzinfo=datetime.timezone(datetime.timedelta(hours=-3))),
    datetime.datetime(2026, 10, 25, 2, 30, fold=1, tzinfo=zoneinfo.ZoneInfo("Europe/Zurich")),
    datetime.date(2026, 11, 22),
    datetime.time(7, 20, 5, 1),
    datetime.time(7, 20, tzinfo=datetime.timezone.utc),
    datetime.timedelta(days=-1, seconds=5, microseconds=7),
    decimal.Decimal("0.10"),
    decimal.Decimal("NaN"),
    uuid.UUID("12345678-1234-5678-1234-567812345678"),
    b"\x00\xff",
    bytearray(b"abc"),
    complex(1, -2),
    complex(math.nan, math.inf),
    np.array([[1.5, 2.5], [3.5, math.nan]]),
    np.array([0, 5], dtype="timedelta64[Y]"),
    np.array(["2026-01-01", "NaT"], dtype="datetime64[D]"),
    np.array([1, 2, 3], dtype=np.int32),
    np.array(["a", "bc"]),
    np.array([], dtype=np.float64),
    np.array([(1, 2), None], dtype=object),
    np.datetime64("2026-11-22T07:20", "m"),
    np.timedelta64(3, "Y"),
]


@pytest.mark.parametrize("value", VALUES, ids=repr)
def test_roundtrip_keeps_value_and_type(value):
    assert same(value, roundtrip(value))


def test_roundtrip_nested():
    value = {"nested": VALUES}
    assert same(value, roundtrip(value))


def test_exchange_like_data_roundtrips():
    exchange = {
        "input": ("ecoinvent-3.13-biosphere", "38a622c6-f086-4763-a952-7c6b3b1c42ba"),
        "output": ("ecoinvent-3.13-cutoff", "0a1b2c3d"),
        "amount": 0.00123,
        "type": "biosphere",
        "uncertainty type": 2,
        "loc": math.log(0.00123),
        "scale": 0.1,
        "pedigree": {"reliability": 2, "completeness": 3},
        "properties": {"carbon content": {"amount": 0.5, "unit": "dimensionless"}},
        "categories": ("air", "urban air close to ground"),
        "classifications": [("CPC", "171: Electrical energy")],
    }
    assert same(exchange, roundtrip(exchange))


def test_plain_json_is_stored_as_plain_json():
    assert dumps({"a": 1, "b": [1.5, None, True, "x"]}) == b'{"a":1,"b":[1.5,null,true,"x"]}'


def test_tagged_value_format():
    assert dumps((1, 2)) == b'{"__bw__":"tuple","v":[1,2]}'
    assert dumps({1: "a"}) == b'{"__bw__":"dict","v":[[1,"a"]]}'


def test_plain_json_written_elsewhere_can_be_read():
    assert loads('{"a": [1, 2]}') == {"a": [1, 2]}
    assert loads(b"[1, 2]") == [1, 2]


def test_loads_accepts_str_bytes_and_memoryview():
    data = dumps({"a": (1,)})
    assert loads(data) == loads(data.decode()) == loads(memoryview(data)) == {"a": (1,)}


def test_equal_sets_are_stored_identically():
    assert dumps({3, 1, 2}) == dumps({2, 3, 1})


def test_numpy_scalars_are_stored_as_python_numbers():
    value = {
        "loc": np.float64(-6.7),
        "n": np.int64(3),
        "f32": np.float32(0.5),
        "ok": np.bool_(True),
        "s": np.str_("x"),
    }
    result = roundtrip(value)
    assert same(result, {"loc": -6.7, "n": 3, "f32": 0.5, "ok": True, "s": "x"})
    assert dumps(np.float64(1.5)) == b"1.5"


class MyDict(dict):
    pass


class Thing:
    pass


@pytest.mark.parametrize("value", [Thing(), MyDict(), print, {1.5j: Thing}, os.environ], ids=repr)
def test_unknown_types_raise(value):
    with pytest.raises(TypeError, match="Can't store"):
        dumps(value)


def test_error_names_where_the_value_is():
    with pytest.raises(TypeError, match=r"value\['a'\]\[1\]"):
        dumps({"a": [1, Thing()]})


class FakeTZ(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=1)

    def dst(self, dt):
        return datetime.timedelta(0)


def test_datetime_with_other_time_zone_class_raises():
    with pytest.raises(TypeError, match="time zone"):
        dumps({"t": datetime.datetime(2026, 1, 1, tzinfo=FakeTZ())})


def test_structured_numpy_array_raises():
    with pytest.raises(TypeError, match="structured"):
        dumps(np.zeros(2, dtype=[("a", "i4"), ("b", "f8")]))


class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __eq__(self, other):
        return isinstance(other, Point) and (self.x, self.y) == (other.x, other.y)


def test_register_custom_type(clean_registry):
    codec.register(Point, "tests.Point", lambda p: (p.x, p.y), lambda v: Point(*v))
    value = {"p": Point(1, (2, 3))}
    assert dumps(value) == (
        b'{"p":{"__bw__":"tests.Point",'
        b'"v":{"__bw__":"tuple","v":[1,{"__bw__":"tuple","v":[2,3]}]}}}'
    )
    assert roundtrip(value) == value
    assert roundtrip(value)["p"].y == (2, 3)


def test_register_refuses_builtin_tags_and_conflicts(clean_registry):
    with pytest.raises(ValueError, match="reserved"):
        codec.register(Point, "tuple", list, tuple)
    with pytest.raises(ValueError, match="reserved"):
        codec.register(Point, "float", list, tuple)
    codec.register(Point, "tests.Point", list, tuple)
    codec.register(Point, "tests.Point", list, tuple)  # same again is fine
    with pytest.raises(ValueError, match="already registered"):
        codec.register(Thing, "tests.Point", list, tuple)
    with pytest.raises(ValueError, match="already registered"):
        codec.register(Point, "tests.Other", list, tuple)


def test_unknown_tag_is_kept_and_stored_again_unchanged(clean_registry):
    codec._entry_points_loaded = True
    data = b'{"a":{"__bw__":"some_plugin.Thing","v":{"x":[1,{"__bw__":"tuple","v":[2]}]}}}'
    value = loads(data)
    assert type(value["a"]) is UnknownTaggedValue
    assert value["a"]["v"] == {"x": [1, (2,)]}
    assert dumps(value) == data


def test_failing_plugin_decoder_warns_and_keeps_data(clean_registry):
    def broken(payload):
        raise RuntimeError("old format")

    codec.register(Point, "tests.Point", lambda p: [p.x, p.y], broken)
    data = dumps(Point(1, 2))
    with pytest.warns(UserWarning, match="Can't restore a value tagged 'tests.Point'"):
        value = loads(data)
    assert type(value) is UnknownTaggedValue
    assert dumps(value) == data


def test_corrupt_builtin_tag_raises():
    with pytest.raises(Exception):
        loads(b'{"__bw__":"datetime","v":"not a date"}')


class FakeEntryPoint:
    value = "tests.fake:register"

    def __init__(self, func):
        self.func = func

    def load(self):
        return self.func


def test_entry_points_are_loaded_for_unknown_types(clean_registry):
    codec._entry_points_loaded = False

    def plugin():
        codec.register(Point, "tests.Point", lambda p: [p.x, p.y], lambda v: Point(*v))

    with patch.object(codec, "entry_points", return_value=[FakeEntryPoint(plugin)]) as eps:
        assert roundtrip(Point(1, 2)) == Point(1, 2)
        dumps(Point(3, 4))
    eps.assert_called_once_with(group="bw2data.codec")


def test_entry_points_are_loaded_for_unknown_tags(clean_registry):
    codec._entry_points_loaded = False

    def plugin():
        codec.register(Point, "tests.Point", lambda p: [p.x, p.y], lambda v: Point(*v))

    with patch.object(codec, "entry_points", return_value=[FakeEntryPoint(plugin)]):
        assert loads(b'{"__bw__":"tests.Point","v":[1,2]}') == Point(1, 2)


def test_broken_entry_point_warns(clean_registry):
    codec._entry_points_loaded = False

    def plugin():
        raise ImportError("missing dependency")

    with patch.object(codec, "entry_points", return_value=[FakeEntryPoint(plugin)]):
        with pytest.warns(UserWarning, match="missing dependency"):
            with pytest.raises(TypeError):
                dumps(Thing())


# Restricted unpickler


@pytest.mark.parametrize(
    "value",
    [
        {"name": "x", "categories": ("air",), "amount": 1.5, "n": None},
        [1, {2, 3}, frozenset([4]), complex(1, 2), bytearray(b"a"), b"\x00"],
        OrderedDict(a=1),
        datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        datetime.date(2026, 1, 1),
        datetime.time(1, 2),
        datetime.timedelta(seconds=5),
        decimal.Decimal("1.10"),
        uuid.uuid4(),
        np.float64(-6.7),
        np.int32(3),
        np.array([[1, 2], [3, 4]]),
        np.array([0, 5], dtype="timedelta64[Y]"),
        np.array(["a"], dtype=object),
    ],
    ids=repr,
)
@pytest.mark.parametrize("protocol", [2, 4, 5])
def test_safe_pickle_loads_reads_stored_types(value, protocol):
    result = safe_pickle_loads(pickle.dumps(value, protocol=protocol))
    assert same(value, result)


def test_safe_pickle_loads_reads_numpy_1_pickles_without_warning():
    # numpy 1.x wrote ``numpy.core.multiarray``, a deprecated alias in numpy 2
    data = pickle.dumps(np.float64(1.5), protocol=2)
    old = data.replace(b"numpy._core.multiarray\n", b"numpy.core.multiarray\n")
    assert b"numpy.core.multiarray" in old
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert safe_pickle_loads(old) == 1.5


class Evil:
    def __reduce__(self):
        return (os.system, ("echo pwned",))


def test_safe_pickle_loads_refuses_code():
    with pytest.raises(UnsafePickleError, match="system"):
        safe_pickle_loads(pickle.dumps(Evil()))


def test_safe_pickle_loads_refuses_getattr():
    # ``getattr`` on an allowed class could reach anything, e.g. ``type.__subclasses__``
    with pytest.raises(UnsafePickleError, match="getattr"):
        safe_pickle_loads(pickle.dumps(zoneinfo.ZoneInfo("Europe/Zurich"), protocol=4))


def test_safe_pickle_loads_maps_python_2_names_before_checking():
    # ``cos\nsystem`` would be refused as is; ``__builtin__.set`` is allowed as ``builtins.set``
    assert safe_pickle_loads(b"c__builtin__\nset\n(]tR.") == set()
    with pytest.raises(UnsafePickleError):
        safe_pickle_loads(b"cos\nsystem\n(S'echo pwned'\ntR.")


def test_safe_pickle_loads_refuses_unknown_classes():
    data = pickle.dumps({"p": Point(1, 2)})
    with pytest.raises(UnsafePickleError, match=r"test_codec\.Point"):
        safe_pickle_loads(data)
    # It's still an ``UnpicklingError`` for code that catches those
    with pytest.raises(pickle.UnpicklingError):
        safe_pickle_loads(data)


def test_trusted_pickle_classes_allow_a_class():
    data = pickle.dumps({"p": Point(1, 2)})
    name = f"{Point.__module__}.Point"
    with patch.object(config, "trusted_pickle_classes", [name]):
        assert safe_pickle_loads(data) == {"p": Point(1, 2)}
    with pytest.raises(UnsafePickleError):
        safe_pickle_loads(data)


def test_safe_pickle_loads_accepts_memoryview():
    assert safe_pickle_loads(memoryview(pickle.dumps((1, 2)))) == (1, 2)


def test_pickled_data_converts_to_json():
    # What the storage migration will do with every stored row
    original = {
        "loc": np.float64(-6.7),
        "categories": ("air",),
        "x": {1: datetime.date(2026, 1, 1)},
    }
    value = safe_pickle_loads(pickle.dumps(original, protocol=4))
    assert same(
        roundtrip(value), {"loc": -6.7, "categories": ("air",), "x": {1: datetime.date(2026, 1, 1)}}
    )
