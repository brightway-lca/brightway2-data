"""Tagged JSON for data stored by bw2data, and a restricted unpickler for data stored before.

JSON has no tuples, sets, datetimes, non-string keys, NaN or integers beyond 64 bits, and
``json.dumps`` and ``orjson.dumps`` convert some of these without saying so (a tuple becomes
a list, ``{1: "a"}`` becomes ``{"1": "a"}``). ``dumps`` walks the whole value first and
replaces every value that JSON can't hold exactly with a *tagged* value::

    {"__bw__": "tuple", "v": ["ecoinvent-3.13-biosphere", "a1b2c3"]}
    {"__bw__": "datetime", "v": "2026-11-22T07:20:00+00:00"}
    {"__bw__": "dict", "v": [[1, "a"], [[2, 3], "b"]]}

``loads`` turns tagged values back into the original objects, with the same types. A dict
with non-string keys, or a dict that happens to have a ``"__bw__"`` key itself, is stored
as a ``"dict"`` tagged value, so a stored value can never be mistaken for a tag.

* A type that can't be stored raises ``TypeError`` naming where it was found. Values are
  never converted to something else silently. numpy scalars are the one exception: they
  are stored as the Python number they hold (``numpy.float64(1.5)`` becomes ``1.5``).
* Other packages add their own types with ``register``, or through an entry point in the
  ``bw2data.codec`` group whose object is a callable that calls ``register``. Entry points
  are loaded the first time an unknown type or tag is met.
* A tag that no installed package knows is kept as an ``UnknownTaggedValue``, and stored
  again unchanged, so no data is lost when a plugin is missing.

``safe_pickle_loads`` reads pickles written by earlier bw2data versions. It only loads the
classes those pickles are expected to contain (see ``SAFE_PICKLE_GLOBALS``), so a file from
someone else can't run code when it is read.
"""

import _compat_pickle
import base64
import datetime
import decimal
import io
import math
import pickle
import uuid
import warnings
import zoneinfo
from collections import OrderedDict
from collections.abc import Callable
from importlib.metadata import entry_points
from typing import Any

import numpy as np
import orjson

from bw2data.configuration import config
from bw2data.errors import UnsafePickleError

#: Key that marks a tagged value
TAG = "__bw__"
_TAG_BYTES = b'"__bw__"'
_TAG_STR = '"__bw__"'

#: Entry point group for packages which register their own types
ENTRY_POINT_GROUP = "bw2data.codec"

# orjson reads and writes integers in this range exactly; it turns larger ones into floats
_INT_MIN = -(2**63)
_INT_MAX = 2**64 - 1


class UnknownTaggedValue(dict):
    """A tagged value whose tag isn't registered, for example because the package that
    stored it isn't installed. It is a plain dict with the ``"__bw__"`` and ``"v"`` keys, and
    is stored again exactly as it was read."""


#: Exact type -> (tag, to_payload)
_ENCODERS: dict[type, tuple[str, Callable[[Any], Any]]] = {}
#: Tag -> from_payload
_DECODERS: dict[str, Callable[[Any], Any]] = {}
#: Tags defined in this module, which other packages can't replace
_BUILTIN_TAGS: set[str] = set()
_entry_points_loaded = False


def register(
    cls: type,
    tag: str,
    to_payload: Callable[[Any], Any],
    from_payload: Callable[[Any], Any],
) -> None:
    """Store instances of ``cls`` (exactly this class, not subclasses) as tagged values.

    ``to_payload(obj)`` returns the value to store. It may contain anything ``dumps`` can
    store, including other tagged types. ``from_payload(payload)`` gets that value back,
    already decoded, and returns the object. Tags should be namespaced by package, e.g.
    ``"bw_temporalis.TemporalDistribution"``.

    Registering the same class and tag again replaces the functions. A tag or class that is
    already registered for something else raises ``ValueError``."""
    if not isinstance(tag, str) or not tag:
        raise ValueError(f"Tag must be a non-empty string, not {tag!r}")
    if tag in _BUILTIN_TAGS:
        raise ValueError(f"Tag {tag!r} is reserved by bw2data")
    if cls in _ENCODERS and _ENCODERS[cls][0] != tag:
        raise ValueError(f"{_name(cls)} is already registered with tag {_ENCODERS[cls][0]!r}")
    for other_cls, (other_tag, _) in _ENCODERS.items():
        if other_tag == tag and other_cls is not cls:
            raise ValueError(f"Tag {tag!r} is already registered for {_name(other_cls)}")
    _ENCODERS[cls] = (tag, to_payload)
    _DECODERS[tag] = from_payload


def _register_builtin(cls, tag, to_payload, from_payload):
    _ENCODERS[cls] = (tag, to_payload)
    _DECODERS[tag] = from_payload
    _BUILTIN_TAGS.add(tag)


def _name(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _load_entry_points() -> bool:
    """Load the ``bw2data.codec`` entry points once. Returns whether this call loaded them.

    A plugin which fails to load is skipped with a warning; its tags stay unknown."""
    global _entry_points_loaded
    if _entry_points_loaded:
        return False
    _entry_points_loaded = True
    for entry_point in entry_points(group=ENTRY_POINT_GROUP):
        try:
            entry_point.load()()
        except Exception as err:
            warnings.warn(
                f"Can't load the bw2data codec plugin {entry_point.value!r}: {err}",
                stacklevel=3,
            )
    return True


# Built-in types


def _datetime_to_payload(obj: datetime.datetime):
    tz = obj.tzinfo
    if tz is None or type(tz) is datetime.timezone:
        return obj.isoformat()
    if type(tz) is zoneinfo.ZoneInfo and tz.key:
        return {"iso": obj.replace(tzinfo=None).isoformat(), "zone": tz.key, "fold": obj.fold}
    raise TypeError(
        f"Can't store a datetime with time zone {tz!r} ({_name(type(tz))}); use "
        "datetime.timezone or zoneinfo.ZoneInfo"
    )


def _datetime_from_payload(payload):
    if isinstance(payload, str):
        return datetime.datetime.fromisoformat(payload)
    return datetime.datetime.fromisoformat(payload["iso"]).replace(
        tzinfo=zoneinfo.ZoneInfo(payload["zone"]), fold=payload["fold"]
    )


def _time_to_payload(obj: datetime.time):
    if obj.tzinfo is not None and type(obj.tzinfo) is not datetime.timezone:
        raise TypeError(f"Can't store a time with time zone {obj.tzinfo!r}; use datetime.timezone")
    return obj.isoformat()


def _sorted_if_possible(values) -> list:
    """Sorted, so that equal sets are stored the same way; unsortable mixes keep their order."""
    try:
        return sorted(values)
    except TypeError:
        return list(values)


def _bytes_to_payload(obj) -> str:
    return base64.b64encode(obj).decode("ascii")


def _ndarray_to_payload(arr: np.ndarray):
    if arr.dtype.fields is not None:
        raise TypeError(f"Can't store a numpy array with a structured dtype ({arr.dtype})")
    if arr.dtype.kind in "Mm":
        # datetime64 and timedelta64: ``tolist`` would give ints for some units and
        # datetimes for others, so store the raw integers and the unit in the dtype
        data = arr.astype(np.int64).ravel().tolist()
    else:
        data = arr.ravel().tolist()
    return [str(arr.dtype), list(arr.shape), data]


def _ndarray_from_payload(payload):
    dtype, shape, data = payload
    dtype = np.dtype(dtype)
    if dtype.kind in "Mm":
        return np.array(data, dtype=np.int64).astype(dtype).reshape(shape)
    if dtype.kind == "O":
        # ``np.array`` would turn elements which are sequences into extra dimensions
        arr = np.empty(len(data), dtype=object)
        for i, item in enumerate(data):
            arr[i] = item
        return arr.reshape(shape)
    return np.array(data, dtype=dtype).reshape(shape)


def _datetime64_scalar_from_payload(payload):
    dtype, value = payload
    return np.array(value, dtype=np.int64).astype(np.dtype(dtype))[()]


def _str_from_payload(payload):
    return base64.b64decode(payload).decode("utf-8", "surrogatepass")


_FLOATS = {"nan": math.nan, "inf": math.inf, "-inf": -math.inf}

_register_builtin(tuple, "tuple", list, tuple)
_register_builtin(set, "set", _sorted_if_possible, set)
_register_builtin(frozenset, "frozenset", _sorted_if_possible, frozenset)
_register_builtin(OrderedDict, "OrderedDict", lambda d: [[k, v] for k, v in d.items()], OrderedDict)
_register_builtin(datetime.datetime, "datetime", _datetime_to_payload, _datetime_from_payload)
_register_builtin(datetime.date, "date", datetime.date.isoformat, datetime.date.fromisoformat)
_register_builtin(datetime.time, "time", _time_to_payload, datetime.time.fromisoformat)
_register_builtin(
    datetime.timedelta,
    "timedelta",
    lambda td: [td.days, td.seconds, td.microseconds],
    lambda p: datetime.timedelta(days=p[0], seconds=p[1], microseconds=p[2]),
)
_register_builtin(decimal.Decimal, "decimal", str, decimal.Decimal)
_register_builtin(uuid.UUID, "uuid", str, uuid.UUID)
_register_builtin(bytes, "bytes", _bytes_to_payload, base64.b64decode)
_register_builtin(
    bytearray, "bytearray", _bytes_to_payload, lambda p: bytearray(base64.b64decode(p))
)
_register_builtin(complex, "complex", lambda c: [c.real, c.imag], lambda p: complex(*p))
_register_builtin(np.ndarray, "numpy.ndarray", _ndarray_to_payload, _ndarray_from_payload)
# Tags without an encoder entry: written by ``_encode`` directly
_DECODERS["dict"] = lambda pairs: {k: v for k, v in pairs}
_DECODERS["float"] = _FLOATS.__getitem__
_DECODERS["int"] = int
_DECODERS["str"] = _str_from_payload
_DECODERS["numpy.datetime64"] = _datetime64_scalar_from_payload
_DECODERS["numpy.timedelta64"] = _datetime64_scalar_from_payload
_BUILTIN_TAGS.update(["dict", "float", "int", "str", "numpy.datetime64", "numpy.timedelta64"])


# Encoding


class _Unstorable(TypeError):
    def __init__(self, obj, reason=None):
        self.obj = obj
        self.reason = reason
        super().__init__(reason or f"Can't store {_name(type(obj))} as JSON: {obj!r}")


def _has_surrogates(s: str) -> bool:
    try:
        s.encode("utf-8")
    except UnicodeEncodeError:
        return True
    return False


def _plain(v, strings: bool) -> bool:
    """Whether ``v`` is stored as it is: a string (when not checking for surrogates), a
    boolean, ``None``, a finite float, or an integer in orjson's range."""
    t = type(v)
    return (
        (t is str and not strings)
        or v is None
        or t is bool
        or (t is float and v - v == 0.0)  # False for NaN and infinities
        or (t is int and _INT_MIN <= v <= _INT_MAX)
    )


def _encode(obj, strings: bool = False):
    """Return ``obj`` as a tree of plain JSON values. With ``strings``, strings that aren't
    valid UTF-8 (lone surrogates) are tagged; that check is slow, so ``dumps`` only does it
    when orjson refuses a string."""
    t = type(obj)
    if t is str:
        if strings and _has_surrogates(obj):
            return {
                TAG: "str",
                "v": base64.b64encode(obj.encode("utf-8", "surrogatepass")).decode(),
            }
        return obj
    if t is bool or obj is None:
        return obj
    if t is int:
        if _INT_MIN <= obj <= _INT_MAX:
            return obj
        return {TAG: "int", "v": str(obj)}
    if t is float:
        if math.isfinite(obj):
            return obj
        return {TAG: "float", "v": "nan" if obj != obj else ("inf" if obj > 0 else "-inf")}
    if t is list:
        return [item if _plain(item, strings) else _encode(item, strings) for item in obj]
    if t is dict:
        if TAG not in obj:
            # Most values are plain strings and numbers; skipping the call for each of them
            # makes encoding about twice as fast
            out = {}
            for k, v in obj.items():
                if type(k) is not str or (strings and _has_surrogates(k)):
                    break
                vt = type(v)
                # Same test as ``_plain``, written out because this loop is the hot path
                if (
                    (vt is str and not strings)
                    or (vt is float and v - v == 0.0)
                    or v is None
                    or vt is bool
                    or (vt is int and _INT_MIN <= v <= _INT_MAX)
                ):
                    out[k] = v
                else:
                    out[k] = _encode(v, strings)
            else:
                return out
        return {
            TAG: "dict",
            "v": [[_encode(k, strings), _encode(v, strings)] for k, v in obj.items()],
        }
    if t is UnknownTaggedValue:
        return {k: _encode(v, strings) for k, v in obj.items()}
    entry = _ENCODERS.get(t)
    if entry is not None:
        tag, to_payload = entry
        try:
            payload = to_payload(obj)
        except _Unstorable:
            raise
        except TypeError as err:
            raise _Unstorable(obj, str(err)) from err
        return {TAG: tag, "v": _encode(payload, strings)}
    if isinstance(obj, np.generic):
        if obj.dtype.kind == "M":
            return {TAG: "numpy.datetime64", "v": [str(obj.dtype), int(obj.astype(np.int64))]}
        if obj.dtype.kind == "m":
            return {TAG: "numpy.timedelta64", "v": [str(obj.dtype), int(obj.astype(np.int64))]}
        return _encode(obj.item(), strings)
    if _load_entry_points():
        return _encode(obj, strings)
    raise _Unstorable(obj)


def _path_to(target, obj, path="value"):
    """Where ``target`` is inside ``obj``, as a readable path, or ``None``."""
    if obj is target:
        return path
    if isinstance(obj, dict):
        for k, v in obj.items():
            found = _path_to(target, v, f"{path}[{k!r}]")
            if found:
                return found
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            found = _path_to(target, v, f"{path}[{i}]")
            if found:
                return found
    return None


def encode(obj) -> Any:
    """Return ``obj`` as plain JSON values (dicts, lists, strings, numbers, booleans and
    ``None``), with tagged values for everything else. Raises ``TypeError`` for a value that
    can't be stored."""
    try:
        return _encode(obj)
    except _Unstorable as err:
        where = _path_to(err.obj, obj) or "value"
        raise TypeError(f"{err} (at {where})") from None


def dumps(obj) -> bytes:
    """Serialize ``obj`` to UTF-8 JSON bytes. See the module docstring for what can be stored.

    Raises ``TypeError`` for a value that can't be stored."""
    tree = encode(obj)
    try:
        return orjson.dumps(tree)
    except orjson.JSONEncodeError:
        # ``encode`` only leaves values orjson can write, except strings with lone
        # surrogates, which pickle stored but UTF-8 can't represent. Tag those and try again;
        # anything else fails again here.
        return orjson.dumps(_encode(obj, strings=True))


# Decoding


def _decode_tagged(raw: dict) -> Any:
    tag = raw[TAG]
    payload = _decode(raw.get("v"))
    decoder = _DECODERS.get(tag)
    if decoder is None and tag not in _BUILTIN_TAGS and _load_entry_points():
        decoder = _DECODERS.get(tag)
    if decoder is None:
        return UnknownTaggedValue({k: payload if k == "v" else _decode(v) for k, v in raw.items()})
    if tag in _BUILTIN_TAGS:
        return decoder(payload)
    try:
        return decoder(payload)
    except Exception as err:
        warnings.warn(
            f"Can't restore a value tagged {tag!r} ({err}); keeping it as an " "UnknownTaggedValue",
            stacklevel=4,
        )
        return UnknownTaggedValue({k: payload if k == "v" else _decode(v) for k, v in raw.items()})


def _decode(obj):
    t = type(obj)
    if t is list:
        return [_decode(item) for item in obj]
    if t is dict:
        if TAG in obj:
            return _decode_tagged(obj)
        return {k: _decode(v) for k, v in obj.items()}
    return obj


def decode(tree) -> Any:
    """Turn plain JSON values written by ``encode`` back into the original objects."""
    return _decode(tree)


def loads(data: bytes | bytearray | memoryview | str) -> Any:
    """Read JSON written by ``dumps`` (or any plain JSON) back into Python objects."""
    if isinstance(data, memoryview):
        data = data.tobytes()
    tree = orjson.loads(data)
    tagged = _TAG_STR in data if isinstance(data, str) else _TAG_BYTES in data
    if not tagged:
        # Nothing is tagged, so ``tree`` is already the value
        return tree
    return _decode(tree)


# Restricted unpickler for legacy data

#: ``(module, name)`` pairs that pickles written by bw2data may contain. Everything else is
#: refused unless listed in ``config.trusted_pickle_classes``.
SAFE_PICKLE_GLOBALS = frozenset(
    {
        ("builtins", "set"),
        ("builtins", "frozenset"),
        ("builtins", "complex"),
        ("builtins", "bytearray"),
        ("builtins", "slice"),
        ("builtins", "range"),
        ("_codecs", "encode"),  # bytes in protocol 2 pickles
        ("collections", "OrderedDict"),
        ("datetime", "datetime"),
        ("datetime", "date"),
        ("datetime", "time"),
        ("datetime", "timedelta"),
        ("datetime", "timezone"),
        # Not ``zoneinfo.ZoneInfo``: it is pickled through ``builtins.getattr``, which would
        # let a pickle reach any attribute of any allowed class
        ("decimal", "Decimal"),
        ("uuid", "UUID"),
        ("uuid", "SafeUUID"),
        ("numpy", "dtype"),
        ("numpy", "ndarray"),
        ("numpy.core.multiarray", "scalar"),
        ("numpy.core.multiarray", "_reconstruct"),
        ("numpy.core.numeric", "_frombuffer"),
        ("numpy._core.multiarray", "scalar"),
        ("numpy._core.multiarray", "_reconstruct"),
        ("numpy._core.numeric", "_frombuffer"),
    }
)

_NUMPY_2 = int(np.__version__.split(".")[0]) >= 2


def _trusted(module: str, name: str) -> bool:
    trusted = config.trusted_pickle_classes or ()
    return f"{module}.{name}" in trusted or f"{module}:{name}" in trusted


class _RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        # Protocol 0-2 pickles can use Python 2 names (``__builtin__.set``); map them first,
        # as ``pickle.Unpickler.find_class`` does, so the check sees the real names
        if (module, name) in _compat_pickle.NAME_MAPPING:
            module, name = _compat_pickle.NAME_MAPPING[(module, name)]
        elif module in _compat_pickle.IMPORT_MAPPING:
            module = _compat_pickle.IMPORT_MAPPING[module]
        if (module, name) not in SAFE_PICKLE_GLOBALS and not _trusted(module, name):
            raise UnsafePickleError(
                f"Refusing to load {module}.{name} from a pickle. Only built-in types, "
                "datetime, decimal, uuid and numpy are allowed. If you trust where this data "
                f"came from, add '{module}.{name}' to `config.trusted_pickle_classes`."
            )
        if _NUMPY_2 and module.startswith("numpy.core."):
            # Written by numpy 1.x; ``numpy.core`` is a deprecated alias in numpy 2
            module = "numpy._core." + module[len("numpy.core.") :]
        return super().find_class(module, name)

    def persistent_load(self, pid):
        raise UnsafePickleError("Refusing to load a pickle with persistent ids")


def safe_pickle_loads(data: bytes | bytearray | memoryview) -> Any:
    """Unpickle ``data``, refusing anything except the classes in ``SAFE_PICKLE_GLOBALS``
    and ``config.trusted_pickle_classes``. Raises ``UnsafePickleError`` for anything else."""
    return _RestrictedUnpickler(io.BytesIO(bytes(data))).load()
