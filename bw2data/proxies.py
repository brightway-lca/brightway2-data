from . import databases
from .errors import InvalidExchange
from .utils import get_activity
from collections.abc import MutableMapping
from numbers import Number
from stats_arrays import uncertainty_choices


class ProxyBase(MutableMapping):
    def __init__(self, data, *args, **kwargs):
        self._data = data

    def as_dict(self):
        return self._data

    def __str__(self):
        return "Instance of base proxy class"

    __repr__ = lambda x: str(x)

    def __contains__(self, key):
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __eq__(self, other):
        return self._dict == other

    def __hash__(self):
        return hash(self._dict)


class ActivityProxyBase(ProxyBase):
    def __str__(self):
        if self.valid():
            return "'{}' ({}, {}, {})".format(
                self.get("name"),
                self.get("unit"),
                self.get("location"),
                self.get("categories"),
            )
        else:
            return "Activity with missing fields (call ``valid(why=True)`` to see more)"

    @property
    def key(self):
        return (self.get("database"), self.get("code"))

    def __eq__(self, other):
        return self.key == other

    def __lt__(self, other):
        if not isinstance(other, ActivityProxyBase):
            raise TypeError
        else:
            return self.key < other.key

    def __hash__(self):
        return hash(self.key)

    def __getitem__(self, key):
        # Basically a hack to let this act like a tuple with two
        # elements, database and code. Useful for using as functional unit in LCA.
        if key == 0:
            return self["database"]
        elif key == 1:
            return self["code"]
        return self._data[key]

    def __delitem__(self, key):
        del self._data[key]

    def valid(self, why=False):
        errors = []
        if not self.get("database"):
            errors.append("Missing field ``database``")
        elif self.get("database") not in databases:
            errors.append("``database`` refers to unknown database")
        if not self.get("code"):
            errors.append("Missing field ``code``")
        if not self.get("name"):
            errors.append("Missing field ``name``")
        if errors:
            if why:
                return (False, errors)
            else:
                return False
        else:
            return True

    def lca(self, method=None, amount=1.0):
        """Shortcut to construct an LCA object for this activity."""
        from bw2calc import LCA

        lca = LCA({self: amount}, method=method)
        lca.lci()
        if method is not None:
            lca.lcia()
        lca.fix_dictionaries()
        return lca


class ExchangeProxyBase(ProxyBase):
    def __str__(self):
        if self.valid():
            return "Exchange: {} {} {} to {}>".format(
                self.amount, self.unit, self.input, self.output
            )
        else:
            return "Exchange with missing fields (call ``valid(why=True)`` to see more)"

    def __lt__(self, other):
        if not isinstance(other, ExchangeProxyBase):
            raise TypeError
        else:
            return (self.input.key, self.output.key) < (
                other.input.key,
                other.output.key,
            )

    def _get_input(self):
        """Get or set the exchange input.

        When getting, returns an `Activity` - this will raise an error if the linked activity doesn't yet exist.

        When setting, either an `Activity` or a tuple can be given. The linked activity does not have to exist yet."""
        if not self.get("input"):
            raise InvalidExchange("Missing valid data for `input` field")
        elif not hasattr(self, "_input"):
            self._input = get_activity(self["input"])
        return self._input

    def _set_input(self, value):
        if isinstance(value, ActivityProxyBase):
            self._input = value
            self._data["input"] = value.key
        elif isinstance(value, (tuple, list)):
            self._data["input"] = value
        else:
            raise ValueError("Provided input data is invalid")

    def _get_output(self):
        """Get or set the exchange output.

        When getting, returns an `Activity` - this will raise an error if the linked activity doesn't yet exist.

        When setting, either an `Activity` or a tuple can be given. The linked activity does not have to exist yet."""
        if not self.get("output"):
            raise InvalidExchange("Missing valid data for `output` field")
        elif not hasattr(self, "_output"):
            self._output = get_activity(self["output"])
        return self._output

    def _set_output(self, value):
        if isinstance(value, ActivityProxyBase):
            self._output = value
            self._data["output"] = value.key
        elif isinstance(value, (tuple, list)):
            self._data["output"] = value
        else:
            raise ValueError("Provided input data is invalid")

    input = property(_get_input, _set_input)
    output = property(_get_output, _set_output)

    def __setitem__(self, key, value):
        if key == "input":
            self.input = value
        elif key == "output":
            self.output = value
        else:
            self._data[key] = value

    def valid(self, why=False):
        errors = []
        if not self.get("input"):
            errors.append("Missing field ``input``")
        elif not isinstance(self["input"], tuple):
            errors.append("Field ``input`` must be a tuple")
        elif self["input"][0] not in databases:
            errors.append(
                "Input database ``{}`` doesn't exist".format(self["input"][0])
            )

        if not self.get("output"):
            errors.append("Missing field ``output``")
        elif not isinstance(self["output"], tuple):
            errors.append("Field ``output`` must be a tuple")
        elif self["output"][0] not in databases:
            errors.append(
                "Output database ``{}`` doesn't exist".format(self["output"][0])
            )

        if not isinstance(self.get("amount", None), Number):
            errors.append("Invalid or missing field ``amount``")
        if not self.get("type"):
            errors.append("Missing field ``type``")
        if errors:
            if why:
                return (False, errors)
            else:
                return False
        else:
            return True

    @property
    def unit(self):
        """Get exchange unit.

        Separate property because the unit is a property of the input, not the exchange itself."""
        return self.input.get("unit")

    @property
    def amount(self):
        return self.get("amount")

    @property
    def uncertainty(self):
        """Get uncertainty dictionary that can be used in uncertainty analysis."""
        KEYS = {
            "uncertainty type",
            "loc",
            "scale",
            "shape",
            "minimum",
            "maximum",
            "negative",
        }
        return {k: v for k, v in self.items() if k in KEYS}

    @property
    def uncertainty_type(self):
        """Get uncertainty type as a ``stats_arrays`` class."""
        return uncertainty_choices[self.get("uncertainty type", 0)]

    def random_sample(self, n=100):
        """Draw a random sample from this exchange."""
        ut = self.uncertainty_type
        array = ut.from_dicts(self.uncertainty)
        return ut.bounded_random_variables(array, n).ravel()

    def lca(self, method=None, amount=None):
        """Shortcut to construct an LCA object for this exchange **input**.

        Uses the exchange amount if no other amount is provided."""
        from bw2calc import LCA

        if amount is None:
            amount = self["amount"]

        lca = LCA({self.input: amount}, method=method)
        lca.lci()
        if method is not None:
            lca.lcia()
        lca.fix_dictionaries()
        return lca
