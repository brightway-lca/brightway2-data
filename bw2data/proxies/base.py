from .. import databases
from ..errors import InvalidExchange
from ..utils import get_activity
from stats_arrays import uncertainty_choices
import collections


class ProxyBase(collections.MutableMapping):
    def __init__(self, data, *args, **kwargs):
        self._data = data

    def as_dict(self):
        return self._data

    def __str__(self):
        return unicode(self).encode('utf-8')

    __repr__ = __str__

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
    def __unicode__(self):
        if self.valid():
            return u"'%s' (%s, %s, %s)".format(
                self.get('name'),
                self.get('unit'),
                self.get('location'),
                self.get('categories')
            )
        else:
            return u"Activity with missing fields (call ``valid(why=True)`` to see more)"

    @property
    def key(self):
        return (self.get("database"), self.get("code"))

    def __eq__(self, other):
        return self.key == other

    def __hash__(self):
        return hash(self.key)

    def __getitem__(self, key):
        # Basically a hack to let this act like a tuple with two
        # elements, database and code.
        if key == 0:
            return self.database
        elif key == 1:
            return self.code
        return self._data[key]

    def __delitem__(self, key):
        del self._data[key]

    def valid(self, why=False):
        errors = []
        if not self.get('database'):
            errors.append(u"Missing field ``database``")
        elif self.get('database') not in databases:
            errors.append(u"``database`` refers to unknown database")
        if not self.get('code'):
            errors.append(u"Missing field ``code``")
        if not self.get('name'):
            errors.append(u"Missing field ``name``")
        if errors:
            if why:
                return (False, errors)
            else:
                return False
        else:
            return True

    def lca(self, method=None, amount=1.):
        """Shortcut to construct an LCA object for this activity."""
        from bw2calc import LCA

        lca = LCA({self: amount}, method=method)
        lca.lci()
        if method is not None:
            lca.lcia()
        lca.fix_dictionaries()
        return lca

    def as_functional_unit(self, amount=1.):
        """Return functional unit as expected by LCA classes"""
        return {self: amount}

    def lca(self, method=None, amount=1.):
        """Shortcut to construct an LCA object for this activity."""
        from bw2calc import LCA

        lca = LCA(self.as_functional_unit(amount), method=method)
        lca.lci()
        if method is not None:
            lca.lcia()
        lca.fix_dictionaries()
        return lca


class ExchangeProxyBase(ProxyBase):
    def __unicode__(self):
        if self.valid():
            return u"Exchange: {} {} {} to {}>".format(self.amount, self.unit,
                self.input, self.output)
        else:
            return u"Exchange with missing fields (call ``valid(why=True)`` to see more)"

    def _get_input(self):
        """Get or set the exchange input.

        When getting, returns an `Activity` - this will raise an error if the linked activity doesn't yet exist.

        When setting, either an `Activity` or a tuple can be given. The linked activity does not have to exist yet."""
        if not self.get("input"):
            raise InvalidExchange("Missing valid data for `input` field")
        elif not hasattr(self, "_input"):
            self._input = get_activity(self['input'])

    def _set_input(self, value):
        if isinstance(value, ActivityProxyBase):
            self._input = value
            self._data['input'] = value.key
        elif isinstance(value, (tuple, list)):
            self._data['input'] = value
        else:
            raise ValueError("Provided input data is invalid")

    def _get_output(self):
        """Get or set the exchange output.

        When getting, returns an `Activity` - this will raise an error if the linked activity doesn't yet exist.

        When setting, either an `Activity` or a tuple can be given. The linked activity does not have to exist yet."""
        if not self.get("output"):
            raise InvalidExchange("Missing valid data for `output` field")
        elif not hasattr(self, "_output"):
            self._output = get_activity(self['output'])

    def _set_output(self, value):
        if isinstance(value, ActivityProxyBase):
            self._output = value
            self._data['output'] = value.key
        elif isinstance(value, (tuple, list)):
            self._data['output'] = value
        else:
            raise ValueError("Provided input data is invalid")

    input = property(_get_input, _set_input)
    output = property(_get_output, _set_output)

    def valid(self, why=False):
        errors = []
        if not self.get('input'):
            errors.append(u"Missing field ``input``")
        if not self.get('output'):
            errors.append(u"Missing field ``output``")
        if not self.get('amount'):
            errors.append(u"Missing field ``amount``")
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
        return self.input.get('unit')

    @property
    def amount(self):
        return self.get('amount')

    @property
    def uncertainty(self):
        """Get uncertainty dictionary that can be used in uncertainty analysis."""
        KEYS = {
            u'uncertainty type',
            u'loc',
            u'scale',
            u'shape',
            u'minimum',
            u'maximum'
        }
        return {k: v for k, v in self.items() if k in KEYS}

    @property
    def uncertainty_type(self):
        """Get uncertainty type as a ``stats_arrays`` class."""
        return uncertainty_choices[self.get(u"uncertainty type", 0)]

    def random_sample(self, n=100):
        """Draw a random sample from this exchange."""
        ut = self.uncertainty_type
        array = ut.from_dicts(self.uncertainty)
        return ut.bounded_random_variables(array, n).ravel()

    def as_functional_unit(self):
        """Return functional unit as expected by LCA classes"""
        return {self.input: self.amount}

    def lca(self, method=None):
        """Shortcut to construct an LCA object for this activity."""
        from bw2calc import LCA

        lca = LCA(self.as_functional_unit(), method=method)
        lca.lci()
        if method is not None:
            lca.lcia()
        lca.fix_dictionaries()
        return lca
