# -*- coding: utf-8 -*
from .activity import Activity
from stats_arrays import uncertainty_choices
import collections


class Exchange(collections.Mapping):
    """
Simple proxy for an exchange between activity datasets. Makes manipulation and use in command line more convenient.

.. warning:: This proxy is read only! To save changes to a dataset, you will need to work with the raw database data.

Usually these proxies are created by the :ref:`activity`, but you can instantiate one with the dictionary of exchange data and an Activity proxy of the consuming activity:

.. code-block:: python

    exchange = Exchange({"my exchange data": "goes here"}, my_activity_proxy)

Properties:

* ``input``: Returns :ref:`activity`
* ``to``: Returns :ref:`activity`
* ``amount``
* ``uncertainty``: Returns dictionary of uncertainty data
* ``uncertainty_type``: Returns ``stats_arrays`` uncertainty type
* ``unit``

    """

    def __init__(self, exc, activity):
        self._raw = exc
        self.input = Activity(self._raw['input'])
        self.to = activity

    def __str__(self):
        return "%s %s to %s" % (self.amount, str(self.input), str(self.to))

    def __unicode__(self):
        return u"%.2g %s from %s to %s" % (self.amount, self.unit, self.input, self.to)

    def __repr__(self):
        return unicode(self).encode('utf-8')

    def __setitem__(self, key, value):
        raise AttributeError("Exchange proxies are read-only.")

    def __getitem__(self, key):
        return self._raw[key]

    def __getattr__(self, attr):
        attr = unicode(attr)
        if attr in self:
            return self._raw[attr]
        else:
            return None

    def __iter__(self):
        return iter(self._raw)

    def __len__(self):
        return len(self._raw)

    @property
    def unit(self):
        return self.input.unit

    @property
    def uncertainty(self):
        KEYS = {
            u'uncertainty type',
            u'loc',
            u'scale',
            u'shape',
            u'minimum',
            u'maximum'
        }
        return {k: v for k, v in self._raw.items() if k in KEYS}

    @property
    def uncertainty_type(self):
        return uncertainty_choices[self._raw.get(u"uncertainty type", 0)]

    def random_sample(self, n=100):
        """Draw a random sample from this exchange."""
        ut = self.uncertainty_type
        array = ut.from_dicts(self.uncertainty)
        return ut.bounded_random_variables(array, n).ravel()

    def as_functional_unit(self):
        return {self.input: self.amount}


class Exchanges(collections.Sequence):
    """Proxy for a list of Exchange objects."""

    def __init__(self, exchanges, activity=None, as_exchanges=False):
        if as_exchanges:
            self._exchanges = exchanges
        else:
            if not activity:
                raise ValueError(u"Missing `activity`")
            self._exchanges = [Exchange(obj, activity) for obj in exchanges]

    @property
    def technosphere(self):
        return Exchanges([obj for obj in self if obj.type == u"technosphere"], as_exchanges=True)

    @property
    def biosphere(self):
        return Exchanges([obj for obj in self if obj.type == u"biosphere"], as_exchanges=True)

    @property
    def production(self):
        return Exchanges([obj for obj in self if obj.type == u"production"], as_exchanges=True)

    def __getitem__(self, index):
        return self._exchanges[index]

    def __len__(self):
        return len(self._exchanges)

    def __str__(self):
        if len(self):
            return "Exchanges proxy with %s exchanges" % len(self)
        else:
            return "Exchanges proxy (empty)"

    def __unicode__(self):
        if len(self):
            return u"Exchanges proxy with %s exchanges:\n\t" % len(self) + \
                u"\n\t".join([unicode(obj) for obj in self])
        else:
            return u"Exchanges proxy (empty)"

    def __repr__(self):
        return unicode(self).encode('utf-8')
