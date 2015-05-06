# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ...proxies import ActivityProxyBase, ExchangeProxyBase
from ...utils import get_activity


class Activity(ActivityProxyBase):
    def __init__(self, key, data={}):
        data["database"], data["code"] = key[0], key[1]
        self._data = data

    def __setitem__(self, key, value):
        raise AttributeError("Activity proxies are read-only.")

    def save(self):
        raise NotImplemented

    def exchanges(self):
        return [Exchange(exc, self)
                for exc in self._data.get(u'exchanges', [])]

    def technosphere(self):
        return [Exchange(exc, self)
                for exc in self._data.get(u'exchanges', [])
                if exc.get('type') == 'technosphere']

    def biosphere(self):
        return [Exchange(exc, self)
                for exc in self._data.get(u'exchanges', [])
                if exc.get('type') == 'biosphere']

    def upstream(self, *args, **kwargs):
        raise NotImplemented

# -*- coding: utf-8 -*


class Exchange(ExchangeProxyBase):
    """
Simple proxy for an exchange between activity datasets. Makes manipulation and use in command line more convenient.

.. warning:: This proxy is read only! To save changes to a dataset, you will need to work with the raw database data.

Usually these proxies are created by the :ref:`activity`, but you can instantiate one with the dictionary of exchange data and an Activity proxy of the consuming activity:

.. code-block:: python

    exchange = Exchange({"my exchange data": "goes here"})

Properties:

* ``input``: Returns :ref:`activity`
* ``output``: Returns :ref:`activity`
* ``amount``
* ``uncertainty``: Returns dictionary of uncertainty data
* ``uncertainty_type``: Returns ``stats_arrays`` uncertainty type
* ``unit``

    """
    def __setitem__(self, key, value):
        raise AttributeError("Exchange proxies are read-only.")

    def save(self):
        raise NotImplemented
