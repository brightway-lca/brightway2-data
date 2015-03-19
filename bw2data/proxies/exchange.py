# -*- coding: utf-8 -*
from .base import ExchangeProxyBase


class Exchange(ExchangeProxyBase):
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
        # Avoid circular reference
        from ..database import get_activity

        self._data = exc
        self.input = get_activity(exc['input'])
        # Output not specified, unless from SQLite
        self.output = activity

    def __setitem__(self, key, value):
        raise AttributeError("Exchange proxies are read-only.")

    def save(self):
        raise NotImplemented
