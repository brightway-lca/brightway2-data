# -*- coding: utf-8 -*
from .base import ExchangeProxyBase
from ..utils import get_activity


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
