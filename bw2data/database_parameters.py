# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *
from future.utils import python_2_unicode_compatible

from .serialization import PickledDict
from bw2parameters import ParameterSet
import collections
import copy


class DatabaseParameters(PickledDict):
    """A dictionary for global variables and formulas."""
    filename = "database-parameters.pickle"


database_parameters = DatabaseParameters()


@python_2_unicode_compatible
class DatabaseParameterSet(collections.MutableMapping):
    def __init__(self, key, data):
        self._key = key
        self._data = data

    def __contains__(self, key):
        return key in self._data

    def __hash__(self):
        return hash(self._data)

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return "Database parameter set with {} values".format(len(self))

    __repr__ = __str__

    def __setitem__(self, key, value):
        self._data[key] = value
        # Flushes data to disk
        database_parameters[self._key] = self._data

    def __getitem__(self, key):
        return self._data[key]

    def __delitem__(self, key):
        del self._data[key]

    def as_parameter_set(self):
        return ParameterSet(copy.deepcopy(self._data))

    def evaluate(self):
        return self.as_parameter_set().evaluate()
