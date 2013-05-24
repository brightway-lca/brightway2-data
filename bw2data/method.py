# -*- coding: utf-8 -*-
from . import config, mapping, methods, geomapping
from copy import copy
from errors import UnknownObject, MissingIntermediateData
from utils import MAX_INT_32, random_string
from validate import ia_validator
import numpy as np
import os
import string
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle


def abbreviate(names, length=8):
    abbrev = lambda x: x if x[0] in string.digits else x[0].lower()
    name = " ".join(names).split(" ")[0].lower() + \
        "".join([abbrev(x) for x in " ".join(names).split(" ")[1:]])
    return name + "-" + random_string(length)


class Method(object):
    """A manager for a method. This class can register or deregister methods, write intermediate data, process data to parameter arrays, validate, and copy methods.

    The Method class never holds intermediate data, but it can load or write intermediate data. The only attribute is *method*, which is the name of the method being managed.

    Instantiation does not load any data. If this method is not yet registered in the metadata store, a warning is written to ``stdout``.

    Methods are hierarchally structured, and this structure is preserved in the method name. It is a tuple of strings, like ``('ecological scarcity 2006', 'total', 'natural resources')``.

    Args:
        * *method* (tuple): Name of the method to manage. Must be a tuple of strings.

    """
    def __init__(self, method, *args, **kwargs):
        self.method = tuple(method)
        if self.method not in methods and not \
                getattr(config, "dont_warn", False):
            warnings.warn("\n\t%s not a currently installed method" % (
                " : ".join(method)), UserWarning)

    def get_abbreviation(self):
        """Abbreviate a method identifier (a tuple of long strings) for a filename. Random characters are added because some methods have similar names which would overlap when abbreviated."""
        try:
            return methods[self.method]["abbreviation"]
        except KeyError:
            raise UnknownObject("This method is not yet registered")

    def copy(self, name=None):
        """Make a copy of the method.

        Args:
            * *name* (tuple, optional): Name of the new method.

        """
        name = tuple(name) or self.method[:-1] + ("Copy of " +
            self.method[-1],)
        new_method = Method(name)
        metadata = copy(methods[self.method])
        del metadata["abbreviation"]
        new_method.register(**metadata)
        new_method.write(self.load())

    def register(self, unit, description="", num_cfs=0):
        """Register a database with the metadata store.

        Methods must be registered before data can be written.

        Args:
            * *unit* (str): Unit for impact assessment CFs
            * *description* (str): Description
            * *num_cfs* (int): Number of characterization factors

        """
        assert self.method not in methods
        methods[self.method] = {
            "abbreviation": abbreviate(self.method),
            "unit": unit,
            "description": description,
            "num_cfs": num_cfs
        }

    def deregister(self):
        """Remove a method from the metadata store. Does not delete any files."""
        del methods[self.method]

    def validate(self, data):
        """Validate data. Must be called manually.

        Args:
            * *data* (dict): The data, in its processed form.

        """
        ia_validator(data)
        return True

    def write(self, data):
        """Serialize data to disk.

        Args:
            * *data* (dict): Inventory data

        """
        if self.method not in methods:
            raise UnknownObject("This database is not yet registered")
        mapping.add(set([x[0] for x in data]))
        geomapping.add(set([x[2] for x in data]))
        filepath = os.path.join(
            config.dir,
            "intermediate",
            "%s.pickle" % self.get_abbreviation()
        )
        with open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load(self):
        """Load the intermediate data for this method.

        Returns:
            The intermediate data, a dictionary.

        """
        try:
            return pickle.load(open(os.path.join(
                config.dir,
                "intermediate",
                "%s.pickle" % self.get_abbreviation()
            ), "rb"))
        except OSError:
            raise MissingIntermediateData("Can't load intermediate data")

    def process(self):
        """
Process intermediate data from a Python dictionary to a `NumPy <http://numpy.scipy.org/>`_ `Structured <http://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html#numpy.recarray>`_ `Array <http://docs.scipy.org/doc/numpy/user/basics.rec.html>`_. A structured array (also called record array) is a heterogeneous array, where each column has a different label and data type. These structured arrays act as a standard data format for LCA and Monte Carlo calculations, and are the native data format for the Stats Arrays package.

Processed arrays are saved in the ``processed`` directory.

Although it is not standard to provide uncertainty distributions for impact assessment methods, the structured array includes uncertainty fields.

The structure for processed inventory databases is:

================ ======== ===================================
Column name      Type     Description
================ ======== ===================================
uncertainty_type uint8    integer type defined in `stats_toolkit.uncertainty_choices`
flow             uint32   integer value from `Mapping`
index            uint32   column filled with `NaN` values, used for matrix construction
geo              uint32   integer value from `GeoMapping`
amount           float32  location parameter, e.g. mean
sigma            float32  shape parameter, e.g. std
minimum          float32  minimum bound
maximum          float32  maximum bound
negative         bool     `amount` < 0
================ ======== ===================================

See also `NumPy data types <http://docs.scipy.org/doc/numpy/user/basics.types.html>`_.

Doesn't return anything, but writes a file to disk.

        """
        data = pickle.load(open(os.path.join(
            config.dir,
            "intermediate",
            "%s.pickle" % self.get_abbreviation()
        ), "rb"))
        assert data
        dtype = [
            ('uncertainty_type', np.uint8),
            ('flow', np.uint32),
            ('index', np.uint32),
            ('geo', np.uint32),
            ('amount', np.float32),
            ('sigma', np.float32),
            ('minimum', np.float32),
            ('maximum', np.float32),
            ('negative', np.bool)
        ]
        arr = np.zeros((len(data), ), dtype=dtype)
        arr['minimum'] = arr['maximum'] = arr['sigma'] = np.NaN
        for i, (key, value, geo) in enumerate(data):
            arr[i] = (
                0,
                mapping[key],
                MAX_INT_32,
                geomapping[geo],
                value,
                np.NaN,
                np.NaN,
                np.NaN,
                False
            )
        filepath = os.path.join(config.dir, "processed", "%s.pickle" %
                                self.get_abbreviation())
        with open(filepath, "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)
