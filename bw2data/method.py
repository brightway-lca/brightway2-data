# -*- coding: utf-8 -*-
from . import mapping, methods, geomapping
from .utils import MAX_INT_32
from .validate import ia_validator
from .ia_data_store import ImpactAssessmentDataStore
import numpy as np


class Method(ImpactAssessmentDataStore):
    """A manager for a method. This class can register or deregister methods, write intermediate data, process data to parameter arrays, validate, and copy methods.

    The Method class never holds intermediate data, but it can load or write intermediate data. The only attribute is *method*, which is the name of the method being managed.

    Instantiation does not load any data. If this method is not yet registered in the metadata store, a warning is written to ``stdout``.

    Methods are hierarchally structured, and this structure is preserved in the method name. It is a tuple of strings, like ``('ecological scarcity 2006', 'total', 'natural resources')``.

    Args:
        * *name* (tuple): Name of the method to manage. Must be a tuple of strings.

    """
    metadata = methods
    label = u"method"

    @property
    def method(self):
        return self.name

    def register(self, unit, description="", num_cfs=0, **kwargs):
        """Register a method with the metadata store.

        Methods must be registered before data can be written.

        Args:
            * *unit* (str): Unit for impact assessment CFs
            * *description* (str): Description
            * *num_cfs* (int): Number of characterization factors

        """
        kwargs.update({
            "unit":unit,
            "description": description,
            "num_cfs": num_cfs
        })
        super(Method, self).register(**kwargs)

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
            * *data* (dict): Method data

        """
        mapping.add(set([x[0] for x in data]))
        geomapping.add(set([x[2] for x in data]))
        super(Method, self).write(data)

    def process(self):
        """
Process intermediate data from a Python dictionary to a `stats_arrays <https://pypi.python.org/pypi/stats_arrays/>`_ array, which is a `NumPy <http://numpy.scipy.org/>`_ `Structured <http://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html#numpy.recarray>`_ `Array <http://docs.scipy.org/doc/numpy/user/basics.rec.html>`_. A structured array (also called record array) is a heterogeneous array, where each column has a different label and data type.

Processed arrays are saved in the ``processed`` directory.

Although it is not standard to provide uncertainty distributions for impact assessment methods, the structured array includes uncertainty fields.

The structure for processed IA methods includes additional columns beyond the basic ``stats_arrays`` format:

================ ======== ===================================
Column name      Type     Description
================ ======== ===================================
uncertainty_type uint8    integer type defined in `stats_arrays.uncertainty_choices`
flow             uint32   integer value from `Mapping`
index            uint32   column filled with `NaN` values, used for matrix construction
geo              uint32   integer value from `GeoMapping`
amount           float32  location parameter, e.g. mean
loc              float32  location parameter, e.g. mean
scale            float32  scale parameter, e.g. standard deviation
shape            float32  shape parameter
minimum          float32  minimum bound
maximum          float32  maximum bound
negative         bool     `amount` < 0
================ ======== ===================================

See also `NumPy data types <http://docs.scipy.org/doc/numpy/user/basics.types.html>`_.

Doesn't return anything, but writes a file to disk.

        """
        data = self.load()
        assert data
        dtype = [
            ('uncertainty_type', np.uint8),
            ('flow', np.uint32),
            ('index', np.uint32),
            ('geo', np.uint32),
            ('amount', np.float32),
            ('loc', np.float32),
            ('scale', np.float32),
            ('shape', np.float32),
            ('minimum', np.float32),
            ('maximum', np.float32),
            ('negative', np.bool)
        ]
        arr = np.zeros((len(data), ), dtype=dtype)
        for i, (key, value, geo) in enumerate(data):
            if isinstance(value, dict):
                # LCIA with uncertainty
                arr[i] = (
                    value["uncertainty type"],
                    mapping[key],
                    MAX_INT_32,
                    geomapping[geo],
                    value["amount"],
                    value.get("loc", np.NaN),
                    value.get("scale", np.NaN),
                    value.get("shape", np.NaN),
                    value.get("minimum", np.NaN),
                    value.get("maximum", np.NaN),
                    value.get("amount" < 0)
                )
            else:
                arr[i] = (
                    0,
                    mapping[key],
                    MAX_INT_32,
                    geomapping[geo],
                    value,
                    value,
                    np.NaN,
                    np.NaN,
                    np.NaN,
                    np.NaN,
                    False
                )
        self.write_processed_array(arr)
