# -*- coding: utf-8 -*-
from . import mapping, methods, geomapping, config
from .utils import MAX_INT_32
from .validate import ia_validator
from .ia_data_store import ImpactAssessmentDataStore
import numpy as np


class Method(ImpactAssessmentDataStore):
    """A manager for an impact assessment method. This class can register or deregister methods, write intermediate data, process data to parameter arrays, validate, and copy methods.

    The Method class never holds intermediate data, but it can load or write intermediate data. The only attribute is *name*, which is the name of the method being managed.

    Instantiation does not load any data. If this method is not yet registered in the metadata store, a warning is written to ``stdout``.

    Methods are hierarchally structured, and this structure is preserved in the method name. It is a tuple of strings, like ``('ecological scarcity 2006', 'total', 'natural resources')``.

    The data schema for IA methods is:

    .. code-block:: python

            Schema([Any(
                [valid_tuple, maybe_uncertainty],         # site-generic
                [valid_tuple, maybe_uncertainty, object]  # regionalized
            )])

    where:
        * ``valid_tuple`` is a dataset identifier, like ``("biosphere", "CO2")``
        * ``maybe_uncertainty`` is either a number or an uncertainty dictionary
        * ``object`` is a location, needed only for regionalized LCIA

    Args:
        * *name* (tuple): Name of the method to manage. Must be a tuple of strings.

    """
    metadata = methods
    validator = ia_validator
    dtype_fields = [
            ('flow', np.uint32),
            ('geo', np.uint32),
            ('row', np.uint32),
            ('col', np.uint32),
    ]

    def add_mappings(self, data):
        mapping.add({x[0] for x in data})
        geomapping.add({x[2] for x in data if len(x) == 3})

    def process_data(self, row):
        return (
            mapping[row[0]],
            geomapping[row[2]] if len(row) == 3 \
                else geomapping[config.global_location],
            MAX_INT_32,
            MAX_INT_32,
            ), row[1]
