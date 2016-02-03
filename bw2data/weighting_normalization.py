# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .ia_data_store import ImpactAssessmentDataStore
from .meta import weightings, mapping, normalizations
from .project import writable_project
from .utils import MAX_INT_32, numpy_string
from .validate import weighting_validator, normalization_validator
import numpy as np


class Weighting(ImpactAssessmentDataStore):
    """
    LCIA weighting data - used to combine or compare different impact categories.

    The data schema for weighting is a one-element list:

    .. code-block:: python

            Schema(All(
                [uncertainty_dict],
                Length(min=1, max=1)
            ))

    """
    _metadata = weightings
    validator = weighting_validator
    dtype_fields = []

    @writable_project
    def write(self, data):
        """Because of DataStore assumptions, need a one-element list"""
        if not isinstance(data, list) or not len(data) == 1:
            raise ValueError("Weighting data must be one-element list")
        super(Weighting, self).write(data)

    def process_data(self, row):
        """Return an empty tuple (as ``dtype_fields`` is empty), and the weighting uncertainty dictionary."""
        return (
            (), # don't know much,
            row # but I know I love you
        )


class Normalization(ImpactAssessmentDataStore):
    """
    LCIA normalization data - used to transform meaningful units, like mass or damage, into "person-equivalents" or some such thing.

    The data schema for IA normalization is:

    .. code-block:: python

            Schema([
                [valid_tuple, maybe_uncertainty]
            ])

    where:
        * ``valid_tuple`` is a dataset identifier, like ``("biosphere", "CO2")``
        * ``maybe_uncertainty`` is either a number or an uncertainty dictionary

    """
    _metadata = normalizations
    validator = normalization_validator
    dtype_fields = [
        (numpy_string('flow'), np.uint32),
        (numpy_string('index'), np.uint32),
    ]

    def add_mappings(self, data):
        """Add each normalization flow (should be biosphere flows) to global mapping"""
        mapping.add({obj[0] for obj in data})

    def process_data(self, row):
        """Return values that match ``dtype_fields``, as well as number or uncertainty dictionary"""
        return (
            mapping[row[0]],
            MAX_INT_32,
            ), row[1]
