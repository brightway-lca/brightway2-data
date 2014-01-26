from .ia_data_store import ImpactAssessmentDataStore
from .meta import weightings, mapping, normalizations
from .utils import MAX_INT_32
from .validate import weighting_validator, normalization_validator
import numpy as np


class Weighting(ImpactAssessmentDataStore):
    metadata = weightings
    valdiator = weighting_validator
    dtype_fields = []

    def write(self, data):
        """Because of DataStore assumptions, need a one-element list"""
        if not len(data) == 1 or not isinstance(data, list):
            raise ValueError("Weighting data must be one-element list")
        super(Weighting, self).write(data)

    def process_data(self, row):
        return (), row


class Normalization(ImpactAssessmentDataStore):
    metadata = normalizations
    valdiator = normalization_validator
    dtype_fields = [
        ('flow', np.uint32),
        ('index', np.uint32),
    ]

    def add_mappings(self, data):
        mapping.add({obj[0] for obj in data})

    def process_data(self, row):
        return (
            mapping[row[0]],
            MAX_INT_32,
            ), row[1]
