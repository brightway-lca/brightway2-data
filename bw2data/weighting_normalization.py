from .ia_data_store import ImpactAssessmentDataStore
from .meta import weightings, mapping, normalizations
from .utils import MAX_INT_32
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
    metadata = weightings
    valdiator = weighting_validator
    dtype_fields = []

    def write(self, data):
        """Because of DataStore assumptions, need a one-element list"""
        if not len(data) == 1 or not isinstance(data, list):
            raise ValueError("Weighting data must be one-element list")
        super(Weighting, self).write(data)

    def process_data(self, row):
        return ((), # don't know much,
            row)    # but I know I love you


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
