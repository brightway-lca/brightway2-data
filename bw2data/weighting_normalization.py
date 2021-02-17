from .backends.schema import get_id
from .ia_data_store import ImpactAssessmentDataStore
from .meta import weightings, normalizations
from .project import writable_project
from .utils import as_uncertainty_dict
from .validate import weighting_validator, normalization_validator


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
    matrix = "weighting_matrix"

    @writable_project
    def write(self, data):
        """Because of DataStore assumptions, need a one-element list"""
        if self.name not in self._metadata:
            self.register()
        if not isinstance(data, list) or not len(data) == 1:
            raise ValueError("Weighting data must be one-element list")
        super(Weighting, self).write(data)

    def process_row(self, row):
        """Return an empty tuple (as ``dtype_fields`` is empty), and the weighting uncertainty dictionary."""
        return {**as_uncertainty_dict(row), "row": 0}


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
    matrix = "normalization_matrix"

    def process_row(self, row):
        """Given ``(flow key, amount)``, return a dictionary for array insertion."""
        return {
            **as_uncertainty_dict(row[1]),
            "row": get_id(row[0]),
        }
