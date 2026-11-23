from bw2data.backends.schema import get_id
from bw2data.ia_data_store import (
    ImpactAssessmentDataStore,
    SQLiteFactorStore,
    columns_as_value,
    flow_id,
    value_as_columns,
)
from bw2data.meta import normalizations, weightings
from bw2data.utils import as_uncertainty_dict
from bw2data.validate import normalization_validator, weighting_validator


class Weighting(SQLiteFactorStore, ImpactAssessmentDataStore):
    """
    LCIA weighting data - used to combine or compare different impact categories.

    The data schema for weighting is a one-element list:

    .. code-block:: python

            Schema(All(
                [uncertainty_dict],
                Length(min=1, max=1)
            ))

    The value is stored in the ``weightingfactor`` table of ``lci/databases.db``.

    """

    _metadata = weightings
    validator = weighting_validator
    matrix = "weighting_matrix"
    _factor_owner = "weighting"

    def _factor_model(self):
        from bw2data.backends.schema import WeightingFactor

        return WeightingFactor

    def _factor_rows(self, data) -> list[dict]:
        return [value_as_columns(value) for value in data]

    def _data_row(self, row: dict):
        return columns_as_value(row["amount"], row["uncertainty"])

    def write(self, data, process=True):
        """Because of DataStore assumptions, need a one-element list"""
        if not isinstance(data, list) or not len(data) == 1:
            raise ValueError("Weighting data must be one-element list")
        super().write(data, process=process)

    def process_row(self, row):
        """Return an empty tuple (as ``dtype_fields`` is empty), and the weighting uncertainty dictionary."""
        return {**as_uncertainty_dict(row), "row": 0}


class Normalization(SQLiteFactorStore, ImpactAssessmentDataStore):
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

    Flows are stored as activity ids, in the ``normalizationfactor`` table of
    ``lci/databases.db``; ``load()`` returns ``(flow id, amount)`` rows.

    """

    _metadata = normalizations
    validator = normalization_validator
    matrix = "normalization_matrix"
    _factor_owner = "normalization"

    def _factor_model(self):
        from bw2data.backends.schema import NormalizationFactor

        return NormalizationFactor

    def _factor_rows(self, data) -> list[dict]:
        rows = []
        for line in data:
            if not isinstance(line, (tuple, list)) or len(line) != 2:
                raise ValueError(f"A normalization row must be `(flow, amount)`, not {line!r}")
            rows.append({"flow": flow_id(line[0]), **value_as_columns(line[1])})
        return rows

    def _data_row(self, row: dict) -> tuple:
        return (row["flow"], columns_as_value(row["amount"], row["uncertainty"]))

    def process_row(self, row):
        """Given ``(flow, amount)``, return a dictionary for array insertion."""
        return {
            **as_uncertainty_dict(row[1]),
            "row": get_id(row[0]),
        }
