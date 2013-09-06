from .ia_data_store import ImpactAssessmentDataStore
from .meta import weightings, mapping, normalizations
from .utils import MAX_INT_32
from .validate import weighting_validator, normalization_validator
import numpy as np


class Weighting(ImpactAssessmentDataStore):
    metadata = weightings
    label = "weighting"

    def process(self):
        data = self.load()
        assert isinstance(data, dict)
        dtype = [
            ('uncertainty_type', np.uint8),
            ('amount', np.float32),
            ('loc', np.float32),
            ('scale', np.float32),
            ('shape', np.float32),
            ('minimum', np.float32),
            ('maximum', np.float32),
            ('negative', np.bool)
        ]
        corrected_data = (
            data.get("uncertainty type", 0),
            data["amount"],
            data.get("loc", np.NaN),
            data.get("scale", np.NaN),
            data.get("shape", np.NaN),
            data.get("minimum", np.NaN),
            data.get("maximum", np.NaN),
            data["amount"] < 0
        )
        self.write_processed_array(np.array(corrected_data, dtype=dtype))

    def validate(self, data):
        weighting_validator(data)
        return True


class Normalization(ImpactAssessmentDataStore):
    metadata = normalizations
    label = "normalization"

    def process(self):
        data = self.load()
        assert data
        dtype = [
            ('uncertainty_type', np.uint8),
            ('flow', np.uint32),
            ('index', np.uint32),
            ('amount', np.float32),
            ('loc', np.float32),
            ('scale', np.float32),
            ('shape', np.float32),
            ('minimum', np.float32),
            ('maximum', np.float32),
            ('negative', np.bool)
        ]
        array = np.zeros((len(data),), dtype=dtype)
        for index, row in enumerate(data):
            array[index] = (
                row.get("uncertainty type", 0),
                mapping[row['flow']],
                MAX_INT_32,
                row["amount"],
                row.get("loc", np.NaN),
                row.get("scale", np.NaN),
                row.get("shape", np.NaN),
                row.get("minimum", np.NaN),
                row.get("maximum", np.NaN),
                row["amount"] < 0
            )
        self.write_processed_array(array)

    def validate(self, data):
        normalization_validator(data)
        return True
