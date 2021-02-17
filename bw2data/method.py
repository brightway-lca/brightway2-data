from . import methods, geomapping, config
from .ia_data_store import ImpactAssessmentDataStore
from .utils import as_uncertainty_dict
from .validate import ia_validator
from .backends.schema import get_id


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
        * *valid_tuple* (tuple): A dataset identifier, like ``("biosphere", "CO2")``.
        * *maybe_uncertainty* (uncertainty dict or number): Either a number or an uncertainty dictionary.
        * *object* (object, optional) is a location identifier, used only for regionalized LCIA.

    Args:
        * *name* (tuple): Name of impact assessment method to manage.

    """

    _metadata = methods
    validator = ia_validator
    matrix = "characterization_matrix"

    def add_geomappings(self, data):
        geomapping.add({x[2] for x in data if len(x) == 3})

    def process_row(self, row):
        """Given ``(flow, amount, maybe location)``, return a dictionary for array insertion."""
        return {
            **as_uncertainty_dict(row[1]),
            "row": get_id(row[0]),
            "col": (
                geomapping[row[2]]
                if len(row) >= 3
                else geomapping[config.global_location]
            ),
        }

    def write(self, data, process=True):
        """Serialize intermediate data to disk.

        Sets the metadata key ``num_cfs`` automatically."""
        if self.name not in self._metadata:
            self.register()
        self.metadata["num_cfs"] = len(data)
        self._metadata.flush()
        super(Method, self).write(data)

    def process(self, **extra_metadata):
        extra_metadata["global_index"] = geomapping[config.global_location]
        super().process(**extra_metadata)
