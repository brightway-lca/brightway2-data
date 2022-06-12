from . import config, geomapping, methods
from .backends.schema import get_id
from .utils import as_uncertainty_dict, get_geocollection


from . import Location
from ..data_store import ProcessedDataStore
from ..validate import ia_validator
from ..sqlite import TupleJSONField, JSONField
from peewee import DoesNotExist, fn, SchemaManager, Model, TextField, DateTimeField, BooleanField, ForeignKeyField, FloatField
from ..utils import abbreviate
import datetime


class Method(Model, ProcessedDataStore):
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

    validator = ia_validator
    matrix = "characterization_matrix"

    name = TupleJSONField(unique=True, null=False)
    filename = TextField(null=True)
    metadata = JSONField(default={})
    modified = DateTimeField(default=datetime.datetime.now)

    def save(self, *args, **kwargs):
        if not self.filename:
            self.filename = abbreviate(self.name)
        super().save(*args, **kwargs)

    def __str__(self):
        return "Brightway2 %s: %s" % (self.__class__.__name__, ": ".join(self.name))

    # def copy(self, name=None):

    # def register(self, **kwargs):

    def add_geomappings(self, data):
        Location.add_many({x[2] for x in data if len(x) == 3})

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

        third = lambda x: x[2] if len(x) == 3 else None

        geocollections = {
            get_geocollection(third(elem), default_global_location=True)
            for elem in data
        }
        if None in geocollections:
            print(
                "Not able to determine geocollections for all CFs. This method is not ready for regionalization."
            )
            geocollections.discard(None)

        self.metadata["geocollections"] = sorted(geocollections)
        self._metadata.flush()
        super(Method, self).write(data)

    def process(self, **extra_metadata):
        extra_metadata["global_index"] = geomapping[config.global_location]
        super().process(**extra_metadata)


class CharacterizationFactor(Model):
    method = ForeignKeyField(Method, backref='cfs')
    location = ForeignKeyField(Location, null=True, backref='cfs')
    uncertainty = JSONField(default={})
    value = FloatField(null=False)
