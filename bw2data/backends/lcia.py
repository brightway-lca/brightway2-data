import warnings

from . import config
from .backends.schema import get_id
from .utils import as_uncertainty_dict, get_geocollection
from .schema import Location
from ..data_store import ProcessedDataStore
from ..sqlite import TupleJSONField, JSONField
from peewee import DoesNotExist, Model, TextField
from ..utils import abbreviate
from ..errors import UnknownObject


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
    matrix = "characterization_matrix"

    name = TupleJSONField(unique=True, null=False)
    filename = TextField(null=True)
    metadata = JSONField(default={})

    def save(self, *args, **kwargs):
        if not self.filename:
            self.filename = abbreviate(self.name)
        super().save(*args, **kwargs)

    def __str__(self):
        return "Brightway2 %s: %s" % (self.__class__.__name__, ": ".join(self.name))

    @classmethod
    def __contains__(cls, key):
        return bool(cls.select().where(cls.name == key).count())

    @classmethod
    def __len__(cls):
        return cls.select().count()

    @classmethod
    def __getitem__(cls, key):
        warnings.warn("Compatibility shim for `methods.__getitem__()`. Deprecated, use `Method.get(name=something)` instead.", DeprecationWarning)
        try:
            return cls.get(name=key)
        except DoesNotExist:
            raise KeyError

    # def copy(self, name=None):

    def register(self, **kwargs):
        warnings.warn("`.register()` is deprecated, please set `.metadata` directly", DeprecationWarning)
        self.metadata.update(**kwargs)
        self.save()

    def add_geomappings(self, data):
        Location.add_many({x[2] for x in data if len(x) == 3})

    def process_row(self, row):
        """Given ``(flow, amount, maybe location)``, return a dictionary for array insertion."""
        try:
            return {
                **as_uncertainty_dict(row[1]),
                "row": get_id(row[0]),
                "col": (
                    Location.from_key(row[2]).id
                    if len(row) >= 3
                    else Location.from_key(config.global_location).id
                ),
            }
        except UnknownObject:
            raise UnknownObject("Can't find flow `{}`, specified in CF row `{}` for method `{}`".format({row[0]}, row, self.name))
        except KeyError:
            if len(row) >= 3 and row[2] not in Location:
                raise UnknownObject("Can't find location `{}`, specified in CF row `{}` for method `{}`".format({row[2]}, row, self.name))
            elif config.global_location not in Location:
                raise UnknownObject("Can't find default global location! It's supposed to be `{}`, but this isn't in `Location`".format(config.global_location))

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
        try:
            extra_metadata["global_index"] = Location.from_key(config.global_location).id
        except KeyError:
            raise KeyError("Can't find default global location! It's supposed to be `{}`, defined in `config`, but this isn't in `Location`".format(config.global_location))
        super().process(**extra_metadata)

    # Backwards compatibility
    @classmethod
    def flush(cls):
        warnings.warn("Compatibility shim for `methods.flush()`. Deprecated no-op.", DeprecationWarning)

    @classmethod
    def list(cls):
        warnings.warn("Compatibility shim for `methods.list()`. Deprecated, use `Method.select()` instead.", DeprecationWarning)
        return sorted(obj.name for obj in cls)
