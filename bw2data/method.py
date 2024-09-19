from typing import Iterable

from bw2data import config, geomapping, methods
from bw2data.backends.proxies import Activity
from bw2data.backends.schema import get_id
from bw2data.errors import UnknownObject
from bw2data.ia_data_store import ImpactAssessmentDataStore
from bw2data.utils import as_uncertainty_dict, get_geocollection, get_node
from bw2data.validate import ia_validator


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
        - *valid_tuple* (tuple): A dataset identifier, like ``("biosphere", "CO2")``.
        - *maybe_uncertainty* (uncertainty dict or number): Either a number or an uncertainty dictionary.
        - *object* (object, optional) is a location identifier, used only for regionalized LCIA.

    Args:
        * *name* (tuple): Name of impact assessment method to manage.

    """

    _metadata = methods
    validator = ia_validator
    matrix = "characterization_matrix"

    def __iter__(self):
        """Iterate over characterization factors and return `Node` instances with CFs and geo ids"""
        data = self.load()
        for line in data:
            if isinstance(line[0], tuple):
                node = get_node(key=line[0])
                yield (node, *line[1:])
            elif isinstance(line[0], int):
                node = get_node(id=line[0])
                yield (node, *line[1:])
            else:
                # Our `.write()` function won't allow this, but our users are creative
                raise ValueError(
                    f"Can't understand elementary flow identifier {line[0]} in line {line}"
                )

    def add_geomappings(self, data):
        geomapping.add({x[2] for x in data if len(x) == 3})

    def process_row(self, row):
        """Given ``(flow, amount, maybe location)``, return a dictionary for array insertion."""
        try:
            return {
                **as_uncertainty_dict(row[1]),
                "row": get_id(row[0]),
                "col": (
                    geomapping[row[2]] if len(row) >= 3 else geomapping[config.global_location]
                ),
            }
        except UnknownObject:
            raise UnknownObject(
                "Can't find flow `{}`, specified in CF row `{}` for method `{}`".format(
                    {tuple(row[0]) if isinstance(row[0], list) else row[0]},
                    row,
                    self.name,
                )
            )
        except KeyError:
            if len(row) >= 3 and row[2] not in geomapping:
                raise UnknownObject(
                    "Can't find location `{}`, specified in CF row `{}` for method `{}`".format(
                        {row[2]}, row, self.name
                    )
                )
            elif config.global_location not in geomapping:
                raise UnknownObject(
                    "Can't find default global location! It's supposed to be `{}`, but this isn't in the `geomapping`".format(
                        config.global_location
                    )
                )

    def write(self, data, process=True):
        """Serialize intermediate data to disk.

        Sets the metadata key ``num_cfs`` automatically."""
        if self.name not in self._metadata:
            self.register()
        self.metadata["num_cfs"] = len(data)

        def normalize_ids(line: Iterable) -> tuple:
            if isinstance(line[0], Activity):
                return (line[0].id, *line[1:])
            elif isinstance(line[0], tuple):
                return (get_node(key=line[0]).id, *line[1:])
            elif not isinstance(line[0], int):
                raise ValueError(
                    f"Can't understand elementary flow identifier {line[0]} in data line {line}"
                )
            else:
                return tuple(line)

        data = [normalize_ids(line) for line in data]
        third = lambda x: x[2] if len(x) == 3 else None

        geocollections = {
            get_geocollection(third(elem), default_global_location=True) for elem in data
        }
        if None in geocollections:
            geocollections.discard(None)

        self.metadata["geocollections"] = sorted(geocollections)
        super(Method, self).write(data, process=process)

    def process(self, **extra_metadata):
        try:
            extra_metadata["global_index"] = geomapping[config.global_location]
        except KeyError:
            raise KeyError(
                "Can't find default global location! It's supposed to be `{}`, defined in `config`, but this isn't in the `geomapping`".format(
                    config.global_location
                )
            )
        super().process(**extra_metadata)
