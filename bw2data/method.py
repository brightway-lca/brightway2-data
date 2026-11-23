from bw2data import config, geomapping, methods
from bw2data.backends.schema import get_id
from bw2data.errors import UnknownObject
from bw2data.ia_data_store import (
    ImpactAssessmentDataStore,
    SQLiteFactorStore,
    columns_as_value,
    flow_id,
    value_as_columns,
)
from bw2data.utils import as_uncertainty_dict, get_geocollection, get_node
from bw2data.validate import ia_validator


def _location(row):
    """Location of a CF row ``(flow, amount, maybe location)``; a missing or empty location
    means ``config.global_location``."""
    return row[2] if len(row) >= 3 and row[2] else config.global_location


class Method(SQLiteFactorStore, ImpactAssessmentDataStore):
    """A manager for an impact assessment method. This class can register or deregister methods, write characterization factors, process data to parameter arrays, validate, and copy methods.

    The Method class never holds characterization factors, but it can load or write them; they are stored in the ``characterizationfactor`` table of the per-project ``lci/databases.db``. The only attribute is *name*, which is the name of the method being managed.

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
    _factor_owner = "method"

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
        # A missing or empty location means the global location
        geomapping.add({_location(x) for x in data if len(x) == 3 and x[2]})

    def process_row(self, row):
        """Given ``(flow, amount, maybe location)``, return a dictionary for array insertion."""
        try:
            return {
                **as_uncertainty_dict(row[1]),
                "row": get_id(row[0]),
                "col": geomapping[_location(row)],
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
            if _location(row) == config.global_location:
                raise UnknownObject(
                    "Can't find default global location! It's supposed to be `{}`, but this isn't in the `geomapping`".format(
                        config.global_location
                    )
                )
            raise UnknownObject(
                "Can't find location `{}`, specified in CF row `{}` for method `{}`".format(
                    {row[2]}, row, self.name
                )
            )

    def _factor_model(self):
        from bw2data.backends.schema import CharacterizationFactor

        return CharacterizationFactor

    @staticmethod
    def _location_columns(row) -> tuple:
        """``(geocollection, location)`` columns for the location of a CF row."""
        if len(row) < 3 or row[2] is None or row[2] == "":
            return None, None
        location = row[2]
        if isinstance(location, str):
            return None, location
        if (
            isinstance(location, (tuple, list))
            and len(location) == 2
            and all(isinstance(x, str) and x for x in location)
        ):
            return tuple(location)
        raise ValueError(
            f"Invalid location {location!r} in CF row {row!r}: must be a location string, or "
            "a `(geocollection, location)` tuple of two strings"
        )

    @staticmethod
    def _check_row(line) -> None:
        if not isinstance(line, (tuple, list)) or len(line) not in (2, 3):
            raise ValueError(
                f"A CF row must be `(flow, amount)` or `(flow, amount, location)`, not {line!r}"
            )

    def _factor_rows(self, data) -> list[dict]:
        rows = []
        for line in data:
            self._check_row(line)
            geocollection, location = self._location_columns(line)
            rows.append(
                {
                    "flow": flow_id(line[0]),
                    **value_as_columns(line[1]),
                    "geocollection": geocollection,
                    "location": location,
                }
            )
        return rows

    def _data_row(self, row: dict) -> tuple:
        value = columns_as_value(row["amount"], row["uncertainty"])
        if row["location"] is None:
            return (row["flow"], value)
        if row["geocollection"] is None:
            return (row["flow"], value, row["location"])
        return (row["flow"], value, (row["geocollection"], row["location"]))

    def write(self, data, process=True):
        """Replace the characterization factors of this method, registering it first if needed.

        Each row is ``(flow, amount)`` or ``(flow, amount, location)``. ``flow`` is an activity
        id, an ``Activity`` or a ``(database, code)`` key; it is stored as an id. ``amount``
        is a number or an uncertainty dictionary. ``location`` is a location string, a
        ``(geocollection, location)`` tuple, or missing (or ``None``) for the global
        location. Rows are read back by ``load()`` as tuples, without a missing location.

        Sets the metadata keys ``num_cfs`` and ``geocollections``."""
        def normalize(line):
            # Checked here, and not only in ``_factor_rows``, as the rows are used below
            self._check_row(line)
            self._location_columns(line)
            line = (flow_id(line[0]), *line[1:])
            if len(line) == 3 and isinstance(line[2], list):
                line = (*line[:2], tuple(line[2]))
            return line

        data = [normalize(line) for line in data]
        third = lambda x: x[2] if len(x) == 3 else None
        geocollections = {
            get_geocollection(third(elem), default_global_location=True) for elem in data
        }
        geocollections.discard(None)
        super().write(
            data,
            process=process,
            metadata={"num_cfs": len(data), "geocollections": sorted(geocollections)},
        )

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
