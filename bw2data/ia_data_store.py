import hashlib
import numbers
import string

import numpy as np
from bw_processing import safe_filename

from bw2data.data_store import ProcessedDataStore
from bw2data.errors import MissingIntermediateData, UnknownObject


def abbreviate(names, length=8):
    """Take a tuple or list, and construct a string, doing the following:

    First, apply :func:`.filesystem.safe_filename` to each element in ``names``.

    Next, take the following, in order:
        - The first word of the first element in names, lower-cased, where word is defined as everything up to the first empty space character.
        - Join the rest of the first element (i.e. after the first word) with all other elements. Use the empty space character to join.
        - In this long string separated by spaces, take the lowercase first character of each word. Add the first word to this new string.
        - Finally, add a dash, and then the MD5 hash of the entire identifier, where each element is joined by a dash character.

    ``('ReCiPe Endpoint (E,A)', 'human health', 'ionising radiation')`` becomes ``'recipee(hhir-70eeef20a20deb6347ad428e3f6c5f3c'``.

    The MD5 hash is needed because taking the first characters doesn't guarantee unique strings.

    """
    safe_names = [safe_filename(x, False) for x in names]
    abbrev = lambda x: x if x[0] in string.digits else x[0].lower()
    name = " ".join(safe_names).split(" ")[0].lower() + "".join(
        [abbrev(x) for x in " ".join(safe_names).split(" ")[1:]]
    )
    return name + "." + str(hashlib.md5(("-".join(names)).encode("utf-8")).hexdigest())


class ImpactAssessmentDataStore(ProcessedDataStore):
    """
    A subclass of ``DataStore`` for impact assessment methods.

    IA objects are hierarchically structured, and their identifier uses this structure, like ``('ecological scarcity 2006', 'total', 'natural resources')``. The identifier must be a ``tuple``, i.e. ``()``, not a ``list``, i.e. ``[]``. The identifier should only contain unicode strings, and can be of any length >= 1.

    Because impact assessment methods are identified by a tuple of strings, e.g. ``('ReCiPe Endpoint (E,A)', 'human health', 'ionising radiation')``, we need to transform this identifier before it can be used e.g. as a filename. We do this using the :func:`.abbreviate` function, which returns a single unicode string.

    Args:
        - *name* (tuple): Name of the IA object to manage. Must be a tuple of unicode strings.

    """

    def __str__(self):
        return "Brightway2 %s: %s" % (self.__class__.__name__, ": ".join(self.name))

    def get_abbreviation(self):
        """Retrieve the abbreviation of the method identifier from the metadata store. See class documentation."""
        self.register()
        # Objects added directly to the metadata store (``methods[name] = {...}``) have none
        return self.metadata.get("abbreviation") or abbreviate(self.name)

    def copy(self, name=None):
        """Make a copy of the method, including its CFs and metadata.

        If ``name`` is not provided, add "Copy of" to the last element of the original name, e.g. ``("foo", "bar")`` becomes ``("foo", "Copy of bar")``

        Args:
            - *name* (tuple, optional): Name of the new method.

        Returns:
            The new object.

        """
        if name is None:
            name = self.name[:-1] + ("Copy of " + self.name[-1],)
        else:
            name = tuple(name)

        return super(ImpactAssessmentDataStore, self).copy(name)

    def register(self, **kwargs):
        """Register an object with the metadata store.

        The metadata key ``abbreviation`` is set automatically.

        Objects must be registered before data can be written. If this object is not yet registered in the metadata store, a warning is written to **stdout**.

        Takes any number of keyword arguments.

        """
        kwargs["abbreviation"] = abbreviate(self.name)
        super(ImpactAssessmentDataStore, self).register(**kwargs)

    @property
    def filename(self):
        """
        Returns the abbreviated identifier of the method, used as the filename.

        Returns
        -------
        str
            The abbreviated identifier of the method.
        """
        return self.get_abbreviation()

    def process(self, **extra_metadata):
        """
        Processes the impact assessment method and updates the metadata, including
        the identifier.

        Parameters
        ----------
        extra_metadata
            Additional metadata to be included during processing.

        Notes
        -----
        The method updates the metadata with the list form of the method's identifier.
        """
        extra_metadata["identifier"] = list(self.name)
        super().process(**extra_metadata)


def _plain(value):
    """``value`` with NumPy scalars converted to Python numbers, so it can be stored as JSON."""
    return value.item() if isinstance(value, np.generic) else value


def _amount(amount) -> float:
    """``amount`` as stored in the ``amount`` column of a factor table. Raises ``ValueError``
    if it isn't a real number, or is NaN: SQLite stores NaN as ``NULL``, which would be read
    back as a missing amount."""
    if not isinstance(amount, numbers.Number) or isinstance(amount, bool):
        raise ValueError(f"Amount must be a number, not {amount!r}")
    try:
        result = float(amount)
    except TypeError:
        raise ValueError(f"Amount must be a real number, not {amount!r}") from None
    if result != result:
        raise ValueError("Amount can't be NaN (not a number)")
    return result


def value_as_columns(value) -> dict:
    """Split a factor value (a number, or an uncertainty dictionary) into the ``amount`` and
    ``uncertainty`` columns of a factor table. Raises ``ValueError`` for anything else, and
    for an amount which is NaN."""
    if isinstance(value, dict):
        amount = value.get("amount")
        return {
            "amount": None if amount is None else _amount(amount),
            "uncertainty": {k: _plain(v) for k, v in value.items() if k != "amount"},
        }
    if isinstance(value, numbers.Number) and not isinstance(value, bool):
        return {"amount": _amount(value), "uncertainty": None}
    raise ValueError(f"A value must be a number or an uncertainty dictionary, not {value!r}")


def columns_as_value(amount, uncertainty):
    """Inverse of :func:`value_as_columns`."""
    if uncertainty is None:
        return amount
    if amount is None:
        return dict(uncertainty)
    return {"amount": amount, **uncertainty}


def flow_id(flow) -> int:
    """Activity id of ``flow``: an id, an ``Activity``, or a ``(database, code)`` key."""
    from bw2data.backends.proxies import Activity
    from bw2data.utils import get_node

    if isinstance(flow, Activity):
        return flow.id
    elif isinstance(flow, numbers.Integral) and not isinstance(flow, bool):
        # Also NumPy integers
        return int(flow)
    elif isinstance(flow, (tuple, list)) and len(flow) == 2:
        return get_node(key=tuple(flow)).id
    raise ValueError(f"Can't understand flow identifier {flow!r}")


class SQLiteFactorStore:
    """Mixin for ``ImpactAssessmentDataStore`` subclasses whose data is stored in a factor
    table in the per-project ``lci/databases.db`` (one row per factor, referring to the
    object's row in the registry table), instead of in ``intermediate/*.pickle``.

    Subclasses implement ``_factor_model``, ``_factor_rows`` (data to rows, raising
    ``ValueError`` before anything is written) and ``_data_row`` (a row back to data), and set
    ``_factor_owner``, the name of the factor model's foreign key field.

    ``write`` replaces all factors, sets ``has_data`` and saves pending metadata changes in
    one transaction."""

    _factor_owner = None

    def _factor_model(self):
        raise NotImplementedError

    def _factor_rows(self, data) -> list[dict]:
        raise NotImplementedError

    def _data_row(self, row: dict):
        raise NotImplementedError

    def _record(self):
        record = self._metadata._get_row(self.name)
        if record is None:
            raise UnknownObject("This object is not registered and has no data")
        return record

    def load(self):
        """Load the data of this object from SQLite.

        Raises ``UnknownObject`` if the object isn't registered, and
        ``MissingIntermediateData`` if no data was written for it."""
        record = self._record()
        if not record.has_data:
            raise MissingIntermediateData("No data has been written for this object")
        model = self._factor_model()
        owner = getattr(model, self._factor_owner)
        columns = [
            field for name, field in model._meta.fields.items() if name != self._factor_owner
        ]
        query = model.select(*columns).where(owner == record.id).order_by(model.position)
        return [self._data_row(row) for row in query.dicts()]

    def write(self, data, process=True, metadata=None):
        """Replace the data of this object, registering it first if needed.

        ``metadata`` is merged into the object's metadata, saved in the same transaction as
        the data. Raises ``ValueError`` (writing nothing) if the data can't be stored."""
        from bw2data.backends.schema import _insert_many
        from bw2data.meta import _write_transaction

        rows = self._factor_rows(data)
        self.register()
        self.add_geomappings(data)

        model = self._factor_model()
        owner = getattr(model, self._factor_owner)
        record_model = self._metadata._model()
        proxy = self.metadata
        try:
            with _write_transaction(record_model):
                record = self._record()
                model.delete().where(owner == record.id).execute()
                for position, row in enumerate(rows):
                    row["position"] = position
                    row[self._factor_owner] = record.id
                _insert_many(model, rows)
                record_model.update(has_data=True).where(record_model.id == record.id).execute()
                for key, value in (metadata or {}).items():
                    proxy[key] = value
                proxy.save()
        except BaseException:
            # Forget unsaved metadata changes, which are part of the failed write
            self._metadata._proxies.pop(self._metadata._key(self.name), None)
            raise
        if process:
            self.process()
