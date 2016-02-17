# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import config, projects
from .errors import UnknownObject, MissingIntermediateData
from .fatomic import open as atomic_open
from .project import writable_project
from .utils import safe_filename, numpy_string
from future.utils import python_2_unicode_compatible
import numpy as np
import os
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle

@python_2_unicode_compatible
class DataStore(object):
    """
Base class for all Brightway2 data stores. Subclasses should define:

    * **metadata**: A :ref:`serialized-dict` instance, e.g. ``databases`` or ``methods``. The custom is that each type of data store has a new metadata store, so the data store ``Foo`` would have a metadata store ``foos``.
    * **validator**: A data validator. Optional. See bw2data.validate.

    """
    validator = None
    _metadata = None
    _intermediate_dir = u'intermediate'

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "Brightway2 %s: %s" % (self.__class__.__name__, self.name)

    __repr__ = lambda x: str(x)

    def _get_metadata(self):
        if self.name not in self._metadata:
            raise UnknownObject(
                "This object is not yet registered; can't get or set metadata"
            )
        return self._metadata[self.name]

    @writable_project
    def _set_metadata(self, value):
        self._get_metadata()
        self._metadata[self.name] = value
        self._metadata.flush()

    metadata = property(_get_metadata, _set_metadata)

    @property
    def filename(self):
        """Remove filesystem-unsafe characters and perform unicode normalization on ``self.name`` using :func:`.utils.safe_filename`."""
        return safe_filename(self.name)

    @property
    def registered(self):
        return self.name in self._metadata

    def register(self, **kwargs):
        """Register an object with the metadata store. Takes any number of keyword arguments."""
        @writable_project
        def _register(kwargs):
            self._metadata[self.name] = kwargs

        if self.name not in self._metadata:
            _register(kwargs)

    @writable_project
    def deregister(self):
        """Remove an object from the metadata store. Does not delete any files."""
        del self._metadata[self.name]

    def load(self):
        """Load the intermediate data for this object.

        Returns:
            The intermediate data.

        """
        if not self.registered:
            raise UnknownObject("This object is not registered and has no data")
        try:
            return pickle.load(open(os.path.join(
                projects.dir,
                "intermediate",
                self.filename + ".pickle"
            ), "rb"))
        except OSError:
            raise MissingIntermediateData("Can't load intermediate data")

    @writable_project
    def copy(self, name):
        """Make a copy of this object with a new ``name``.

        This method only changes the name, but not any of the data or metadata.

        Args:
            * *name* (object): Name of the new object.

        Returns:
            The new object.

        """
        assert name not in self._metadata, "%s already exists" % name
        new_obj = self.__class__(name)
        new_obj.register(**self.metadata)
        new_obj.write(self.load())
        return new_obj

    def backup(self):
        """Save a backup to ``backups`` folder.

        Returns:
            File path of backup.

        """
        from bw2io import BW2Package
        return BW2Package.export_obj(self)

    @writable_project
    def write(self, data):
        """Serialize intermediate data to disk.

        Args:
            * *data* (object): The data

        """
        self.register()
        filepath = os.path.join(
            projects.dir,
            self._intermediate_dir,
            self.filename + ".pickle"
        )
        with atomic_open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    def validate(self, data):
        """Validate data. Must be called manually."""
        self.validator(data)
        return True


class ProcessedDataStore(DataStore):
    """
Brightway2 data stores that can be processed to NumPy arrays. In addition to ``metadata`` and (optionally) ``validator``, subclasses should define:

    * **dtype_fields**: A list of fields to construct a NumPy structured array, e.g. ``[('foo', np.int), ('bar', np.float)]``. Fields names **must** be bytestrings, not unicode (i.e. ``b"foo"`` instead of ``"foo"``). Uncertainty fields (``base_uncertainty_fields``) are added automatically.

In order to use ``dtype_fields``, subclasses should override the method ``process_data``. This function takes rows of data, and returns the correct values for the custom dtype fields (as a tuple), **and** the ``amount`` field with its associated uncertainty. This second part is a little flexible - if there is no uncertainty, a number can be returned; otherwise, an uncertainty dictionary should be returned.

Subclasses should also override ``add_mappings``. This method takes the entire dataset, and loads objects to :ref:`mapping` or :ref:`geomapping` as needed.

    """
    dtype_fields = None
    # Numpy columns names can't be unicode
    base_uncertainty_fields = [
        (numpy_string('uncertainty_type'), np.uint8),
        (numpy_string('amount'), np.float32),
        (numpy_string('loc'), np.float32),
        (numpy_string('scale'), np.float32),
        (numpy_string('shape'), np.float32),
        (numpy_string('minimum'), np.float32),
        (numpy_string('maximum'), np.float32),
        (numpy_string('negative'), np.bool),
    ]

    @property
    def dtype(self):
        """Returns both the generic ``base_uncertainty_fields`` plus class-specific ``dtype_fields``. ``dtype`` determines the columns of the :ref:`processed array <processing-data>`."""
        return self.dtype_fields + self.base_uncertainty_fields

    def filepath_processed(self):
        return os.path.join(
            projects.dir,
            "processed",
            self.filename + ".pickle"
        )

    @writable_project
    def write(self, data, process=True):
        """Serialize intermediate data to disk.

        Args:
            * *data* (object): The data

        """
        self.register()
        self.add_mappings(data)
        filepath = os.path.join(
            projects.dir,
            self._intermediate_dir,
            self.filename + ".pickle"
        )
        with atomic_open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        if process:
            self.process()

    def process_data(self, row):
        """Translate data into correct order"""
        raise NotImplementedError

    def process(self):
        """
Process intermediate data from a Python dictionary to a `stats_arrays <https://pypi.python.org/pypi/stats_arrays/>`_ array, which is a `NumPy <http://numpy.scipy.org/>`_ `Structured <http://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html#numpy.recarray>`_ `Array <http://docs.scipy.org/doc/numpy/user/basics.rec.html>`_. A structured array (also called record array) is a heterogeneous array, where each column has a different label and data type.

Processed arrays are saved in the ``processed`` directory.

Uses ``pickle`` instead of the native NumPy ``.tofile()``. Although pickle is ~2 times slower, this difference in speed has no practical effect (e.g. one twentieth of a second slower for ecoinvent 2.2), and the numpy ``fromfile`` and ``tofile`` functions don't preserve the datatype of structured arrays.

If the uncertainty type is no uncertainty, undefined, or not specified, then the 'amount' value is used for 'loc' as well. This is needed for the random number generator.

Doesn't return anything, but writes a file to disk.

        """
        data = self.load()
        arr = np.zeros((len(data),), dtype=self.dtype)

        for index, row in enumerate(data):
            values, number = self.process_data(row)
            uncertainties = self.as_uncertainty_dict(number)
            assert len(values) == len(self.dtype_fields)
            assert u'amount' in uncertainties, "Must provide at least `amount` field in `uncertainties`"
            arr[index] = values + (
                uncertainties.get("uncertainty type", 0),
                uncertainties["amount"],
                uncertainties["amount"] \
                    if uncertainties.get("uncertainty type", 0) in (0, 1) \
                    else uncertainties.get("loc", np.NaN),
                uncertainties.get("scale", np.NaN),
                uncertainties.get("shape", np.NaN),
                uncertainties.get("minimum", np.NaN),
                uncertainties.get("maximum", np.NaN),
                uncertainties.get("amount") < 0,
            )
        filepath = os.path.join(
            projects.dir,
            "processed",
            self.filename + ".pickle"
        )
        with open(filepath, "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

    def as_uncertainty_dict(self, value):
        """Convert floats to ``stats_arrays`` uncertainty dict, if necessary"""
        if isinstance(value, dict):
            return value
        try:
            return {'amount': float(value)}
        except:
            raise TypeError(
                "Value must be either an uncertainty dict. or number"
                " (got %s: %s)" % (type(value), value)
            )

    def add_mappings(self, data):
        """Add objects to ``mapping`` or ``geomapping``, if necessary.

        Args:
            * *data* (object): The data

        """
        return

    def validate(self, data):
        """Validate data. Must be called manually."""
        self.validator(data)
        return True
