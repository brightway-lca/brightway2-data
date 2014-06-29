# -*- coding: utf-8 -*
from . import config
from .errors import UnknownObject, MissingIntermediateData
from .utils import safe_filename
import numpy as np
import os
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle


class DataStore(object):
    """
Base class for all Brightway2 data stores. Subclasses should define:

    * **metadata**: A :ref:`serialized-dict` instance, e.g. ``databases`` or ``methods``. The custom is that each type of data store has a new metadata store, so the data store ``Foo`` would have a metadata store ``foos``.
    * **dtype_fields**: A list of fields to construct a NumPy structured array, e.g. ``[('foo', np.int), ('bar', np.float)]``. Fields names **must** be bytestrings, not unicode (i.e. ``"foo"`` instead of ``u"foo"``). Uncertainty fields (``base_uncertainty_fields``) are added automatically.
    * **validator**: A data validator. Optional. See bw2data.validate.

In order to use ``dtype_fields``, subclasses should override the method ``process_data``. This function takes rows of data, and returns the correct values for the custom dtype fields (as a tuple), **and** the ``amount`` field with its associated uncertainty. This second part is a little flexible - if there is no uncertainty, a number can be returned; otherwise, an uncertainty dictionary should be returned.

Subclasses should also override ``add_mappings``. This method takes the entire dataset, and loads objects to :ref:`mapping` or :ref:`geomapping` as needed.

    """
    validator = None
    metadata = None
    dtype_fields = None
    # Numpy columns names can't be unicode
    base_uncertainty_fields = [
        ('uncertainty_type', np.uint8),
        ('amount', np.float32),
        ('loc', np.float32),
        ('scale', np.float32),
        ('shape', np.float32),
        ('minimum', np.float32),
        ('maximum', np.float32),
        ('negative', np.bool),
    ]

    def __init__(self, name):
        self.name = name
        if self.name not in self.metadata and not \
                getattr(config, "dont_warn", False):
            warnings.warn(u"\n\t%s is not registered" % self, UserWarning)

    def __unicode__(self):
        return u"Brightway2 %s: %s" % (self.__class__.__name__, self.name)

    def __str__(self):
        return unicode(self).encode('utf-8')

    @property
    def filename(self):
        """Can be overwritten in cases where the filename is not the name"""
        return safe_filename(self.name)

    def register(self, **kwargs):
        """Register an object with the metadata store.

        Objects must be registered before data can be written. If this object is not yet registered in the metadata store, a warning is written to **stdout**.

        Takes any number of keyword arguments.

        """
        assert self.name not in self.metadata, u"%s is already registered" % self
        self.metadata[self.name] = kwargs

    def deregister(self):
        """Remove an object from the metadata store. Does not delete any files."""
        del self.metadata[self.name]

    def assert_registered(self):
        """Raise ``UnknownObject`` if not yet registered"""
        if self.name not in self.metadata:
            raise UnknownObject(u"%s is not yet registered" % self)

    def load(self):
        """Load the intermediate data for this object.

        Returns:
            The intermediate data.

        """
        self.assert_registered()
        try:
            return pickle.load(open(os.path.join(
                config.dir,
                u"intermediate",
                self.filename + u".pickle"
            ), "rb"))
        except OSError:
            raise MissingIntermediateData(u"Can't load intermediate data")

    @property
    def dtype(self):
        """Get custom dtype fields plus generic uncertainty fields"""
        return self.dtype_fields + self.base_uncertainty_fields

    def copy(self, name):
        """Make a copy of this object. Takes new name as argument. Returns the new object."""
        assert name not in self.metadata, u"%s already exists" % name
        new_obj = self.__class__(name)
        new_obj.register(**self.metadata[self.name])
        new_obj.write(self.load())
        new_obj.process()
        return new_obj

    def backup(self):
        """Save a backup to ``backups`` folder.

        Returns:
            File path of backup.

        """
        from .io import BW2Package
        return BW2Package.export_obj(self)

    def write(self, data):
        """Serialize intermediate data to disk.

        Args:
            * *data* (object): The data

        """
        self.assert_registered()
        self.add_mappings(data)
        filepath = os.path.join(
            config.dir,
            u"intermediate",
            self.filename + u".pickle"
        )
        with open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

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
                uncertainties.get(u"uncertainty type", 0),
                uncertainties[u"amount"],
                uncertainties[u"amount"] \
                    if uncertainties.get(u"uncertainty type", 0) in (0, 1) \
                    else uncertainties.get(u"loc", np.NaN),
                uncertainties.get(u"scale", np.NaN),
                uncertainties.get(u"shape", np.NaN),
                uncertainties.get(u"minimum", np.NaN),
                uncertainties.get(u"maximum", np.NaN),
                uncertainties.get(u"amount" < 0),
            )
        filepath = os.path.join(
            config.dir,
            u"processed",
            self.filename + u".pickle"
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
        """Validate data. Must be called manually.

        Need some metaprogramming because class methods have `self` injected automatically."""
        self.validator(data)
        return True
