from . import projects
from .errors import UnknownObject, MissingIntermediateData
from .fatomic import open as atomic_open
from .project import writable_project
from bw_processing import create_datapackage, clean_datapackage_name, safe_filename
import pickle
from fs.zipfs import ZipFS


class DataStore(object):
    """
Base class for all Brightway2 data stores. Subclasses should define:

    * **metadata**: A :ref:`serialized-dict` instance, e.g. ``databases`` or ``methods``. The custom is that each type of data store has a new metadata store, so the data store ``Foo`` would have a metadata store ``foos``.
    * **validator**: A data validator. Optional. See bw2data.validate.

    """

    validator = None
    _metadata = None
    _intermediate_dir = "intermediate"

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "Brightway2 %s: %s" % (self.__class__.__name__, self.name)

    __repr__ = lambda self: str(self)

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
        """Remove filesystem-unsafe characters and perform unicode normalization on ``self.name`` using :func:`.filesystem.safe_filename`."""
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
            return pickle.load(
                open(projects.dir / "intermediate" / (self.filename + ".pickle"), "rb",)
            )
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
        try:
            from bw2io import BW2Package

            return BW2Package.export_obj(self)
        except ImportError:
            print("bw2io not installed")

    @writable_project
    def write(self, data):
        """Serialize intermediate data to disk.

        Args:
            * *data* (object): The data

        """
        self.register()
        filepath = projects.dir / self._intermediate_dir / (self.filename + ".pickle")
        with atomic_open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=4)

    def validate(self, data):
        """Validate data. Must be called manually."""
        self.validator(data)
        return True


class ProcessedDataStore(DataStore):
    """
Brightway2 data stores that can be processed to NumPy arrays.

In addition to ``metadata`` and (optionally) ``validator``, subclasses should override ``add_geomappings``. This method takes the entire dataset, and loads objects to :ref:`geomapping` as needed.

    """

    matrix = "unknown"

    def dirpath_processed(self):
        return projects.dir / "processed"

    def filename_processed(self):
        return clean_datapackage_name(self.filename + ".zip")

    def filepath_processed(self):
        return self.dirpath_processed() / self.filename_processed()

    @writable_project
    def write(self, data, process=True):
        """Serialize intermediate data to disk.

        Args:
            * *data* (object): The data

        """
        self.register()
        self.add_geomappings(data)
        filepath = projects.dir / self._intermediate_dir / (self.filename + ".pickle")
        with atomic_open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=4)
        if process:
            self.process()

    def process_row(self, row):
        """Translate data into a dictionary suitable for array inputs.

        See `bw_processing documentation <https://github.com/brightway-lca/bw_processing>`__."""
        raise NotImplementedError

    def process(self, **extra_metadata):
        """
Process intermediate data from a Python dictionary to a `stats_arrays <https://pypi.python.org/pypi/stats_arrays/>`_ array, which is a `NumPy <http://numpy.scipy.org/>`_ `Structured <http://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html#numpy.recarray>`_ `Array <http://docs.scipy.org/doc/numpy/user/basics.rec.html>`_. A structured array (also called record array) is a heterogeneous array, where each column has a different label and data type.

Processed arrays are saved in the ``processed`` directory.

If the uncertainty type is no uncertainty, undefined, or not specified, then the 'amount' value is used for 'loc' as well. This is needed for the random number generator.

Doesn't return anything, but writes a file to disk.

        """
        data = self.load()
        dp = create_datapackage(
            fs=ZipFS(str(self.filepath_processed()), write=True),
            name=self.filename_processed(),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )
        dp.add_persistent_vector_from_iterator(
            matrix=self.matrix,
            name=clean_datapackage_name(str(self.name) + " matrix data"),
            dict_iterator=(self.process_row(row) for row in data),
            nrows=len(data),
            **extra_metadata
        )
        dp.finalize_serialization()

    def add_geomappings(self, data):
        """Add objects to ``geomapping``, if necessary.

        Args:
            * *data* (object): The data

        """
        return

    def validate(self, data):
        """Validate data. Must be called manually."""
        self.validator(data)
        return True
