import copy

from bw_processing import clean_datapackage_name, create_datapackage, safe_filename, load_datapackage
from fs.zipfs import ZipFS

from . import projects


class ProcessedDataStore:
    """
    Brightway2 data stores that can be processed to NumPy arrays.

    In addition to ``metadata`` and (optionally) ``validator``. This method takes the entire dataset, and loads objects to :ref:`geomapping` as needed.

    """

    matrix = "unknown"
    validator = None

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "Brightway2 %s: %s" % (self.__class__.__name__, self.name)

    __repr__ = lambda self: str(self)

    @property
    def filename(self):
        """Remove filesystem-unsafe characters and perform unicode normalization on ``self.name`` using :func:`.filesystem.safe_filename`."""
        return safe_filename(self.name)

    @property
    def registered(self):
        """Obsolete method kept for backwards compatibility."""
        return bool(self.id)

    def register(self, **kwargs):
        """Obsolete method kept for backwards compatibility."""
        pass

    def deregister(self):
        """Remove an object from the metadata store. Does not delete any files."""
        self.delete_instance()
        self.id = None

    def load(self):
        """Load the intermediate data for this object.

        Must be defined in subclasses.

        """
        raise NotImplementedError

    def copy(self, name):
        """Make a copy of this object with a new ``name``.

        This method only changes the name, but not any of the data or metadata.

        Args:
            * *name* (object): Name of the new object.

        Returns:
            The new object.

        """
        assert not self.select().where(self.name == name).count(), f"{name} already exists"
        new_obj = self.__class__()
        new_obj.name = name
        new_obj.data = copy.copy(self.data)
        new_obj.data['format'] = "Brightway copy"
        new_obj.save()
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

    def write(self, data, process=True):
        """Write associated object data (e.g. activities, CFs) to disk

        Must be defined in subclasses.

        Args:
            * *data* (object): The data

        """
        raise NotImplementedError

    def validate(self, data):
        """Validate data. Must be called manually."""
        self.validator(data)
        return True

    def dirpath_processed(self):
        return projects.dir / "processed"

    def filename_processed(self):
        return clean_datapackage_name(self.filename + ".zip")

    def filepath_processed(self):
        return self.dirpath_processed() / self.filename_processed()

    def datapackage(self):
        return load_datapackage(ZipFS(self.filepath_processed()))

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
