from peewee import DoesNotExist
from .backends import SQLiteBackend, DatabaseMetadata
from .backends.iotable import IOTableBackend


def DatabaseChooser(name, backend=None):
    """A method that returns a database class instance.

    Database types are specified in `DatabaseMetdata.data['backend']`.

    """
    try:
        dm = DatabaseMetadata.get(DatabaseMetadata.name == name)
        backend = dm.data.get('backend', backend or "sqlite")
    except DoesNotExist:
        backend = backend or "sqlite"

    # Backwards compatibility
    if backend == "sqlite":
        return SQLiteBackend(name)
    elif backend == "iotable":
        return IOTableBackend(name)
    else:
        raise ValueError("Backend {} not found".format(backend))


# Backwards compatibility
Database = DatabaseChooser
