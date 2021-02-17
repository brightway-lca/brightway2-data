from . import databases
from .backends import SQLiteBackend
from .backends.iotable import IOTableBackend


def DatabaseChooser(name, backend=None):
    """A method that returns a database class instance.

    Database types are specified in `databases[database_name]['backend']`.

    """
    if name in databases:
        backend = databases[name].get("backend", backend or "sqlite")
    else:
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
