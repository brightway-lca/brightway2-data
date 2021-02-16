# -*- coding: utf-8 -*-
from . import databases, config
from .backends.peewee import SQLiteBackend
from .backends.iotable import IOTableBackend
from .utils import get_activity


def DatabaseChooser(name, backend=None):
    """A method that returns a database class instance.

    Database types are specified in `databases[database_name]['backend']`.

    New database types can be registered with the config object:

    .. code-block:: python

        config.backends['backend type string'] = MyNewBackendClass

    .. warning:: Registering new backends must be done each time you start the Python interpreter.

    To test whether an object is a database subclass, do:

    .. code-block:: python

        from bw2data.backends import LCIBackend
        isinstance(my_database, LCIBackend)

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
    elif backend in config.backends:
        return config.backends[backend](name)
    else:
        raise ValueError("Backend {} not found".format(backend))


# Backwards compatibility
Database = DatabaseChooser
Database.get = get_activity
