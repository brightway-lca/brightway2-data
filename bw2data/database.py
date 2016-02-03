# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import databases, config
from .backends.single_file import SingleFileDatabase
from .backends.json import JSONDatabase
from .backends.peewee import SQLiteBackend
from .backends.iotable import IOTableBackend
from .utils import get_activity


def DatabaseChooser(name, backend=None):
    """A method that returns a database class instance. The default database type is `SingleFileDatabase`. `JSONDatabase` stores each process dataset in indented JSON in a separate file. Database types are specified in `databases[database_name]['backend']`.

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
    if backend == "default":
        databases[name]['backend'] = 'singlefile'
        databases.flush()
        return SingleFileDatabase(name)
    elif backend == "sqlite":
        return SQLiteBackend(name)
    elif backend == "singlefile":
        return SingleFileDatabase(name)
    elif backend == "iotable":
        return IOTableBackend(name)
    elif backend == "json":
        raise ValueError("JSON backend not supported in dev release")
        return JSONDatabase(name)
    elif backend in config.backends:
        return config.backends[backend](name)
    else:
        raise ValueError("Backend {} not found".format(backend))

# Backwards compatibility
Database = DatabaseChooser
Database.get = get_activity
