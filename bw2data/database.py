from . import databases, config
from .backends.default import SingleFileDatabase
from .backends.json import JSONDatabase


def DatabaseChooser(name, backend=None):
    """A method that returns a database class instance. The default database type is `SingleFileDatabase`. `JSONDatabase` stores each process dataset in indented JSON in a separate file. Database types are specified in `databases[database_name]['backend']`.

    New database types can be registered with the config object like this:

    .. code-block:: python

        config.backends['backend type string'] = MyNewBackendClass

    Registering new backends must be done each time you start the Python interpreter.

    To test whether an object is a ``Database`` subclass, do:

    .. code-block:: python

        from bw2data.backends import DatabaseBase
        isinstance(my_database, DatabaseBase)

    """
    if name in databases:
        backend = databases[name].get(u"backend", u"default")
    else:
        backend = backend or u"default"

    if backend == u"default":
        return SingleFileDatabase(name)
    elif backend == u"json":
        return JSONDatabase(name)
    elif backend in config.backends:
        return config.backends[backend](name)
    else:
        raise ValueError(u"Backend {} not found".format(backend))


# Backwards compatibility
Database = DatabaseChooser
