from . import databases, config
from .backends.default import SingleFileDatabase
from .backends.json import JSONDatabase


def Database(name, backend=None):
    """A method that will return a class instance.

    Breaks isinstance(my_database, Database). Use this instead:

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
