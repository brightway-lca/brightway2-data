from peewee import DoesNotExist
from .backends import SQLiteBackend
from .backends.iotable import IOTableBackend


def DatabaseChooser(name, backend=None):
    """A method that returns a database class instance.

    Database types are specified in `DatabaseMetdata.data['backend']`.

    """
    if backend not in ("sqlite", "iotable"):
        raise ValueError("Please instantiate non-standard databases manually")

    try:
        db = SQLiteBackend.get(SQLiteBackend.name == name)
    except DoesNotExist:
        raise KeyError(f"Can't find database {name}")

    if db.backend == 'iotable':
        db = IOTableBackend.get(IOTableBackend.name == name)
    return db


# Backwards compatibility
Database = DatabaseChooser
