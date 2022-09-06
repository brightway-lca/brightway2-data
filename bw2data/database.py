import warnings

from .backends import Database
from .errors import UnknownObject


def DatabaseChooser(name, backend=None):
    warnings.warn(
        "`DatabaseChooser` is deprecated, use `Database` instead", DeprecationWarning
    )
    if Database.select().where(Database.name == name).count():
        db = Database.get(Database.name == name)
        if backend and db.backend != backend:
            raise UnknownObject(
                "Inconsistent backend: Database {} has backend {} but you gave {}".format(
                    name, db.backend, backend
                )
            )
        return db
    else:
        db = Database()
        db.name = name
        if backend:
            db.backend = backend
        return db
