from .backends import Database
import warnings


def DatabaseChooser(name, backend=None):
    warnings.warn("`DatabaseChooser` is deprecated, use `Database` instead", DeprecationWarning)
    return Database(name)
