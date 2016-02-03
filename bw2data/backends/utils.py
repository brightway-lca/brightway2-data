# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .. import config
from ..meta import databases
import copy
import warnings


def convert_backend(database_name, backend):
    """Convert a Database to another backend.

    bw2data currently supports the `default` and `json` backends.

    Args:
        * `database_name` (unicode): Name of database.
        * `backend` (unicode): Type of database. `backend` should be recoginized by `DatabaseChooser`.

    Returns `False` if the old and new backend are the same. Otherwise returns an instance of the new Database object."""
    if database_name not in databases:
        print("Can't find database {}".format(database_name))

    from ..database import Database
    db = Database(database_name)
    if db.backend == backend:
        return False
    # Needed to convert from async json dict
    data = db.load(as_dict=True)
    if database_name in config.cache:
        del config.cache[database_name]
    metadata = copy.deepcopy(db.metadata)
    metadata[u"backend"] = str(backend)
    del databases[database_name]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        new_db = Database(database_name, backend)
        new_db.register(**metadata)
    new_db.write(data)
    return new_db
