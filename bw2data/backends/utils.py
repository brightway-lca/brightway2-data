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
    from ..database import Database
    db = Database(database_name)
    if db.backend == backend:
        return False
    # Needed to convert from async json dict
    data = {key: dict(value) for key, value in db.load().items()}
    metadata = copy.deepcopy(databases[database_name])
    metadata[u"backend"] = unicode(backend)
    del databases[database_name]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        new_db = Database(database_name, backend)
        new_db.register(**metadata)
    new_db.write(data)
    new_db.process()
    return new_db
