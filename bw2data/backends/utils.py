from ..meta import databases
import copy


def convert_backend(database_name, backend):
    from ..database import Database
    db = Database(database_name)
    if db.backend == backend:
        return False
    data = db.load()
    metadata = copy.deepcopy(databases[database_name])
    del databases[database_name]
    new_db = Database(database_name, backend)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        new_db.register(**metadata)
    new_db.write(data)
    new_db.process()
    return new_db
