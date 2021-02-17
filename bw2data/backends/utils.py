from .. import config
from ..meta import methods, databases
from ..method import Method
from ..errors import UntypedExchange, InvalidExchange
import copy
import numpy as np
import warnings


def get_csv_data_dict(ds):
    fields = {"name", "reference product", "unit", "location"}
    dd = {field: ds.get(field) for field in fields}
    dd["id"] = get_id(ds)
    return dd


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
    metadata["backend"] = str(backend)
    del databases[database_name]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        new_db = Database(database_name, backend)
        new_db.register(**metadata)
    new_db.write(data)
    return new_db


def check_exchange(exc):
    """Check exchange data validity when processing"""
    if "type" not in exc:
        raise UntypedExchange
    if "amount" not in exc or "input" not in exc:
        raise InvalidExchange
    if np.isnan(exc["amount"]) or np.isinf(exc["amount"]):
        raise ValueError("Invalid amount in exchange {}".format(exc))


def dict_as_activitydataset(ds):
    return {
        "data": ds,
        "database": ds["database"],
        "code": ds["code"],
        "location": ds.get("location"),
        "name": ds.get("name"),
        "product": ds.get("reference product"),
        "type": ds.get("type", "process"),
    }


def dict_as_exchangedataset(ds):
    return {
        "data": ds,
        "input_database": ds["input"][0],
        "input_code": ds["input"][1],
        "output_database": ds["output"][0],
        "output_code": ds["output"][1],
        "type": ds["type"],
    }


def replace_cfs(old_key, new_key):
    """Replace ``old_key`` with ``new_key`` in characterization factors.

    Returns list of modified methods."""
    altered_methods = []
    for name in methods:
        changed = False
        data = Method(name).load()
        for line in data:
            if line[0] == old_key:
                line[0], changed = new_key, True
        if changed:
            Method(name).write(data)
            altered_methods.append(name)
    return altered_methods


def retupleize_geo_strings(value):
    """Transform data from SQLite representation to Python objects.

    We are using a SQLite3 cursor, which means that the Peewee data conversion code is not called. So ``('foo', 'bar')`` is stored as a string, not a tuple. This code tries to do this conversion correctly.

    TODO: Adapt what Peewee does in this case?"""
    if not value:
        return value
    elif "(" not in value:
        return value
    try:
        # Is this a dirty, dirty hack, or inspiration?
        # Location is retrieved as a string from the database
        # The alternative is to retrieve and process the
        # entire activity dataset...
        return eval(value)
    except NameError:
        # Not everything with a parentheses is a tuple.
        return value
