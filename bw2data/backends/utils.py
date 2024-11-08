import copy
import warnings
from typing import Any, Optional

import numpy as np

from bw2data import config
from bw2data.backends.schema import get_id
from bw2data.configuration import labels
from bw2data.errors import InvalidExchange, UntypedExchange
from bw2data.meta import databases, methods
from bw2data.signals import SignaledDataset
from bw2data.snowflake_ids import snowflake_id_generator


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

    Returns `False` if the old and new backend are the same. Otherwise returns an instance of the new Database object.
    """
    if database_name not in databases:
        raise ValueError(f"Can't find database {database_name}")

    from bw2data import projects
    from bw2data.database import Database

    db = Database(database_name)
    if db.backend == backend:
        return False
    if backend == "iotable" and projects.dataset.is_sourced:
        raise ValueError("`iotable` backend not consistent with `sourced` project")

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
        new_db.write(data, searchable=databases[database_name].get("searchable"))
    return new_db


def check_exchange(exc):
    """Check exchange data validity when processing"""
    if "type" not in exc:
        raise UntypedExchange
    if "amount" not in exc or "input" not in exc:
        raise InvalidExchange
    if np.isnan(exc["amount"]) or np.isinf(exc["amount"]):
        raise ValueError("Invalid amount in exchange {}".format(exc))


def dict_as_activitydataset(ds: Any, add_snowflake_id: bool = False) -> dict:
    val = {
        "data": ds,
        "database": ds["database"],
        "code": ds["code"],
        "location": ds.get("location"),
        "name": ds.get("name"),
        "product": ds.get("reference product"),
        "type": ds.get("type", labels.process_node_default),
    }
    # Use during `insert_many` calls as these skip auto id generation because they don't call
    # `.save()`
    if add_snowflake_id:
        val["id"] = next(snowflake_id_generator)
    return val


def dict_as_exchangedataset(ds: Any) -> dict:
    return {
        "data": ds,
        "input_database": ds["input"][0],
        "input_code": ds["input"][1],
        "output_database": ds["output"][0],
        "output_code": ds["output"][1],
        "type": ds["type"],
    }


def get_obj_as_dict(cls: SignaledDataset, obj_id: Optional[int]) -> dict:
    """
    Loads an object's data from the database as a dictionary.

    The format used is that of the serialization of revisions (see also the
    `dict_as_*` functions above); in particular, an empty dictionary is returned
    if the ID is `None` (but not if the object does not exist).
    """
    if obj_id is None:
        return {}
    to_dict = globals()["dict_as_" + cls.__name__.lower()]
    obj = cls.get_by_id(obj_id)
    ret = to_dict(obj.data)
    ret["id"] = obj_id
    return ret


def replace_cfs(old_key, new_key):
    """Replace ``old_key`` with ``new_key`` in characterization factors.

    Returns list of modified methods."""
    from bw2data.method import Method

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
