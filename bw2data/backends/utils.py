import copy
import json
import math
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


def check_temporal_distribution_sum(exc: dict) -> None:
    """Warn if the amounts of an exchange's temporal distribution don't sum to 1.

    The amounts are the shares of the exchange happening at each point in time,
    so the exchange amount is spread across time by multiplying the exchange
    amount with the amounts of the TD. If the amounts don't sum to 1, the total exchange
    amount is scaled up or down. Only warns, as that can be intended."""

    td = exc["temporal_distribution"]
    if isinstance(td, str):  # serialized with ``TemporalDistribution.to_json()``
        try:
            amounts = json.loads(td)["amount"]
        except (ValueError, KeyError, TypeError):
            return
    else:
        amounts = getattr(td, "amount", None)
        if amounts is None:
            return
    try:
        total = float(np.sum(amounts))
    except (ValueError, TypeError):
        return
    if not math.isclose(total, 1.0, rel_tol=1e-5):
        warnings.warn(
            f"Temporal distribution on exchange {exc.get('input')} -> {exc.get('output')} "
            f"sums to {total:.6g}, not 1. Check the amount array of this TD.",
            UserWarning,
        )


TD_LOADER_PREFIX = "bw_temporalis."


def _import_bw_temporalis():
    """Import ``bw_temporalis`` lazily.

    ``bw_temporalis`` imports ``bw2data``, so a module-level import here would be circular and
    succeed or fail depending on which library was imported first. Catches all exceptions as
    a broken install (e.g. an incompatible Python version) shouldn't break ``bw2data``."""
    try:
        import bw_temporalis
    except Exception:
        return None
    return bw_temporalis


def serialize_temporal_distribution(exc: dict) -> dict:
    """Return ``exc`` with a ``bw_temporalis`` temporal distribution converted to its JSON string.

    Returns a shallow copy if a conversion was made; the input is not modified. Any other
    ``temporal_distribution`` value (including already serialized strings) is left as is."""
    td = exc.get("temporal_distribution")
    if td is None or isinstance(td, str) or not hasattr(td, "to_json"):
        return exc
    bw_temporalis = _import_bw_temporalis()
    if bw_temporalis is None or not isinstance(td, bw_temporalis.TemporalDistribution):
        return exc
    return {**exc, "temporal_distribution": td.to_json()}


def restore_temporal_distribution(exc: dict) -> dict:
    """Return ``exc`` with a JSON ``temporal_distribution`` converted back to a
    ``bw_temporalis`` object, using the ``__loader__`` class written by ``to_json()``.

    Returns a shallow copy if a conversion was made. Values which aren't serialized temporal
    distributions are left as is. Warns and leaves the JSON in place if ``bw_temporalis``
    isn't available or the JSON can't be loaded."""
    td = exc.get("temporal_distribution")
    if not isinstance(td, str) or "__loader__" not in td:
        return exc
    try:
        loader = json.loads(td)["__loader__"]
    except (ValueError, KeyError, TypeError):
        return exc
    if not isinstance(loader, str) or not loader.startswith(TD_LOADER_PREFIX):
        return exc

    bw_temporalis = _import_bw_temporalis()
    if bw_temporalis is None:
        warnings.warn(
            "Found a serialized temporal distribution, but `bw_temporalis` can't be imported. "
            "Leaving it as a JSON string; install `bw_temporalis` to restore it.",
            UserWarning,
        )
        return exc
    try:
        cls = getattr(bw_temporalis, loader[len(TD_LOADER_PREFIX) :])
        restored = cls.from_json(td)
    except Exception as e:
        warnings.warn(
            f"Can't restore temporal distribution with loader {loader}; leaving it as "
            f"a JSON string. Error: {e}",
            UserWarning,
        )
        return exc
    return {**exc, "temporal_distribution": restored}


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
    ds = serialize_temporal_distribution(ds)
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
