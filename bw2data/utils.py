import collections
import itertools
import numbers
import warnings

import requests
import stats_arrays as sa
from atomicwrites import atomic_write

from .errors import MultipleResults, NotFound, UnknownObject


# Type of technosphere/biosphere exchanges used in processed Databases
TYPE_DICTIONARY = {
    "unknown": -1,
    "production": 0,
    "technosphere": 1,
    "biosphere": 2,
    "substitution": 3,
}

DOWNLOAD_URL = "https://brightway.dev/data/"


def safe_filename(*args, **kwargs):
    raise DeprecationWarning("`safe_filename` has been moved to `bw_processing`")


POSITIVE_DISTRIBUTIONS = {2, 6, 8, 9, 10}


def as_uncertainty_dict(value):
    """Given either a number or a ``stats_arrays`` uncertainty dict, return an uncertainty dict"""
    if isinstance(value, dict):
        if (
            value.get("amount", 0) < 0
            and (
                value.get("uncertainty_type") in POSITIVE_DISTRIBUTIONS
                or value.get("uncertainty type") in POSITIVE_DISTRIBUTIONS
            )
            and "negative" not in value
        ):
            value["negative"] = True
        return value
    try:
        return {"amount": float(value)}
    except:
        raise TypeError(
            "Value must be either an uncertainty dict. or number"
            " (got %s: %s)" % (type(value), value)
        )


def uncertainify(data, distribution=None, bounds_factor=0.1, sd_factor=0.1):
    """
    Add some rough uncertainty to exchanges.

    .. warning:: This function only changes exchanges with no uncertainty type or uncertainty type ``UndefinedUncertainty``, and does not change production exchanges!

    Can only apply normal or uniform uncertainty distributions; default is uniform. Distribution, if specified, must be a ``stats_array`` uncertainty object.

    ``data`` is a LCI data dictionary.

    If using the normal distribution:

    * ``sd_factor`` will be multiplied by the mean to calculate the standard deviation.
    * If no bounds are desired, set ``bounds_factor`` to ``None``.
    * Otherwise, the bounds will be ``[(1 - bounds_factor) * mean, (1 + bounds_factor) * mean]``.

    If using the uniform distribution, then the bounds are ``[(1 - bounds_factor) * mean, (1 + bounds_factor) * mean]``.

    Returns the modified data.
    """
    assert distribution in {
        None,
        sa.UniformUncertainty,
        sa.NormalUncertainty,
    }, "``uncertainify`` only supports normal and uniform distributions"
    assert (
        bounds_factor is None or bounds_factor * 1.0 > 0
    ), "bounds_factor must be a positive number"
    assert sd_factor * 1.0 > 0, "sd_factor must be a positive number"

    for key, value in data.items():
        for exchange in value.get("exchanges", []):
            if (exchange.get("type") == "production") or (
                exchange.get("uncertainty type", sa.UndefinedUncertainty.id)
                != sa.UndefinedUncertainty.id
            ):
                continue
            if exchange["amount"] == 0:
                continue

            if bounds_factor is not None:
                exchange.update(
                    {
                        "minimum": (1 - bounds_factor) * exchange["amount"],
                        "maximum": (1 + bounds_factor) * exchange["amount"],
                    }
                )
                if exchange["amount"] < 0:
                    exchange["minimum"], exchange["maximum"] = (
                        exchange["maximum"],
                        exchange["minimum"],
                    )

            if distribution == sa.NormalUncertainty:
                exchange.update(
                    {
                        "uncertainty type": sa.NormalUncertainty.id,
                        "loc": exchange["amount"],
                        "scale": abs(sd_factor * exchange["amount"]),
                    }
                )
            else:
                assert (
                    bounds_factor is not None
                ), "must specify bounds_factor for uniform distribution"
                exchange.update(
                    {
                        "uncertainty type": sa.UniformUncertainty.id,
                    }
                )
    return data


def recursive_str_to_unicode(data, encoding="utf8"):
    """Convert the strings inside a (possibly nested) python data structure to unicode strings using `encoding`."""
    # Adapted from
    # http://stackoverflow.com/questions/1254454/fastest-way-to-convert-a-dicts-keys-values-from-unicode-to-str
    if isinstance(data, str):
        return data
    elif isinstance(data, bytes):
        return str(data, encoding)  # Faster than str.encode
    elif isinstance(data, collections.abc.Mapping):
        return dict(
            map(recursive_str_to_unicode, data.items(), itertools.repeat(encoding))
        )
    elif isinstance(data, collections.abc.Iterable):
        return type(data)(
            map(recursive_str_to_unicode, data, itertools.repeat(encoding))
        )
    else:
        return data


def download_file(filename, directory="downloads", url=None):
    """Download a file and write it to disk in ``downloads`` directory.

    If ``url`` is None, uses the Brightway2 data base URL. ``url`` should everything up to the filename, such that ``url`` + ``filename`` is the valid complete URL to download from.

    Streams download to reduce memory usage.

    Args:
        * *filename* (str): The filename to download.
        * *directory* (str, optional): Directory to save the file. Created if it doesn't already exist.
        * *url* (str, optional): URL where the file is located, if not the default Brightway data URL.

    Returns:
        The path of the created file.

    """
    from . import projects

    assert isinstance(directory, str), "`directory` must be a string"
    dirpath = projects.request_directory(directory)
    filepath = dirpath / filename
    download_path = (url if url is not None else DOWNLOAD_URL) + filename
    request = requests.get(download_path, stream=True)
    if request.status_code != 200:
        raise NotFound(
            "URL {} returns status code {}.".format(download_path, request.status_code)
        )
    download = request.raw
    chunk = 128 * 1024
    with atomic_write(filepath, mode="wb", overwrite=True) as f:
        while True:
            segment = download.read(chunk)
            if not segment:
                break
            f.write(segment)
    return filepath


def get_node(**kwargs):
    from . import databases
    from .backends import Activity
    from .backends import ActivityDataset as AD
    from .backends.iotable.proxies import IOTableActivity

    def node_class(database_name):
        mapping = {
            "sqlite": Activity,
            "iotable": IOTableActivity,
        }
        return mapping[databases[database_name].get("backend", "sqlite")]

    mapping = {
        "id": AD.id,
        "code": AD.code,
        "database": AD.database,
        "location": AD.location,
        "name": AD.name,
        "product": AD.product,
        "type": AD.type,
    }

    qs = AD.select()
    for key, value in kwargs.items():
        try:
            qs = qs.where(mapping[key] == value)
        except KeyError:
            continue

    candidates = [node_class(obj.database)(obj) for obj in qs]

    extended_search = any(key not in mapping for key in kwargs)
    if extended_search:
        if "database" not in kwargs:
            warnings.warn(
                "Given search criteria very broad; try to specify at least a database"
            )
        candidates = [
            obj
            for obj in candidates
            if all(
                obj.get(key) == value
                for key, value in kwargs.items()
                if key not in mapping
            )
        ]
    if len(candidates) > 1:
        raise MultipleResults(
            "Found {} results for the given search".format(len(candidates))
        )
    elif not candidates:
        raise UnknownObject
    return candidates[0]


def get_activity(key=None, **kwargs):
    """Support multiple ways to get exactly one activity node.

    ``key`` can be an integer or a key tuple."""
    from .backends import Activity

    # Includes subclasses
    if isinstance(key, Activity):
        return key
    elif isinstance(key, tuple):
        kwargs["database"] = key[0]
        kwargs["code"] = key[1]
    elif isinstance(key, numbers.Integral):
        kwargs["id"] = key
    return get_node(**kwargs)


def get_geocollection(location, default_global_location=False):
    """conservative approach to finding geocollections. Won't guess about ecoinvent or other databases."""
    if not location:
        if default_global_location:
            return "world"
        else:
            return None
    elif isinstance(location, tuple):
        return location[0]
    elif isinstance(location, str) and (
        len(location) == 2 or location.lower() == "glo"
    ):
        return "world"
    else:
        return None
