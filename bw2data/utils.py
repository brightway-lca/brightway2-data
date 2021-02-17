from . import config
from .errors import WebUIError, UnknownObject, NotFound, ValidityError
from .fatomic import open
from io import StringIO
from pathlib import Path
from peewee import DoesNotExist
import collections
import itertools
import os
import random
import re
import requests
import stats_arrays as sa
import string
import urllib
import warnings
import webbrowser
import zipfile


# Type of technosphere/biosphere exchanges used in processed Databases
TYPE_DICTIONARY = {
    "unknown": -1,
    "production": 0,
    "technosphere": 1,
    "biosphere": 2,
    "substitution": 3,
}

DOWNLOAD_URL = "https://brightwaylca.org/data/"


def maybe_path(x):
    return Path(x) if x else x


def natural_sort(l):
    """Sort the given list in the way that humans expect, e.g. 9 before 10."""
    # http://nedbatchelder.com/blog/200712/human_sorting.html#comments
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split("([0-9]+)", key)]
    return sorted(l, key=alphanum_key)


def random_string(length=8):
    """Generate a random string of letters and numbers.

    Args:
        * *length* (int): Length of string, default is 8

    Returns:
        A string (not unicode)

    """
    return "".join(
        random.choice(string.ascii_letters + string.digits) for i in range(length)
    )


def combine_methods(name, *ms):
    """Combine LCIA methods by adding duplicate characterization factors.

    Args:
        * *ms* (one or more method id tuples): Any number of method ids, e.g. ``("my method", "wow"), ("another method", "wheee")``.

    Returns:
        The new Method instance.

    """
    from . import Method, methods

    data = {}
    units = set([methods[tuple(x)]["unit"] for x in ms])
    for m in ms:
        for key, cf, geo in Method(m).load():
            data[(key, geo)] = data.get((key, geo), 0) + cf
    meta = {
        "description": "Combination of the following methods: "
        + ", ".join([str(x) for x in ms]),
        "unit": list(units)[0] if len(units) == 1 else "Unknown",
    }
    data = [(key, cf, geo) for (key, geo), cf in data.items()]
    method = Method(name)
    method.register(**meta)
    method.write(data)
    return method


def clean_exchanges(data):
    """Make sure all exchange inputs are tuples, not lists."""

    def tupleize(value):
        for exc in value.get("exchanges", []):
            exc["input"] = tuple(exc["input"])
        return value

    return {key: tupleize(value) for key, value in data.items()}


def as_uncertainty_dict(value):
    """Given either a number or a ``stats_arrays`` uncertainty dict, return an uncertainty dict"""
    if isinstance(value, dict):
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
                    {"uncertainty type": sa.UniformUncertainty.id,}
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


def combine_databases(name, *dbs):
    """Combine databases into new database called ``name``."""
    pass


def merge_databases(parent_db, other):
    """Merge ``other`` into ``parent_db``, including updating exchanges.

    All databases must be SQLite databases.

    ``parent_db`` and ``other`` should be the names of databases.

    Doesn't return anything."""
    from .database import Database
    from .backends import (
        ActivityDataset,
        ExchangeDataset,
        SQLiteBackend,
        sqlite3_lci_db,
    )
    from . import databases

    assert parent_db in databases
    assert other in databases

    first = Database(parent_db)
    second = Database(other)

    if type(first) != SQLiteBackend or type(second) != SQLiteBackend:
        raise ValidityError("Both databases must be `SQLiteBackend`")

    first_codes = {
        obj.code
        for obj in ActivityDataset.select().where(ActivityDataset.database == parent_db)
    }
    second_codes = {
        obj.code
        for obj in ActivityDataset.select().where(ActivityDataset.database == other)
    }
    if first_codes.intersection(second_codes):
        raise ValidityError("Duplicate codes - can't merge databases")

    with sqlite3_lci_db.atomic():
        ActivityDataset.update(database=parent_db).where(
            ActivityDataset.database == other
        ).execute()
        ExchangeDataset.update(input_database=parent_db).where(
            ExchangeDataset.input_database == other
        ).execute()
        ExchangeDataset.update(output_database=parent_db).where(
            ExchangeDataset.output_database == other
        ).execute()

    Database(parent_db).process()
    del databases[other]


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
    with open(filepath, "wb") as f:
        while True:
            segment = download.read(chunk)
            if not segment:
                break
            f.write(segment)
    return filepath


def web_ui_accessible():
    """Test if ``bw2-web`` is running and accessible. Returns ``True`` or ``False``."""
    base_url = config.p.get("web_ui_address", "http://127.0.0.1:5000") + "/ping"
    try:
        response = requests.get(base_url)
    except requests.ConnectionError:
        return False
    return response.text == "pong"


def open_activity_in_webbrowser(activity):
    """Open a dataset document in the Brightway2 web UI. Requires ``bw2-web`` to be running.

    ``activity`` is a dataset key, e.g. ``("foo", "bar")``."""
    base_url = config.p.get("web_ui_address", "http://127.0.0.1:5000")
    if not web_ui_accessible():
        raise WebUIError("Can't find bw2-web UI (tried %s)" % base_url)
    url = base_url + "/view/%s/%s" % (
        urllib.quote(activity[0]),
        urllib.quote(activity[1]),
    )
    webbrowser.open_new_tab(url)
    return url


def set_data_dir(dirpath, permanent=True):
    """Set the Brightway2 data directory to ``dirpath``.

    If ``permanent`` is ``True``, then set ``dirpath`` as the default data directory.

    Creates ``dirpath`` if needed. Also creates basic directories, and resets metadata.

    """
    warnings.warn(
        "`set_data_dir` is deprecated; use `projects.set_current('my "
        "project name')` for a new project space.",
        DeprecationWarning,
    )


def switch_data_directory(dirpath):
    from .projects import ProjectDataset, SubstitutableDatabase

    if dirpath == bw.projects._base_data_dir:
        print("dirpath already loaded")
        return
    try:
        assert os.path.isdir(dirpath)
        bw.projects._base_data_dir = dirpath
        bw.projects._base_logs_dir = os.path.join(dirpath, "logs")
        # create folder if it does not yet exist
        if not os.path.isdir(bw.projects._base_logs_dir):
            os.mkdir(bw.projects._base_logs_dir)
        # load new brightway directory
        bw.projects.db = SubstitutableDatabase(
            os.path.join(bw.projects._base_data_dir, "projects.db"), [ProjectDataset]
        )
        print("Loaded brightway2 data directory: {}".format(bw.projects._base_data_dir))

    except AssertionError:
        print('Could not access directory specified "dirpath"')


def create_in_memory_zipfile_from_directory(path):
    # Based on http://stackoverflow.com/questions/2463770/python-in-memory-zip-library
    memory_obj = StringIO()
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    zf = zipfile.ZipFile(memory_obj, "a", zipfile.ZIP_DEFLATED, False)
    for filename in files:
        zf.writestr(filename, open(os.path.join(path, filename)).read())
    # Mark the files as having been created on Windows so that
    # Unix permissions are not inferred as 0000
    for zfile in zf.filelist:
        zfile.create_system = 0
    zf.close()
    memory_obj.seek(0)
    return memory_obj


def get_activity(key):
    from .database import Database
    from .backends import ActivityDataset as AD, Activity

    if isinstance(key, int):
        try:
            return Activity(AD.get(AD.id == key))
        except DoesNotExist:
            raise UnknownObject

    try:
        return Database(key[0]).get(key[1])
    except TypeError:
        raise UnknownObject(
            "Key {} cannot be understood as an activity" " or `(database, code)` tuple."
        )
