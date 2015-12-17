# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import config, projects
from .errors import WebUIError, UnknownObject
from .fatomic import open
from .project import safe_filename
from contextlib import contextmanager
from io import StringIO
import collections
import datetime
import itertools
import os
import random
import re
import requests
import stats_arrays as sa
import string
import urllib
import webbrowser
import zipfile
import sys


# Maximum value for unsigned integer stored in 4 bytes
MAX_INT_32 = 4294967295

# Type of technosphere/biosphere exchanges used in processed Databases
TYPE_DICTIONARY = {
    "unknown": -1,
    "production": 0,
    "technosphere": 1,
    "biosphere": 2,
    "substitution": 3,
}

DOWNLOAD_URL = "https://brightwaylca.org/data/"

numpy_string = lambda x: bytes(x) if sys.version_info < (3, 0) else x


def natural_sort(l):
    """Sort the given list in the way that humans expect, e.g. 9 before 10."""
    # http://nedbatchelder.com/blog/200712/human_sorting.html#comments
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)


def random_string(length=8):
    """Generate a random string of letters and numbers.

    Args:
        * *length* (int): Length of string, default is 8

    Returns:
        A string (not unicode)

    """
    return ''.join(random.choice(string.ascii_letters + string.digits
                                 ) for i in range(length))


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
        "description": "Combination of the following methods: " +
        ", ".join([str(x) for x in ms]),
        "unit": list(units)[0] if len(units) == 1 else "Unknown"
    }
    data = [(key, cf, geo) for (key, geo), cf in data.items()]
    method = Method(name)
    method.register(**meta)
    method.write(data)
    return method


def clean_exchanges(data):
    """Make sure all exchange inputs are tuples, not lists."""
    def tupleize(value):
        for exc in value.get('exchanges', []):
            exc['input'] = tuple(exc['input'])
        return value
    return {key: tupleize(value) for key, value in data.items()}


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
    assert distribution in {None, sa.UniformUncertainty, sa.NormalUncertainty}, \
        u"``uncertainify`` only supports normal and uniform distributions"
    assert bounds_factor is None or bounds_factor * 1. > 0, \
        "bounds_factor must be a positive number"
    assert sd_factor * 1. > 0, "sd_factor must be a positive number"

    for key, value in data.items():
        for exchange in value.get(u'exchanges', []):
            if (exchange.get(u'type') == u'production') or \
                    (exchange.get(u'uncertainty type',
                                  sa.UndefinedUncertainty.id) \
                    != sa.UndefinedUncertainty.id):
                continue
            if exchange[u"amount"] == 0:
                continue

            if bounds_factor is not None:
                exchange.update({
                    u"minimum": (1 - bounds_factor) * exchange['amount'],
                    u"maximum": (1 + bounds_factor) * exchange['amount'],
                })
                if exchange[u"amount"] < 0:
                    exchange[u"minimum"], exchange[u"maximum"] = exchange[u"maximum"], exchange[u"minimum"]

            if distribution == sa.NormalUncertainty:
                exchange.update({
                    u"uncertainty type": sa.NormalUncertainty.id,
                    u"loc": exchange[u'amount'],
                    u"scale": abs(sd_factor * exchange[u'amount']),
                })
            else:
                assert bounds_factor is not None, \
                    "must specify bounds_factor for uniform distribution"
                exchange.update({
                    u"uncertainty type": sa.UniformUncertainty.id,
                })
    return data

def recursive_str_to_unicode(data, encoding="utf8"):
    """Convert the strings inside a (possibly nested) python data structure to unicode strings using `encoding`."""
    # Adapted from
    # http://stackoverflow.com/questions/1254454/fastest-way-to-convert-a-dicts-keys-values-from-unicode-to-str
    if isinstance(data, str):
        return data
    elif isinstance(data, bytes):
        return str(data, encoding)  # Faster than str.encode
    elif isinstance(data, collections.Mapping):
        return dict(map(
            recursive_str_to_unicode,
            data.items(),
            itertools.repeat(encoding)
        ))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(
            recursive_str_to_unicode,
            data,
            itertools.repeat(encoding)
        ))
    else:
        return data


def combine_databases(name, *dbs):
    """Combine databases into new database called ``name``."""
    pass


def merge_databases(parent_db, *others):
    """Merge ``others`` into ``parent_db``, including updating exchanges."""
    pass


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

    assert isinstance(directory, str), "`directory` must be a string"
    dirpath = projects.request_directory(directory)
    filepath = os.path.join(dirpath, filename)
    download_path = (url if url is not None else DOWNLOAD_URL) + filename
    download = requests.get(download_path, stream=True).raw
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
    base_url = config.p.get('web_ui_address', "http://127.0.0.1:5000") + "/ping"
    try:
        response = requests.get(base_url)
    except requests.ConnectionError:
        return False
    return response.text == u"pong"


def open_activity_in_webbrowser(activity):
    """Open a dataset document in the Brightway2 web UI. Requires ``bw2-web`` to be running.

    ``activity`` is a dataset key, e.g. ``("foo", "bar")``."""
    base_url = config.p.get('web_ui_address', "http://127.0.0.1:5000")
    if not web_ui_accessible():
        raise WebUIError("Can't find bw2-web UI (tried %s)" % base_url)
    url = base_url + u"/view/%s/%s" % (
        urllib.quote(activity[0]),
        urllib.quote(activity[1])
    )
    webbrowser.open_new_tab(url)
    return url


def set_data_dir(dirpath, permanent=True):
    """Set the Brightway2 data directory to ``dirpath``.

    If ``permanent`` is ``True``, then set ``dirpath`` as the default data directory.

    Creates ``dirpath`` if needed. Also creates basic directories, and resets metadata.

    """
    warnings.warn("`set_data_dir` is deprecated; use `projects.current = 'my "
                  "project name'` for a new project space.",
                  DeprecationWarning)


def create_in_memory_zipfile_from_directory(path):
    # Based on http://stackoverflow.com/questions/2463770/python-in-memory-zip-library
    memory_obj = StringIO()
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    zf = zipfile.ZipFile(memory_obj, "a", zipfile.ZIP_DEFLATED, False)
    for filename in files:
        zf.writestr(
            filename,
            open(os.path.join(path, filename)).read()
        )
    # Mark the files as having been created on Windows so that
    # Unix permissions are not inferred as 0000
    for zfile in zf.filelist:
        zfile.create_system = 0
    zf.close()
    memory_obj.seek(0)
    return memory_obj


def get_activity(key):
    from .database import Database
    try:
        return Database(key[0]).get(key[1])
    except TypeError:
        raise UnknownObject("Key {} cannot be understood as an activity"
                            " or `(database, code)` tuple.")
