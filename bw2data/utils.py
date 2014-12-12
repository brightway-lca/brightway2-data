# -*- coding: utf-8 -*-
from . import config
from .errors import WebUIError
from contextlib import contextmanager
import codecs
import collections
import datetime
import hashlib
import itertools
import os
import random
import re
import requests
import stats_arrays as sa
import string
import unicodedata
import urllib
import webbrowser
import zipfile
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

# Maximum value for unsigned integer stored in 4 bytes
MAX_INT_32 = 4294967295

# Type of technosphere/biosphere exchanges used in processed Databases
TYPE_DICTIONARY = {
    "unknown": -1,
    "production": 0,
    "technosphere": 1,
    "biosphere": 2,
}

DOWNLOAD_URL = "http://brightwaylca.org/data/"

re_slugify = re.compile('[^\w\s-]', re.UNICODE)


def natural_sort(l):
    """Sort the given list in the way that humans expect, e.g. 9 before 10."""
    # http://nedbatchelder.com/blog/200712/human_sorting.html#comments
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)


def recursively_sort(obj):
    """Recursively sort a nested data structure."""
    if isinstance(obj, dict):
        return sorted([(k, recursively_sort(v)) for k, v in obj.iteritems()])
    elif hasattr(obj, "__iter__"):
        return sorted((recursively_sort(x) for x in obj))
    else:
        return obj


def random_string(length=8):
    """Generate a random string of letters and numbers.

    Args:
        * *length* (int): Length of string, default is 8

    Returns:
        A string (not unicode)

    """
    return ''.join(random.choice(string.letters + string.digits
                                 ) for i in xrange(length))


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
    data = [(key, cf, geo) for (key, geo), cf in data.iteritems()]
    method = Method(name)
    method.register(**meta)
    method.write(data)
    method.process()
    return method


def safe_filename(string, add_hash=True):
    """Convert arbitrary strings to make them safe for filenames. Substitutes strange characters, and uses unicode normalization.

    if `add_hash`, appends hash of `string` to avoid name collisions.

    From http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python"""
    safe = re.sub(
        '[-\s]+',
        '-',
        unicode(
            re_slugify.sub(
                '',
                unicodedata.normalize('NFKD', unicode(string))
            ).strip()
        )
    )
    if add_hash:
        if isinstance(string, unicode):
            string = string.encode("utf8")
        return safe + u"." + hashlib.md5(string).hexdigest()
    else:
        return safe


@contextmanager
def safe_save(filepath):
    """Safe to a temporary filename, and restore to the correct filename only upon a successful write.

    This avoids data loss when a file is not completely written, for whatever reason. If an error occurs, the existing good data will not be overwritten. Usage:

    .. code-block:: python

        with safe_save("foo.txt") as filepath:
            with open(filepath, "w") as f:
                f.write("bar")

    Inside the context block, ``filepath`` is transformed to ``".foo.txt"`` (keeping the correct path). As the context block is exited, the written file is renamed to ``"foo.txt"``.

    Only needed in cases where you expect a data file to already be present on disk, e.g. metadata, or intermediate data. Not used on ``.process()`` as this can be repeated on error without data loss.

    """
    filepath = os.path.abspath(filepath)
    save_filepath = os.path.join(
        os.path.dirname(filepath),
        u"." + os.path.basename(filepath)
    )
    if os.path.exists(save_filepath):
        os.unlink(save_filepath)
    yield save_filepath
    if os.path.exists(filepath):
        backup_filepath = os.path.join(
            os.path.dirname(filepath),
            u"." + os.path.basename(filepath) + u".backup"
        )
        if os.path.exists(backup_filepath):
            os.unlink(backup_filepath)
        os.rename(filepath, backup_filepath)
    os.rename(save_filepath, filepath)


def clean_exchanges(data):
    """Make sure all exchange inputs are tuples, not lists."""
    def tupleize(value):
        for exc in value.get('exchanges', []):
            exc['input'] = tuple(exc['input'])
        return value
    return {key: tupleize(value) for key, value in data.iteritems()}


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

    for key, value in data.iteritems():
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
    if isinstance(data, unicode):
        return data
    elif isinstance(data, str):
        return unicode(data, encoding)  # Faster than str.encode
    elif isinstance(data, collections.Mapping):
        return dict(itertools.imap(
            recursive_str_to_unicode,
            data.iteritems(),
            itertools.repeat(encoding)
        ))
    elif isinstance(data, collections.Iterable):
        return type(data)(itertools.imap(
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


def database_hash(data):
    """Hash a Database.

    Data is recursively sorted so that the hashes are consistent. Useful for ensuring integrity or compatibility when exchanging data.

    Args:
        * *data* (dict): The Database data.

    Returns:
        A MD5 hash string, hex-encoded.

    """
    return hashlib.md5(unicode(recursively_sort(data))).hexdigest()


def activity_hash(data):
    """Hash an activity dataset.

    Used to import data formats like ecospold 1 (ecoinvent v1-2) and SimaPro, where no unique attributes for datasets are given. This is clearly an imperfect and brittle solution, but there is no other obvious approach at this time.

    Uses the following, in order:
        * *name* Lower case, defult is ``""`` (empty string).
        * *categories* In string form, joined together with ``""`` (empty string), default is ``[]``.
        * *unit* Lower case, default is ``""`` (empty string).
        * *location* Lower case, default is ``""`` (empty string).

    Args:
        * *data* (dict): The :ref:`activity dataset data <database-documents>`.

    Returns:
        A MD5 hash string, hex-encoded.

    """
    string = ((data.get(u"name") or u"").lower() +
              u"".join(data.get(u"categories", [])) +
              (data.get(u"unit") or u"").lower() +
              (data.get(u"location") or u"").lower() +
              (data.get(u"reference product") or u"").lower())
    return unicode(hashlib.md5(string.encode('utf-8')).hexdigest())


def download_file(filename):
    """Download a file from the Brightway2 website and write it to disk in ``downloads`` directory.

    Streams download to reduce memory usage.

    Args:
        * *filename* (str): The filename to download.

    Returns:
        The path of the created file.

    """

    dirpath = config.request_dir("downloads")
    filepath = os.path.join(dirpath, filename)
    download = requests.get(DOWNLOAD_URL + filename, stream=True).raw
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
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)

    if permanent:
        user_dir = os.path.expanduser("~")
        filename = "brightway2path.txt" if config._windows else ".brightway2path"
        with codecs.open(
                os.path.join(user_dir, filename),
                "w",
                encoding="utf-8") as f:
            f.write(dirpath)

        config.reset()
    else:
        config.dir = dirpath
    config.create_basic_directories()
    from .meta import reset_meta
    reset_meta()


def bw2setup():
    """Create basic directories, and download biosphere and LCIA methods"""
    from .io import download_biosphere, download_methods
    config.create_basic_directories()
    # config.is_temp_dir = False
    download_biosphere()
    download_methods()


def create_in_memory_zipfile_from_directory(path):
    # Based on http://stackoverflow.com/questions/2463770/python-in-memory-zip-library
    memory_obj = StringIO.StringIO()
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
