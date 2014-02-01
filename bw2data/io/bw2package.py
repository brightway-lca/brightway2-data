# -*- coding: utf-8 -*
from .. import config, JsonWrapper
from ..logs import get_logger
from ..utils import download_file
from ..errors import UnsafeData, InvalidPackage
from ..validate import bw2package_validator
from voluptuous import Invalid
from time import time
import os


class BW2Package(object):
    """This is a format for saving objects which implement the :ref:`datastore` API. Data is stored as a BZip2-compressed file of JSON data. This archive format is compatible across Python versions, and is, at least in theory, programming-language agnostic.

    Validation is done with ``bw2data.validate.bw2package_validator``.

    The data format is:

    .. code-block:: python

        {
            'metadata': {},  # Dictionary of metadata to be written to metadata-store.
            'name': basestring,  # Name of object
            'class': {  # Data on the undying class. A new class is instantiated base on these strings. See _create_class.
                'module': basestring,  # e.g. "bw2data.database"
                'name': basestring  # e.g. "Database"
            },
            'unrolled_dict': bool,  # Flag indicating if dictionary keys needed to be modified for JSON
            'data': object  # Object data, e.g. LCIA method or LCI database
        }

    .. note:: This class does not need to be instantiated, as all its methods are ``classmethods``, i.e. do ``BW2Package.import_obj("foo")`` instead of ``BW2Package().import_obj("foo")``

    """
    APPROVED = {
        'bw2data',
        'bw2regional',
        'bw2calc'
    }

    @classmethod
    def _get_class_metadata(cls, obj):
        return {
            'module': obj.__class__.__module__,
            'name': obj.__class__.__name__
        }

    @classmethod
    def _is_valid_package(cls, data):
        try:
            bw2package_validator(data)
            return True
        except Invalid:
            return False

    @classmethod
    def _is_whitelisted(cls, metadata):
        return metadata['module'].split(".")[0] in cls.APPROVED

    @classmethod
    def _unroll_dict(cls, data):
        """If data is a dictionary with keys that aren't keys in JSON, turn dict from:

            {(1, 2): 3}

        to:

            [
                ((1,2), 3),
            ]

        Only looks at first level of dict, not recursive.

        Returns:
            obj: data (either original or modified)
            bool: data needed modification

        """
        try:
            JsonWrapper.dumps(data)
            return data, False
        except:
            if not isinstance(data, dict):
                raise ValueError("Data not a dict, and can't be JSON serialized")
            return zip(data.keys(), data.values()), True

    @classmethod
    def _reroll_dict(cls, data):
        """Return JSON list to proper python dict form with tuple keys"""
        return {tuple(x): y for x, y in data}

    @classmethod
    def _create_class(cls, metadata, apply_whitelist=True):
        if apply_whitelist and not cls._is_whitelisted(metadata):
            raise UnsafeData("{}.{} not a whitelisted class name".format(
                metadata['module'], metadata['name']
            ))
        exec("from {} import {}".format(metadata['module'], metadata['name']))
        return locals()[metadata['name']]

    @classmethod
    def _prepare_obj(cls, obj):
        data = obj.load()
        ready_data, unrolled = cls._unroll_dict(data)
        return {
            'metadata': obj.metadata[obj.name],
            'name': obj.name,
            'class': cls._get_class_metadata(obj),
            'unrolled_dict': unrolled,
            'data': ready_data,
        }

    @classmethod
    def _load_object(cls, data, whitelist=True):
        if not cls._is_valid_package(data):
            raise InvalidPackage
        data['class'] = cls._create_class(data['class'], whitelist)

        if isinstance(data['name'], list):
            data['name'] = tuple(data['name'])

        if data['unrolled_dict']:
            data['data'] = cls._reroll_dict(data['data'])

        return data

    @classmethod
    def _create_obj(cls, data):
        instance = data['class'](data['name'])

        if data['name'] not in instance.metadata:
            instance.register(**data['metadata'])
        else:
            instance.backup()
            instance.metadata[data['name']] = data['metadata']

        instance.write(data['data'])
        instance.process()
        return instance

    @classmethod
    def export_objs(cls, objs, filename, folder="export"):
        """Export a list of objects. Can have heterogeneous types.

        Args:
            * *objs* (list): List of objects to export.
            * *filename* (str): Name of file to create.
            * *folder* (str, optional): Folder to create file in. Default is ``export``.

        Returns:
            Filepath of created file.

        """
        filepath = os.path.join(
            config.request_dir(folder),
            filename + u".bw2package"
        )
        JsonWrapper.dump_bz2(
            [cls._prepare_obj(o) for o in objs],
            filepath
        )
        return filepath

    @classmethod
    def export_obj(cls, obj, filename=None, folder="export"):
        """Export an object.

        Args:
            * *obj* (object): Object to export.
            * *filename* (str, optional): Name of file to create. Default is ``obj.name``.
            * *folder* (str, optional): Folder to create file in. Default is ``export``.

        Returns:
            Filepath of created file.

        """
        if filename is None:
            filename = obj.name
        filepath = os.path.join(
            config.request_dir(folder),
            filename + u".bw2package"
        )
        JsonWrapper.dump_bz2(cls._prepare_obj(obj), filepath)
        return filepath

    @classmethod
    def load_file(cls, filepath, whitelist=True):
        """Load a bw2package file with one or more objects. Does not create new objects.

        Args:
            * *filepath* (str): Path of file to import
            * *whitelist* (bool): Apply whitelist to allowed types. Default is ``True``.

        Returns the loaded data in the bw2package dict data format, with the following changes:
            * ``"class"`` is an actual class.
            * dictionaries are rerolled, if necessary.
            * if ``"name"`` was a list, it is converted to a tuple.

        """
        raw_data = JsonWrapper.load_bz2(filepath)
        if isinstance(raw_data, dict):
            return cls._load_object(raw_data)
        else:
            return [cls._load_object(o) for o in raw_data]

    @classmethod
    def import_file(cls, filepath, whitelist=True):
        """Import bw2package file, and create the loaded objects, including registering, writing, and processing the created objects.

        Args:
            * *filepath* (str): Path of file to import
            * *whitelist* (bool): Apply whitelist to allowed types. Default is ``True``.

        Returns:
            Created object or list of created objects.

        """
        loaded = cls.load_file(filepath, whitelist)
        if isinstance(loaded, dict):
            return cls._import_obj(loaded)
        else:
            return [cls._import_obj(o) for o in loaded]


def download_biosphere():
    logger = get_logger("io-performance.log")
    start = time()
    filepath = download_file("biosphere-new.bw2package")
    logger.info("Downloading biosphere package: %.4g" % (time() - start))
    start = time()
    BW2Package.import_objs(filepath)
    logger.info("Importing biosphere package: %.4g" % (time() - start))


def download_methods():
    logger = get_logger("io-performance.log")
    start = time()
    filepath = download_file("methods-new.bw2iapackage")
    logger.info("Downloading methods package: %.4g" % (time() - start))
    start = time()
    BW2Package.import_objs(filepath)
    logger.info("Importing methods package: %.4g" % (time() - start))
