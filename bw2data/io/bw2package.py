# -*- coding: utf-8 -*
from .. import Database, databases, config, JsonWrapper, methods, Method
from ..logs import get_logger
from ..utils import database_hash, download_file
from ..errors import UnsafeData, InvalidPackage
from ..validate import bw2package_validator
from voluptuous import Invalid
from time import time
import os
import warnings


class BW2Package(object):
    APPROVED = {
        'bwdata',
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
        """Valid packages must have the following structure:

        {
            'metadata': {
                'module': str,
                'name': str
            },
            'data': object
        }

        """
        try:
            bw2package_validator(data)
            return True
        except Invalid:
            return False

    @classmethod
    def _is_whitelised(cls, metadata):
        return metadata['module'].split(".")[0] in cls.APPROVED

    @classmethod
    def _unroll_dict(cls, data):
        """If data is a dictionary with keys that aren't keys in JSON, turn dict from:

            {(1, 2): 3}

        to:

            [
                [[1,2], 3],
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
        if apply_whitelist and not cls._is_whitelised(metadata):
            raise UnsafeData("{}.{} not a whitelisted class name".format(
                metadata['module'], metadata['name']
            ))
        exec("from {} import {}".format(metadata['module'], metadata['name']))
        return locals()[metadata['name']]

    @classmethod
    def export_objs(cls, objs, folder="export"):
        for obj in objs:
            cls.export_obj(obj, folder)

    @classmethod
    def export_obj(cls, obj, folder="export"):
        data = obj.load()
        ready_data, unrolled = cls._unroll_dict(data)
        to_export = {
            'metadata': obj.metadata[obj.name],
            'name': obj.name,
            'class': cls._get_class_metadata(obj),
            'unrolled_dict': unrolled,
            'data': ready_data,
        }
        filepath = os.path.join(
            config.request_dir(folder),
            obj.filename + u".bw2package"
        )
        JsonWrapper.dump_bz2(to_export, filepath)

    @classmethod
    def import_objs(cls, filepath, whitelist=True):
        unprocessed = JsonWrapper.load_bz2(filepath)
        if isinstance(unprocessed, list):
            for obj in list:
                cls.import_obj(obj)
        else:
            cls.import_obj(unprocessed)

    @classmethod
    def import_obj(cls, data, whitelist=True):
        if not cls._is_valid_package(unprocessed):
            raise InvalidPackage
        obj = cls._create_class(unprocessed['class'], whitelist)

        if isinstance(unprocessed['name'], list):
            name = tuple(unprocessed['name'])
        else:
            name = unprocessed['name']

        instance = obj(name)

        if name not in instance.metadata:
            instance.register(**unprocessed['metadata'])
        else:
            obj(name).backup()
            instance.metadata[name] = unprocessed['metadata']
            # instance.metadata.flush()

        data = unprocessed['data']
        if instance['unrolled_dict']:
            data = cls._reroll_dict(data)

        instance.write(data)
        instance.process()
        return instance


def download_biosphere():
    logger = get_logger("io-performance.log")
    start = time()
    filepath = download_file("biosphere.bw2package")
    logger.info("Downloading biosphere package: %.4g" % (time() - start))
    start = time()
    BW2PackageImporter.importer(filepath)
    logger.info("Importing biosphere package: %.4g" % (time() - start))


def download_methods():
    logger = get_logger("io-performance.log")
    start = time()
    filepath = download_file("methods.bw2iapackage")
    logger.info("Downloading methods package: %.4g" % (time() - start))
    start = time()
    BW2PackageImporter.importer(filepath)
    logger.info("Importing methods package: %.4g" % (time() - start))
