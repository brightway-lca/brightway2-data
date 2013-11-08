# -*- coding: utf-8 -*
from .. import Database, databases, config, JsonWrapper, methods, Method
from ..logs import get_logger
from ..utils import database_hash, download_file
from time import time
import bz2
import os
import warnings


class BW2PackageExporter(object):
    @classmethod
    def _prepare_method(cls, name):
        data = {
            "metadata": methods[name],
            "cfs": [{
                "database": o[0][0],
                "code": o[0][1],
                "amount": o[1],
                "location": o[2],
                "uncertainty type": 0,
                "sigma": None,
                "maximum": None,
                "minimum": None
            } for o in Method(name).load()]
        }
        data["metadata"]["name"] = name
        return data

    @classmethod
    def export_ia_method(cls, name, folder="export"):
        filepath = os.path.join(config.request_dir(folder),
            ".".join(name) + ".bw2iapackage")
        with bz2.BZ2File(filepath, "w") as f:
            f.write(JsonWrapper.dumps([cls._prepare_method(name)]))
        return filepath

    @classmethod
    def export_all_methods(cls, folder="export"):
        filepath = os.path.join(config.request_dir(folder),
            "methods.bw2iapackage")
        with bz2.BZ2File(filepath, "w") as f:
            f.write(JsonWrapper.dumps(
                [cls._prepare_method(name) for name in methods.list]
                ))
        return filepath

    @classmethod
    def export_database(cls, name, include_dependencies=False, **kwargs):
        assert name in databases, "Can't find this database"

        extra_string = kwargs.get("extra_string", "")
        folder = kwargs.get("folder", "export")

        if include_dependencies:
            for dependency in databases[name]["depends"]:
                assert dependency in databases, \
                    "Can't find dependent database %s" % dependency
            to_export = [name] + databases[name]["depends"]
            filename = name + extra_string + ".fat.bw2package"
        else:
            to_export = [name]
            filename = name + extra_string + ".bw2package"
        filepath = os.path.join(config.request_dir(folder), filename)
        with bz2.BZ2File(filepath, "w") as f:
            f.write(JsonWrapper.dumps({db_name: {
                "metadata": databases[db_name],
                "data": {k[1]: v for k, v in Database(db_name).load().iteritems()}
                } for db_name in to_export}))
        return filepath

    @classmethod
    def export(cls, name, include_dependencies=False):
        if isinstance(name, (tuple, list)):
            return cls.export_ia_method(name)
        elif isinstance(name, basestring):
            return cls.export_database(name, include_dependencies)
        else:
            raise ValueError("Unknown input data")


class BW2PackageImporter(object):
    @classmethod
    def importer(cls, filepath, overwrite=False):
        if overwrite:
            raise NotImplementedError
        if filepath.split(".")[-1] == "bw2package":
            return cls.import_database(filepath, overwrite)
        elif filepath.split(".")[-1] == "bw2iapackage":
            return cls.import_method(filepath, overwrite)
        else:
            raise ValueError("Unknown input data")

    @classmethod
    def import_database(cls, filepath, overwrite):
        logger = get_logger("io-performance.log")

        if overwrite:
            raise NotImplementedError
        with bz2.BZ2File(filepath) as f:
            start = time()

            package_data = JsonWrapper.loads(f.read())

            logger.info("Loading BW2Package database (len %s): %.4g" % (len(package_data), time() - start))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for name, data in package_data.iteritems():
                start = time()

                db_data = dict([((name, key), value) for key, value in \
                    data["data"].iteritems()])
                if name in databases:
                    raise ValueError("Database %s already exists" % name)
                metadata = data["metadata"]
                database = Database(name)
                database.register(
                    format=metadata["from format"],
                    depends=metadata["depends"],
                    num_processes=metadata["number"],
                    version=metadata["version"]
                    )
                database.write(db_data)
                database.process()

                logger.info("Processing BW2Package database (len %s): %.4g" % (len(db_data), time() - start))

    @classmethod
    def import_method(cls, filepath, overwrite):
        logger = get_logger("io-performance.log")

        if overwrite:
            raise NotImplementedError
        with bz2.BZ2File(filepath) as f:
            start = time()

            package_data = JsonWrapper.loads(f.read())

            logger.info("Loading BW2Package method (len %s): %.4g" % (len(package_data), time() - start))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            start = time()
            for data in package_data:

                name = tuple(data["metadata"]["name"])
                if name in methods:
                    raise ValueError("Duplicate method")
                method = Method(name)
                method.register(
                    unit=data["metadata"]["unit"],
                    description=data["metadata"]["description"],
                    num_cfs=data["metadata"]["num_cfs"]
                )
                method.write([
                    [(o["database"], o["code"]), o["amount"], o["location"]
                ] for o in data["cfs"]])
                method.process()

            logger.info("Processing BW2Package methods (len %s): %.4g" % (len(package_data), time() - start))


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
