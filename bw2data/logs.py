# -*- coding: utf-8 -*-
from . import config
from logging.handlers import RotatingFileHandler
from utils import random_string, create_in_memory_zipfile_from_directory
import codecs
import datetime
import logging
import os
import requests
import uuid
from serialization import JsonWrapper
try:
    import anyjson
except ImportError:
    anyjson = None


def get_logger(name, level=logging.INFO):
    filename = "%s-%s.log" % (
        name, datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p"))
    handler = RotatingFileHandler(
        os.path.join(config.dir, 'logs', filename),
        maxBytes=50000, encoding='utf-8', backupCount=5)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(lineno)d %(message)s")
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_io_logger(name):
    """Build a logger that records only relevent data for display later as HTML."""
    dirname = config.request_dir("logs")
    assert dirname, "No logs directory found"

    filepath = os.path.join(dirname, "%s.%s.log" % (name, random_string(6)))
    handler = logging.StreamHandler(codecs.open(filepath, "w", "utf-8"))
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(u"%(message)s"))
    logger.addHandler(handler)
    return logger, filepath


def get_verbose_logger(name, level=logging.WARNING):
    filename = "%s-%s.log" % (
        name, datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p"))
    handler = RotatingFileHandler(
        os.path.join(config.dir, 'logs', filename),
        maxBytes=50000, encoding='utf-8', backupCount=5)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler.setFormatter(logging.Formatter('''
Message type:       %(levelname)s
Location:           %(pathname)s:%(lineno)d
Module:             %(module)s
Function:           %(funcName)s
Time:               %(asctime)s
Message:
%(message)s

'''))
    logger.addHandler(handler)
    return logger


def upload_logs_to_server(metadata={}):
    # Hardcoded for now
    url = "http://reports.brightwaylca.org/logs"
    dirpath = config.request_dir("logs")
    zip_fo = create_in_memory_zipfile_from_directory(dirpath)
    files = {'file': (uuid.uuid4().hex + ".zip", zip_fo.read())}
    metadata['json'] = 'native' if anyjson is None else \
        anyjson.implementation.name
    metadata['windows'] = config._windows
    return requests.post(
        url,
        data=metadata,
        files=files
    )
