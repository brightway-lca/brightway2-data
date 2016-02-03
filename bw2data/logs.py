# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import config, projects
from .utils import random_string, create_in_memory_zipfile_from_directory
from logging.handlers import RotatingFileHandler
import codecs
import datetime
import logging
import os
import requests
import uuid
try:
    import anyjson
except ImportError:
    anyjson = None


class FakeLog(object):
    """Like a log object, but does nothing"""
    def fake_function(cls, *args, **kwargs):
        return

    def __getattr__(self, attr):
        return self.fake_function


def get_logger(name, level=logging.INFO):
    filename = u"%s-%s.log" % (
        name, datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p"))
    handler = RotatingFileHandler(
        os.path.join(projects.logs_dir, filename),
        maxBytes=1e6, encoding='utf-8', backupCount=10)
    formatter = logging.Formatter(
        u"%(asctime)s %(levelname)s %(lineno)d %(message)s")
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_io_logger(name):
    """Build a logger that records only relevent data for display later as HTML."""
    filepath = os.path.join(projects.logs_dir, u"%s.%s.log" % (name, random_string(6)))
    handler = logging.StreamHandler(codecs.open(filepath, "w", "utf-8"))
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(u"%(message)s"))
    logger.addHandler(handler)
    return logger, filepath


def get_verbose_logger(name, level=logging.WARNING):
    filename = u"%s-%s.log" % (
        name, datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p"))
    handler = RotatingFileHandler(
        os.path.join(projects.logs_dir, filename),
        maxBytes=50000, encoding='utf-8', backupCount=5)
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level)
    handler.setFormatter(logging.Formatter(u'''
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
    zip_fo = create_in_memory_zipfile_from_directory(projects.logs_dir)
    files = {'file': (uuid.uuid4().hex + ".zip", zip_fo.read())}
    metadata['json'] = 'native' if anyjson is None else \
        anyjson.implementation.name
    metadata['windows'] = config._windows
    return requests.post(
        url,
        data=metadata,
        files=files
    )


def close_log(log):
    """Detach log handlers; flush to disk"""
    handlers = log.handlers[:]
    for handler in handlers:
        handler.close()
        log.removeHandler(handler)
