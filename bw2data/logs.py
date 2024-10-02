import codecs
import datetime
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import structlog

try:
    import anyjson
except ImportError:
    anyjson = None


class FakeLog:
    """Like a log object, but does nothing"""

    def fake_function(cls, *args, **kwargs):
        return

    def __getattr__(self, attr):
        return self.fake_function


def get_logger(name, level=logging.INFO):
    from bw2data import projects

    filename = "{}-{}.log".format(
        name,
        datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p"),
    )
    handler = RotatingFileHandler(
        projects.logs_dir / filename,
        maxBytes=1e6,
        encoding="utf-8",
        backupCount=10,
    )
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(lineno)d %(message)s")
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_stdout_feedback_logger(name: str, level: int = logging.INFO):
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%H:%M:%S%z")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_structlog_stdout_feedback_logger(level: int = logging.INFO):
    structlog.reset_defaults()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="%H:%M:%S%z", utc=False),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger()
    logger.debug("Starting feedback logger")
    structlog.reset_defaults()
    return logger


def get_io_logger(name):
    """Build a logger that records only relevent data for display later as HTML."""
    from bw2data import projects
    from bw2data.utils import random_string

    filepath = projects.logs_dir / "{}.{}.log".format(name, random_string(6))
    handler = logging.StreamHandler(codecs.open(filepath, "w", "utf-8"))
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger, filepath


def get_verbose_logger(name, level=logging.WARNING):
    from bw2data import projects

    filename = "{}-{}.log".format(
        name,
        datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p"),
    )
    handler = RotatingFileHandler(
        projects.logs_dir / filename,
        maxBytes=50000,
        encoding="utf-8",
        backupCount=5,
    )
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            """
Message type:       %(levelname)s
Location:           %(pathname)s:%(lineno)d
Module:             %(module)s
Function:           %(funcName)s
Time:               %(asctime)s
Message:
%(message)s

"""
        )
    )
    logger.addHandler(handler)
    return logger


def close_log(log):
    """Detach log handlers; flush to disk"""
    handlers = log.handlers[:]
    for handler in handlers:
        handler.close()
        log.removeHandler(handler)


if os.getenv("BRIGHTWAY_NO_STRUCTLOG"):
    stdout_feedback_logger = get_stdout_feedback_logger("brightway-stdout-feedback")
else:
    stdout_feedback_logger = get_structlog_stdout_feedback_logger()
