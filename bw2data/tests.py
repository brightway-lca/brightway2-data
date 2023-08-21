import atexit
import random
import shutil
import string
import unittest

import wrapt

from bw2data import config, databases, geomapping, methods
from bw2data.parameters import parameters
from bw2data.project import projects


@wrapt.decorator
def bw2test(wrapped, instance, args, kwargs):
    config.dont_warn = True
    config.is_test = True
    config.cache = {}
    tempdir = projects._use_temp_directory()
    projects.create_project(
        "".join(random.choices(string.ascii_lowercase, k=18)),
        activate=True,
        exist_ok=True,
    )
    atexit.register(shutil.rmtree, tempdir)
    return wrapped(*args, **kwargs)
