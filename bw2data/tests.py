from . import config, databases, methods, geomapping
from .parameters import parameters
from .project import projects
import atexit
import random
import shutil
import string
import unittest
import wrapt


class BW2DataTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        config.dont_warn = True
        config.is_test = True
        config.cache = {}
        projects._use_temp_directory()
        self.extra_setup()

    def extra_setup(self):
        pass

    def test_setup_clean(self):
        self.assertEqual(list(databases), [])
        self.assertEqual(list(methods), [])
        self.assertEqual(len(geomapping), 1)  # GLO
        self.assertTrue("GLO" in geomapping)
        self.assertEqual(len(projects), 1)  # Default project
        self.assertTrue("default" in projects)
        self.assertFalse(len(parameters))


@wrapt.decorator
def bw2test(wrapped, instance, args, kwargs):
    config.dont_warn = True
    config.is_test = True
    config.cache = {}
    tempdir = projects._use_temp_directory()
    projects.set_current("".join(random.choices(string.ascii_lowercase, k=18)))
    atexit.register(shutil.rmtree, tempdir)
    return wrapped(*args, **kwargs)
