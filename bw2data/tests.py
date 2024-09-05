import atexit
import random
import shutil
import string
import tempfile
import unittest
from pathlib import Path

import wrapt

from bw2data import config, databases, geomapping, methods
from bw2data.parameters import parameters
from bw2data.project import projects


class BW2DataTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        config.dont_warn = True
        config.is_test = True
        tempdir = Path(tempfile.mkdtemp())
        project_name = "".join(random.choices(string.ascii_lowercase, k=18))
        projects.change_base_directories(
            base_dir=tempdir,
            base_logs_dir=tempdir,
            project_name=project_name,
            update=False,
        )
        projects._is_temp_dir = True
        self.extra_setup()

    def extra_setup(self):
        pass

    def test_setup_clean(self):
        self.assertEqual(list(databases), [])
        self.assertEqual(list(methods), [])
        self.assertEqual(len(geomapping), 1)  # GLO
        self.assertTrue("GLO" in geomapping)
        self.assertEqual(len(projects), 1)  # Default project
        self.assertTrue("default" not in projects)
        self.assertFalse(len(parameters))


@wrapt.decorator
def bw2test(wrapped, instance, args, kwargs):
    config.dont_warn = True
    config.is_test = True
    tempdir = Path(tempfile.mkdtemp())
    project_name = "".join(random.choices(string.ascii_lowercase, k=18))
    projects.change_base_directories(
        base_dir=tempdir, base_logs_dir=tempdir, project_name=project_name, update=False
    )
    projects._is_temp_dir = True
    atexit.register(shutil.rmtree, tempdir)
    return wrapped(*args, **kwargs)
