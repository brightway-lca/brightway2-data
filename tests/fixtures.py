import atexit
import os
import random
import shutil
import string
import unittest

import wrapt

from bw2data import config, databases, geomapping, methods
from bw2data.parameters import parameters
from bw2data.project import projects


biosphere = {
    ("biosphere", "1"): {
        "categories": ["things"],
        "code": 1,
        "exchanges": [],
        "name": "an emission",
        "type": "emission",
        "unit": "kg",
    },
    ("biosphere", "2"): {
        "categories": ["things"],
        "code": 2,
        "exchanges": [],
        "type": "emission",
        "name": "another emission",
        "unit": "kg",
    },
}

lcia = [
    (("biosphere", "1"), 10),
    (("biosphere", "2"), 1000)
]

food = {
    ("food", "1"): {
        "categories": ["stuff", "meals"],
        "code": 1,
        "exchanges": [
            {
                "amount": 0.5,
                "input": ("food", "2"),
                "type": "technosphere",
                "uncertainty type": 0,
            },
            {
                "amount": 0.05,
                "input": ("biosphere", "1"),
                "type": "biosphere",
                "uncertainty type": 0,
            },
        ],
        "location": "CA",
        "name": "lunch",
        "type": "process",
        "unit": "kg",
    },
    ("food", "2"): {
        "categories": ["stuff", "meals"],
        "code": 2,
        "exchanges": [
            {
                "amount": 0.25,
                "input": ("food", "1"),
                "type": "technosphere",
                "uncertainty type": 0,
            },
            {
                "amount": 0.15,
                "input": ("biosphere", "2"),
                "type": "biosphere",
                "uncertainty type": 0,
            },
        ],
        "location": "CH",
        "name": "dinner",
        "type": "process",
        "unit": "kg",
    },
}

food2 = {
    ("food", "1"): {
        "categories": ["stuff", "meals"],
        "code": 1,
        "exchanges": [
            {
                "amount": 0.5,
                "input": ("food", "2"),
                "type": "technosphere",
                "uncertainty type": 0,
            },
            {
                "amount": 0.05,
                "input": ("biosphere", "1"),
                "type": "biosphere",
                "uncertainty type": 0,
            },
        ],
        "location": "CA",
        "name": "lunch",
        "type": "process",
        "unit": "kg",
    },
    ("food", "2"): {
        "categories": ["stuff", "meals"],
        "code": 2,
        "exchanges": [
            {
                "amount": 0.25,
                "input": ("food", "1"),
                "type": "technosphere",
                "uncertainty type": 0,
            },
            {
                "amount": 0.15,
                "input": ("biosphere", "2"),
                "type": "biosphere",
                "uncertainty type": 0,
            },
        ],
        "location": "CH",
        "name": "dinner",
        "type": "process",
        "unit": "kg",
    },
}

get_naughty = lambda: [
    x.replace("\n", "")
    for x in open(
        os.path.join(os.path.dirname(__file__), "naughty_strings.txt"), encoding="utf8"
    )
    if x[0] != "#"
]


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
    projects.create_project(
        "".join(random.choices(string.ascii_lowercase, k=18)),
        activate=True,
        exist_ok=True
    )
    atexit.register(shutil.rmtree, tempdir)
    return wrapped(*args, **kwargs)
