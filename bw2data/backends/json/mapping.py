# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ...serialization import PickledDict
from ... import projects


class KeyMapping(PickledDict):
    filename = "keys-filenames.mapping"


def get_mapping(filepath):
    print("Called get_mapping with filepath:\n", filepath)
    print("In cache: ", filepath in projects._json_backend_cache)
    if filepath not in projects._json_backend_cache:
        projects._json_backend_cache[filepath] = KeyMapping(filepath)
    return projects._json_backend_cache[filepath]
